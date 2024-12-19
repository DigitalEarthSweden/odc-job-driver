
-- This is a retake on get_next_job that is more modular and flexible. 
-- It also allows update, delete etc. 

-----------------------------------------------------------------------------------
--                         bnp.default_candidate_listing
-----------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bnp.default_candidate_listing(p_src_pattern TEXT)
RETURNS TABLE (id INTEGER, uri TEXT) AS $$
BEGIN
    RETURN QUERY
    SELECT id,
           's3:' || REGEXP_REPLACE(uri_body, '\.stac(_item)?\.json$', '') || '.SAFE' AS uri
    FROM bnp.dataset_location source
    WHERE source.uri_body LIKE '%' || p_src_pattern || '%'
      AND NOT EXISTS (
          SELECT 1
          FROM bnp.process_executions pe
          WHERE pe.src_product_id = source.id
      )
    ORDER BY bnp.tile_name_from_s1c_uri(source.uri_body),
             bnp.acquisition_date_from_s1c_uri(source.uri_body) DESC
    LIMIT 5
    FOR UPDATE SKIP LOCKED;
END;
$$ LANGUAGE plpgsql;


-----------------------------------------------------------------------------------
--                         bnp.create_process_execution
-----------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bnp.create_process_execution(
    p_processor_id INTEGER,
    p_worker_id TEXT,
    p_src_product_id INTEGER
)
RETURNS INTEGER AS $$
DECLARE
    job_id INTEGER;
BEGIN
    INSERT INTO bnp.process_executions (
        processor_id, src_product_id, worker_id, status, attempts, action
    )
    VALUES (
        p_processor_id, 
        p_src_product_id, 
        p_worker_id,
        'running', 
        1, 
        'process'
    )
    RETURNING id INTO job_id;

    RETURN job_id;
END;
$$ LANGUAGE plpgsql;


-----------------------------------------------------------------------------------
--                             bnp.is_power_on
-----------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bnp.is_power_on()
RETURNS BOOLEAN AS $$
DECLARE
    power_status TEXT;
BEGIN
    SELECT value::TEXT INTO power_status
    FROM bnp.globals
    WHERE variable_name = 'power';
    power_status := TRIM(BOTH '"' FROM power_status);
    RETURN power_status = 'on';
END;
$$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------
--                         bnp.get_next_processing_job
-----------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bnp.get_next_processing_job_v2(
    p_processor_id INTEGER,
    p_worker_id TEXT,
    p_src_pattern TEXT DEFAULT 'MSIL1C',
    p_candidate_listing_function TEXT DEFAULT 'bnp.default_candidate_listing',
    p_max_attempts INTEGER DEFAULT 5
)
RETURNS TABLE (job_id INTEGER, src_uri TEXT) AS $$
DECLARE
    product RECORD;
    job_id INTEGER;
    allowed_functions TEXT[] := ARRAY['bnp.default_candidate_listing']; -- Update as needed
    attempt_count INTEGER := 0;
BEGIN
    -- Security check: Ensure the candidate listing function is allowed
    IF NOT p_candidate_listing_function = ANY(allowed_functions) THEN
        RAISE EXCEPTION 'Unauthorized candidate listing function: %', p_candidate_listing_function;
    END IF;

    -- Check if power is 'on'
    IF NOT bnp.is_power_on() THEN
        RAISE NOTICE 'Power is not ON. Returning NULL.';
        RETURN QUERY SELECT NULL::INTEGER, NULL::TEXT;
        RETURN;
    END IF;

    -- Attempt to find and lock a product using the candidate listing function
    RAISE NOTICE 'Attempting to select product using %', p_candidate_listing_function;

    -- Dynamic SQL to call the candidate listing function
    FOR product IN EXECUTE FORMAT(
        'SELECT id, uri FROM %I(%L)',
        p_candidate_listing_function,
        p_src_pattern
    )
    LOOP
        attempt_count := attempt_count + 1;
        IF attempt_count > p_max_attempts THEN
            RAISE NOTICE 'Maximum attempts (% attempts) reached. Returning NULL.', p_max_attempts;
            RETURN QUERY SELECT NULL::INTEGER, NULL::TEXT;
            RETURN;
        END IF;

        RAISE NOTICE 'Attempting to process product ID: %, Attempt % of %', product.id, attempt_count, p_max_attempts;
        BEGIN
            -- The product is already locked in the candidate listing function
            -- Try to create the process_execution
            job_id := bnp.create_process_execution(p_processor_id, p_worker_id, product.id);

            RAISE NOTICE 'Job created: ID: %', job_id;

            -- Return the job ID and URI
            RETURN QUERY SELECT job_id, product.uri;
            RETURN;

        EXCEPTION WHEN unique_violation THEN
            RAISE NOTICE 'Conflict: Job already created by another process.';
            -- Continue to next candidate
            CONTINUE;

        WHEN foreign_key_violation THEN
            RAISE NOTICE 'Foreign key violation when creating process execution for product ID %.', product.id;
            -- This is a serious issue; re-raise the exception
            RAISE;

        WHEN others THEN
            RAISE NOTICE 'Error creating process execution: %', SQLERRM;
            -- Re-raise the exception to let it propagate
            RAISE;
        END;
    END LOOP;

    -- If no product was successfully claimed, return NULL
    RAISE NOTICE 'No available products found or maximum attempts reached. Returning NULL.';
    RETURN QUERY SELECT NULL::INTEGER, NULL::TEXT;
END;
$$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------
--                         bnp.get_next_update_job
-----------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bnp.get_next_update_job(
    p_worker_id TEXT,
    p_new_processor_id INTEGER,
    p_old_processor_id INTEGER,
    p_max_attempts INTEGER DEFAULT 5,
    p_is_deleting BOOLEAN
)
RETURNS TABLE (job_id INTEGER, l1c_src_uri TEXT, old_prod_uri TEXT) AS $$
DECLARE
    job RECORD;
    attempt_count INTEGER := 0;
    current_action = 'update'
BEGIN
    -- Loop to attempt finding and processing a job
    LOOP
        attempt_count := attempt_count + 1;
        IF attempt_count > p_max_attempts THEN
            RAISE NOTICE 'Maximum attempts (% attempts) reached. Returning NULL.', p_max_attempts;
            RETURN QUERY SELECT NULL::INTEGER, NULL::TEXT, NULL::TEXT;
            RETURN;
        END IF;
        if p_is_deleting THEN
            current_action = 'delete'
        END iF;
        -- Attempt to find and lock a job from process_executions
        SELECT pe.id, pe.src_product_id, pe.processor_id, dl.uri_body AS src_uri
        INTO job
        FROM bnp.process_executions pe
        JOIN bnp.dataset_location dl ON pe.src_product_id = dl.id
        WHERE pe.processor_id = p_old_processor_id
        LIMIT 1
        FOR UPDATE SKIP LOCKED;

        -- If no matching job is found, return NULL
        IF NOT FOUND THEN
            RAISE NOTICE 'No matching jobs found in process_executions. Returning NULL.';
            RETURN QUERY SELECT NULL::INTEGER, NULL::TEXT, NULL::TEXT;
            RETURN;
        END IF;

        -- Update the job with the new_processor_id and other necessary fields
        BEGIN
            -- Store the old processor ID before updating
            PERFORM bnp.store_log_message(
                job.id,
                FORMAT('Update started from %s to %s', job.processor_id, p_new_processor_id)
            );

            UPDATE bnp.process_executions
            SET
                processor_id = p_new_processor_id,
                worker_id = p_worker_id,
                status = 'running',
                attempts =  1, -- is resent when reprocessing (should we logged it first? worth the effort?)
                action = current_action, 
                updated_at = NOW() -- Assuming you have a timestamp column to track updates
            WHERE id = job.id;

            -- Log the reprocessing action
            RAISE NOTICE 'Job ID % is being updated by worker % with new processor ID %.', job.id, p_worker_id, p_new_processor_id;

            -- Construct the URIs
            l1c_src_uri := 's3:' || REGEXP_REPLACE(job.src_uri, '\.stac(_item)?\.json$', '') || '.SAFE';
            old_prod_uri := job.src_uri;

            -- Return the job_id and URIs
            RETURN QUERY SELECT job.id, l1c_src_uri, old_prod_uri;
            RETURN;

        EXCEPTION WHEN unique_violation THEN
            RAISE NOTICE 'Conflict: Job already processed by another worker.';
            -- Continue to next attempt
            CONTINUE;

        WHEN foreign_key_violation THEN
            RAISE NOTICE 'Foreign key violation when updating process execution for job ID %.', job.id;
            RAISE;

        WHEN others THEN
            RAISE NOTICE 'Error updating job: %', SQLERRM;
            RAISE;
        END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------
--                         bnp.get_next_retry_job
-----------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bnp.get_next_retry_job(
    p_worker_id TEXT,
    p_processor_id INTEGER,
    p_max_attempts INTEGER DEFAULT 5
)
RETURNS TABLE (job_id INTEGER, src_uri TEXT) AS $$
DECLARE
    job RECORD;
    attempt_count INTEGER := 0;
BEGIN
    -- Loop to attempt finding and processing a retry job
    LOOP
        attempt_count := attempt_count + 1;
        IF attempt_count > p_max_attempts THEN
            RAISE NOTICE 'Maximum attempts (% attempts) reached. Returning NULL.', p_max_attempts;
            RETURN QUERY SELECT NULL::INTEGER, NULL::TEXT;
            RETURN;
        END IF;

        -- Attempt to find and lock a job that needs to be retried
        SELECT pe.id, pe.src_product_id, pe.worker_id AS old_worker_id, pe.attempts, dl.uri_body AS src_uri
        INTO job
        FROM bnp.process_executions pe
        JOIN bnp.dataset_location dl ON pe.src_product_id = dl.id
        WHERE pe.processor_id = p_processor_id
          AND pe.status = 'failed'
        ORDER BY pe.attempts ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED;

        -- If no matching job is found, return NULL
        IF NOT FOUND THEN
            RAISE NOTICE 'No jobs found to retry. Returning NULL.';
            RETURN QUERY SELECT NULL::INTEGER, NULL::TEXT;
            RETURN;
        END IF;

        -- Update the job for retry
        BEGIN
            -- Log the retry action
            PERFORM bnp.store_log_message(
                job.id,
                FORMAT('Retrying job. Old worker ID: %s, new worker ID: %s, attempt: %s',
                    job.old_worker_id, p_worker_id, COALESCE(job.attempts, 0) + 1)
            );

            UPDATE bnp.process_executions
            SET
                worker_id = p_worker_id,
                status = 'running',
                attempts = COALESCE(attempts, 0) + 1,
                updated_at = NOW()
            WHERE id = job.id;

            -- Log the retry action
            RAISE NOTICE 'Job ID % is being retried by worker % (Attempt %).', job.id, p_worker_id, COALESCE(job.attempts, 0) + 1;

            -- Construct the URI
            src_uri := 's3:' || REGEXP_REPLACE(job.src_uri, '\.stac(_item)?\.json$', '') || '.SAFE';

            -- Return the job_id and src_uri
            RETURN QUERY SELECT job.id, src_uri;
            RETURN;

        EXCEPTION WHEN others THEN
            RAISE NOTICE 'Error updating job for retry: %', SQLERRM;
            RAISE;
        END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------
--  PUBLIC                       bnp.get_next_job
-----------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bnp.get_next_job(
    p_action TEXT,
    p_worker_id TEXT,
    p_processor_id INTEGER DEFAULT NULL,
    p_src_pattern TEXT DEFAULT 'MSIL1C',
    p_candidate_listing_function TEXT DEFAULT 'bnp.default_candidate_listing',
    p_old_processor_id INTEGER DEFAULT NULL,
    p_new_processor_id INTEGER DEFAULT NULL,
    p_max_attempts INTEGER DEFAULT 5
)
RETURNS TABLE (job_id INTEGER, l1c_src_uri TEXT, old_prod_uri TEXT) AS $$
BEGIN
    IF p_action = 'process' THEN
        RETURN QUERY
        SELECT job_id, src_uri AS l1c_src_uri, NULL::TEXT AS old_prod_uri
        FROM bnp.get_next_processing_job(
            p_processor_id := p_processor_id,
            p_worker_id := p_worker_id,
            p_src_pattern := p_src_pattern,
            p_candidate_listing_function := p_candidate_listing_function,
            p_max_attempts := p_max_attempts
        );
    
    ELSIF p_action = 'update' THEN
        RETURN QUERY
        SELECT job_id, l1c_src_uri, old_prod_uri
        FROM bnp.get_next_update_job(
            p_worker_id := p_worker_id,
            p_new_processor_id := p_new_processor_id,
            p_old_processor_id := p_old_processor_id,
            p_max_attempts := p_max_attempts
        );
    
    ELSIF p_action = 'retry' THEN
        RETURN QUERY
        SELECT job_id, src_uri AS l1c_src_uri, NULL::TEXT AS old_prod_uri
        FROM bnp.get_next_retry_job(
            p_worker_id := p_worker_id,
            p_processor_id := p_processor_id,
            p_max_attempts := p_max_attempts
        );
    
    ELSE
        RAISE EXCEPTION 'Invalid action specified: %', p_action;
    END IF;
END;
$$ LANGUAGE plpgsql;



-- Enable the pg_trgm extension if not already enabled
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Indexes for bnp.dataset_location

-- 1. GIN index on uri_body for efficient LIKE queries with leading wildcards
CREATE INDEX idx_dataset_location_uri_body_trgm
ON bnp.dataset_location USING GIN (uri_body gin_trgm_ops);

-- 2. Index on bnp.tile_name_from_s1c_uri(uri_body) for ORDER BY optimization
CREATE INDEX idx_dataset_location_tile_name
ON bnp.dataset_location (bnp.tile_name_from_s1c_uri(uri_body));

-- 3. Index on bnp.acquisition_date_from_s1c_uri(uri_body) for ORDER BY optimization
CREATE INDEX idx_dataset_location_acquisition_date
ON bnp.dataset_location (bnp.acquisition_date_from_s1c_uri(uri_body));

-- Indexes for bnp.process_executions

-- 4. Index on src_product_id to optimize joins and subqueries
CREATE INDEX idx_process_executions_src_product_id
ON bnp.process_executions (src_product_id);

-- 5. Index on processor_id to speed up filtering by processor
CREATE INDEX idx_process_executions_processor_id
ON bnp.process_executions (processor_id);

-- 6. Composite index on processor_id, status, and attempts for retry job queries
CREATE INDEX idx_process_executions_processor_status_attempts
ON bnp.process_executions (processor_id, status, attempts);

-- Indexes for bnp.globals

-- 7. Index on variable_name for quick lookup of global variables
CREATE INDEX idx_globals_variable_name
ON bnp.globals (variable_name);
