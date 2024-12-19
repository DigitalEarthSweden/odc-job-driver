
-- This script is made so we can dump it several times
-- Note that updates to the tables requires you to drop them first

CREATE SCHEMA IF NOT EXISTS bnp;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'job_action_type') THEN
        CREATE TYPE bnp.job_action_type AS ENUM ('process', 'update', 'delete');
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'job_status') THEN
        CREATE TYPE bnp.job_status AS ENUM ('running', 'canceled', 'failed', 'finished','skipped');
    END IF;
END;
$$;
-- Create the process_executions table with updated timestamp defaults and constraints
CREATE TABLE bnp.process_executions (
    id SERIAL PRIMARY KEY,
    processor_id INTEGER NOT NULL, -- Links to the processors "table"
    worker_id TEXT, -- Uniquely identifies who is working with this product
    action bnp.job_action_type DEFAULT 'process', -- Enum for action type
    src_product_id INTEGER NOT NULL, -- Foreign key to agdc.dataset_location
    dst_path TEXT, -- For convenience, storing the resulting product's path
    status bnp.job_status NOT NULL, -- Enum for status
    attempts INTEGER DEFAULT 0, -- Number of attempts made for this execution
    start_time TIMESTAMP DEFAULT NOW(), -- When the execution started
    finished_time TIMESTAMP, -- When the execution finished (NULL if incomplete)
    updated_at TIMESTAMP NOT NULL DEFAULT NOW() -- Tracks last modification time
    err_msg TEXT
);
-- Add the unique constraint to ensure one execution per processor_id and src_product_id, if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_schema = 'bnp' 
          AND table_name = 'process_executions' 
          AND constraint_name = 'unique_execution'
    ) THEN
        ALTER TABLE bnp.process_executions
        ADD CONSTRAINT unique_execution UNIQUE (processor_id, src_product_id);
    END IF;
END
$$;

CREATE TABLE bnp.globals (
    variable_name TEXT PRIMARY KEY,
    value JSONB NOT NULL
);

-- Insert variables
INSERT INTO bnp.globals (variable_name, value)
VALUES
    ('power', '"on"');

 
-- Create the user if it doesn't exist, it can only read odc but also write bnp
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bnp_db_rw') THEN
        CREATE ROLE bnp_db_rw LOGIN PASSWORD 'bnp_password';
    END IF;
END
$$;
 
CREATE INDEX idx_processor_status ON bnp.process_executions (processor_id, status);
CREATE INDEX idx_job_id ON bnp.process_executions (id);

-----------------------------------------------------------------------------------
--                             bnp.baseline_from_s1c_uri
-----------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bnp.baseline_from_s1c_uri(uri_body TEXT)
RETURNS INTEGER AS $$
BEGIN
    RETURN CAST(
        regexp_replace(uri_body, '.*_N([0-9]{4})_.*', '\1', 'g')
        AS INTEGER
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-----------------------------------------------------------------------------------
--                             bnp.acquisition_date_from_s1c_uri
-----------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bnp.acquisition_date_from_s1c_uri(uri_body TEXT)
RETURNS TIMESTAMP WITH TIME ZONE AS $$
BEGIN
    RETURN to_timestamp(
        regexp_replace(uri_body, '.*MSIL1C_([0-9]{8}T[0-9]{6}).*', '\1', 'g'),
        'YYYYMMDD"T"HH24MISS'
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-----------------------------------------------------------------------------------
--                              bnp.tile_name_from_s1c_uri
-----------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bnp.tile_name_from_s1c_uri(uri_body CHARACTER VARYING)
RETURNS TEXT AS $$
BEGIN
    RETURN regexp_replace(
        uri_body, 
        '.*_T([0-9A-Z]{5}).*', -- Matches `T` followed by exactly 5 alphanumeric characters
        '\1',
        'g'
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-----------------------------------------------------------------------------------
--                              bnp.product_uri_from_stac_item_uri
-----------------------------------------------------------------------------------
-- Future note, now we handle L1C stac item paths but we can extend this to handle
-- other path by checking which product it is etc to calculate the source path, 
CREATE OR REPLACE FUNCTION bnp.product_uri_from_stac_item_uri(p_uri_body TEXT)
RETURNS TEXT AS $$
BEGIN
    RETURN 's3:' || REGEXP_REPLACE(p_uri_body, '\.stac(_item)?\.json$', '') || '.SAFE';
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-----------------------------------------------------------------------------------
--                              bnp.get_next_processing_job
-----------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bnp.get_next_processing_job(
    p_processor_id INTEGER,
    p_worker_id TEXT,
    p_src_pattern TEXT DEFAULT 'MSIL1C'
)
RETURNS TABLE (job_id INTEGER, src_uri TEXT) AS $$
DECLARE
    product RECORD;
    power_status TEXT;
BEGIN
    -- Log: Starting function execution
    RAISE NOTICE 'Starting get_next_processing_job for processor: %, worker: %', p_processor_id, p_worker_id;

    -- Check if power is 'on'
    SELECT value::TEXT INTO power_status
    FROM bnp.globals
    WHERE variable_name = 'power';

    -- Remove surrounding quotes from JSONB text
    power_status := TRIM(BOTH '"' FROM power_status);

    RAISE NOTICE 'Power status: %', power_status;

    IF NOT FOUND OR power_status <> 'on' THEN
        RAISE NOTICE 'Power is not ON. Returning NULL.';
        RETURN QUERY SELECT NULL::INTEGER, NULL::TEXT;
        RETURN;
    END IF;

    -- Attempt to find and lock a product
    RAISE NOTICE 'Attempting to select product from agdc.dataset_location.';
    FOR product IN
        SELECT  id,'s3:' || REGEXP_REPLACE(uri_body, '\.stac(_item)?\.json$', '') || '.SAFE' AS uri
        FROM bnp.dataset_location source
        WHERE source.uri_body LIKE '%' || p_src_pattern || '%'
          AND NOT EXISTS (
              SELECT 1
              FROM bnp.process_executions pe
              WHERE pe.src_product_id = source.id
          )
        ORDER BY bnp.tile_name_from_s1c_uri(source.uri_body), bnp.acquisition_date_from_s1c_uri(source.uri_body) DESC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    LOOP
        BEGIN
            RAISE NOTICE 'Product found: ID: %, URI: %', product.id, product.uri;

            -- Insert a new process_execution row
            INSERT INTO bnp.process_executions (
                processor_id, src_product_id, worker_id, status, attempts, action
            )
            VALUES (
                p_processor_id, 
                product.id, 
                p_worker_id,
                'running', 
                1, 
                'process'
            )
            RETURNING id INTO job_id;

            RAISE NOTICE 'Job created: ID: %', job_id;

            -- Return the job ID and URI
            RETURN QUERY SELECT job_id, product.uri;

        EXCEPTION WHEN unique_violation THEN
            RAISE NOTICE 'Conflict: Job already created by another process.';
            CONTINUE;
        END;
    END LOOP;

    -- If no product was successfully claimed, return NULL
    RAISE NOTICE 'No available products found. Returning NULL.';
    RETURN QUERY SELECT NULL::INTEGER, NULL::TEXT;
END;
$$ LANGUAGE plpgsql;




-----------------------------------------------------------------------------------
--                         bnp.report_finished_processing
-----------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bnp.report_finished_processing(
    _processor_id INTEGER,  
    _job_id INTEGER,        
    _dst_path TEXT         
)
RETURNS VOID AS $$
BEGIN
    -- Validate that the job exists and is started by the specified processor
    IF NOT EXISTS (
        SELECT 1
        FROM bnp.process_executions pe
        WHERE pe.id = _job_id
          AND pe.processor_id = _processor_id
          AND pe.status = 'running'
    ) THEN
        RAISE EXCEPTION 'Job % is not running or not started by processor %', _job_id, _processor_id;
    END IF;

    -- Update the job as finished
    UPDATE bnp.process_executions
    SET status = 'finished',
        dst_path = _dst_path,
        updated_at = NOW(),
        finished_time = NOW()
    WHERE id = _job_id
      AND processor_id = _processor_id;
    
    PERFORM  bnp.store_log_message(
        _job_id, 
        'Final status set to FINISHED' 
    );
END;
$$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------
--                         bnp.report_processing_failure
-----------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bnp.report_processing_failure(
    p_processor_id INTEGER, 
    p_job_id INTEGER,
    p_message TEXT DEFAULT ''        
)
RETURNS VOID AS $$
BEGIN
    -- Validate that the job exists and is started by the specified processor
    IF NOT EXISTS (
        SELECT 1
        FROM bnp.process_executions pe
        WHERE pe.id = p_job_id
          AND pe.processor_id = p_processor_id
          AND pe.status = 'running'
    ) THEN
        RAISE EXCEPTION 'Job % is not running or not started by processor %', p_job_id, p_processor_id;
    END IF;

    -- Update the job as failed and set the error message
    UPDATE bnp.process_executions
    SET status = 'failed',
        err_msg = p_message,
        updated_at = NOW(),
        finished_time = NOW()
    WHERE id = p_job_id
      AND processor_id = p_processor_id;

    PERFORM  bnp.store_log_message(
    p_job_id, 
    'Final status set to FAILED' 
);
END;
$$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------
--                         bnp.report_processing_skipped
-----------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bnp.report_processing_skipped(
    p_processor_id INTEGER, 
    p_job_id INTEGER,
    p_message TEXT DEFAULT ''        
)
RETURNS VOID AS $$
BEGIN
    -- Validate that the job exists and is started by the specified processor
    IF NOT EXISTS (
        SELECT 1
        FROM bnp.process_executions pe
        WHERE pe.id = p_job_id
          AND pe.processor_id = p_processor_id
          AND pe.status = 'running'
    ) THEN
        RAISE EXCEPTION 'Job % is not running or not started by processor %', p_job_id, p_processor_id;
    END IF;

    -- Update the job as failed and set the error message
    UPDATE bnp.process_executions
    SET status = 'skipped',
        err_msg = p_message,
        updated_at = NOW(),
        finished_time = NOW()
    WHERE id = p_job_id
      AND processor_id = p_processor_id;

    PERFORM  bnp.store_log_message(
    p_job_id, 
    'Final status set to SKIPPED' 
);
END;
$$ LANGUAGE plpgsql;

-- --------------------------------------------------------------------------------
--                            bnp.get_product_from_job_id
-- --------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bnp.get_product_from_job_id(job_id INTEGER)
RETURNS TEXT AS $$
DECLARE
    source_path TEXT;
BEGIN
    -- Query to retrieve and transform the product's source path
    SELECT regexp_replace(split_part(dl.uri_body, '/', -1), '\.stac\.json$', '')
    INTO source_path
    FROM bnp.process_executions pe
    INNER JOIN agdc.dataset_location dl
    ON pe.src_product_id = dl.id
    WHERE pe.id = job_id;

    -- Check if a product was found
    IF source_path IS NULL THEN
        RAISE EXCEPTION 'No product found for job_id: %', job_id;
    END IF;

    -- Return the processed product name
    RETURN source_path;
END;
$$ LANGUAGE plpgsql;

-- --------------------------------------------------------------------------------
--                           bnp.processing_stats
-- --------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bnp.processing_stats()
RETURNS TABLE(status TEXT, count BIGINT) AS $$
BEGIN
    RETURN QUERY
    SELECT status, count(*)
    FROM bnp.process_executions
    GROUP BY status;
END;
$$ LANGUAGE plpgsql;


    worker_last_logs wll ON ws.worker_id = wll.worker_id;

-- --------------------------------------------------------------------------------
--                       bnp.get_processed_products_by_worker                   
-- --------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION bnp.get_processed_products_by_worker(
    p_worker_id TEXT
)
RETURNS TABLE (job_id INT, l1c_source TEXT) AS $$
BEGIN
    RETURN QUERY
    SELECT
        pe.id AS job_id,
        dl.uri_scheme || ':' || dl.uri_body AS l1c_source
    FROM
        bnp.process_executions pe
    INNER JOIN
        agdc.dataset_location dl ON pe.src_product_id = dl.id
    WHERE
        pe.worker_id = p_worker_id
    ORDER BY pe.updated_at desc;

END;
$$ LANGUAGE plpgsql;

-- --------------------------------------------------------------------------------
--                                   bnp.products_view
-- --------------------------------------------------------------------------------
CREATE OR REPLACE VIEW bnp.products_view AS
SELECT
    pe.id AS job_id,
    pe.worker_id,
    dl.uri_scheme || ':' || REPLACE(dl.uri_body, '.stac.json', '.SAFE') AS source_path,
    bnp.acquisition_date_from_s1c_uri(dl.uri_body) AS acquisition_date,
    bnp.tile_name_from_s1c_uri(dl.uri_body) AS tile_name,
    CASE
        WHEN lg.total_execution_time IS NOT NULL THEN
            LPAD(EXTRACT(MINUTE FROM lg.total_execution_time)::TEXT, 2, '0') || ':' ||
            LPAD(EXTRACT(SECOND FROM lg.total_execution_time)::TEXT, 2, '0')
        ELSE
            'N/A'
    END AS total_execution_time,
    pe.status AS status,
    pe.err_msg AS err_msg
FROM
    bnp.process_executions pe
INNER JOIN
    agdc.dataset_location dl ON pe.src_product_id = dl.id
LEFT JOIN (
    SELECT
        l.job_id,
        MAX(l.ts) - MIN(l.ts) AS total_execution_time
    FROM
        bnp.log l
    GROUP BY
        l.job_id
) lg ON pe.id = lg.job_id;




CREATE OR REPLACE FUNCTION delete_datasets_and_explorer_cache(delete_date DATE)
RETURNS VOID AS $$
DECLARE
    dataset_ids UUID[];
BEGIN
    -- Step 1: Collect IDs of datasets added on or after the given date
    SELECT ARRAY_AGG(id)
    INTO dataset_ids
    FROM agdc.dataset
    WHERE added::date >= delete_date;

    -- Step 2: Delete from ODC-dependent tables
    DELETE FROM agdc.dataset_location
    WHERE dataset_ref = ANY(dataset_ids);

    DELETE FROM agdc.dataset_source
    WHERE dataset_ref = ANY(dataset_ids)
       OR source_dataset_ref = ANY(dataset_ids);

    DELETE FROM agdc.dataset
    WHERE id = ANY(dataset_ids);

    -- Step 3: Delete from Explorer tables
    DELETE FROM cubedash.dataset_spatial
    WHERE id = ANY(dataset_ids);

    -- Optional: Update product cache
    UPDATE cubedash.product
    SET time_earliest = NULL, time_latest = NULL, footprint = NULL
    WHERE id IN (
        SELECT dataset_type_ref
        FROM agdc.dataset
        WHERE id = ANY(dataset_ids)
    );

    -- Step 4: Notify completion
    RAISE NOTICE 'Deleted datasets and Explorer cache for entries added on or after %', delete_date;
END;
$$ LANGUAGE plpgsql;


-- --------------------------------------------------------------------------------
--                             bnp.workers_view
-- --------------------------------------------------------------------------------
CREATE OR REPLACE VIEW bnp.workers_view AS
WITH worker_stats AS (
    SELECT
        pe.worker_id,
        COUNT(*) AS total_jobs, 
        SUM(CASE WHEN pe.status = 'running' THEN 1 ELSE 0 END) AS running_jobs,
        SUM(CASE WHEN pe.status = 'finished' THEN 1 ELSE 0 END) AS finished_jobs,
        SUM(CASE WHEN pe.status = 'failed' THEN 1 ELSE 0 END) AS failed_jobs,
        SUM(CASE WHEN pe.status = 'skipped' THEN 1 ELSE 0 END) AS skipped_jobs,
    
        MAX(pe.updated_at) AS last_updated_at
    FROM
        bnp.process_executions pe
    WHERE
        pe.worker_id IS NOT NULL
    GROUP BY
        pe.worker_id
),
worker_last_logs AS (
    SELECT
        pe.worker_id,
        MAX(l.ts) AS last_log_time
    FROM
        bnp.process_executions pe
    INNER JOIN
        bnp.log l ON l.job_id = pe.id
    WHERE
        pe.worker_id IS NOT NULL
    GROUP BY
        pe.worker_id
)
SELECT
    ws.worker_id,
    ws.total_jobs,
    ws.running_jobs,
    ws.finished_jobs,
    ws.failed_jobs,
    ws.skipped_jobs,
    GREATEST(ws.last_updated_at, wll.last_log_time) AS last_seen
FROM
    worker_stats ws
LEFT JOIN
    worker_last_logs wll ON ws.worker_id = wll.worker_id;


-- Grant usage and read-only access to agdc schema
ALTER ROLE bnp_db_rw WITH LOGIN;

GRANT USAGE ON SCHEMA agdc TO bnp_db_rw;
GRANT SELECT ON ALL TABLES IN SCHEMA agdc TO bnp_db_rw;

-- Grant usage and full access to bnp schema
GRANT USAGE, CREATE ON SCHEMA bnp TO bnp_db_rw;
GRANT INSERT, UPDATE, DELETE, SELECT ON ALL TABLES IN SCHEMA bnp TO bnp_db_rw;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA bnp TO bnp_db_rw;

-- Ensure future tables and functions in bnp also grant access
ALTER DEFAULT PRIVILEGES IN SCHEMA bnp
GRANT INSERT, UPDATE, DELETE, SELECT ON TABLES TO bnp_db_rw;
ALTER DEFAULT PRIVILEGES IN SCHEMA bnp
GRANT EXECUTE ON FUNCTIONS TO bnp_db_rw;

GRANT USAGE ON SCHEMA agdc TO bnp_db_rw; -- Allows access to the schema
GRANT SELECT ON agdc.dataset_location TO bnp_db_rw; -- Allows read access to the table
GRANT SELECT ON agdc.dataset_location TO bnp_db_rw; -- Allows read access
GRANT UPDATE ON agdc.dataset_location TO bnp_db_rw; -- Required for FOR UPDATE
GRANT SELECT, UPDATE ON TABLE agdc.dataset_location TO bnp_db_rw; -- Verify SELECT and UPDATE privileges
GRANT USAGE ON SCHEMA agdc TO bnp_db_rw; -- Ensure schema-level access
GRANT USAGE, SELECT ON SEQUENCE bnp.process_executions_id_seq TO bnp_db_rw;
GRANT USAGE, CREATE ON SCHEMA bnp TO bnp_db_rw;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bnp TO bnp_db_rw;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA bnp TO bnp_db_rw;
ALTER DEFAULT PRIVILEGES IN SCHEMA bnp
GRANT USAGE, SELECT ON SEQUENCES TO bnp_db_rw;
