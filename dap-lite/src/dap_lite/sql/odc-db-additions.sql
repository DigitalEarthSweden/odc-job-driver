
-- This script is made so we can dump it several times
-- Note that updates to the tables requires you to drop them first

CREATE SCHEMA IF NOT EXISTS bnp;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'job_action_type') THEN
        CREATE TYPE bnp.job_action_type AS ENUM ('process', 'update', 'delete');
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'job_status') THEN
        CREATE TYPE bnp.job_status AS ENUM ('running', 'canceled', 'failed', 'finished');
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

 
-- Create the user if it doesn't exist, it can only read odc but also write bnp
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bnp_db_rw') THEN
        CREATE ROLE bnp_db_rw LOGIN PASSWORD 'bnp_password';
    END IF;
END
$$;
 
 

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
BEGIN
    -- Find the next available product from agdc.dataset_location
    SELECT id, 
           's3:' || REPLACE(uri_body, '.stac.json', '.SAFE') AS uri
    INTO product
    FROM agdc.dataset_location source
    WHERE source.uri_body LIKE '%' || p_src_pattern || '%'  -- Match the desired product type
      AND NOT EXISTS (
          SELECT 1
          FROM bnp.process_executions pe
          WHERE pe.src_product_id = source.id
      )
    ORDER BY source.added DESC -- Process newer products first
    LIMIT 1
    FOR UPDATE SKIP LOCKED;

    -- If no product is found, return NULL
    IF NOT FOUND THEN
        RETURN QUERY SELECT NULL::INTEGER, NULL::TEXT;
    END IF;

    -- Create a new process_execution row
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

    -- Return the process_execution ID and the product's URI
    RETURN QUERY SELECT job_id, product.uri;
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
        updated_at = NOW()
    WHERE id = p_job_id
      AND processor_id = p_processor_id;
END;
$$ LANGUAGE plpgsql;

CREATE INDEX idx_processor_status ON bnp.process_executions (processor_id, status);
CREATE INDEX idx_job_id ON bnp.process_executions (id);



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
