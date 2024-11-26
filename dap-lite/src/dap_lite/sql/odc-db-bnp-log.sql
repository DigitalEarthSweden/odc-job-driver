-- --------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bnp.log (
    id SERIAL PRIMARY KEY,           -- Unique identifier for each log entry
    job_id INT NOT NULL,              -- Associated job ID
    message TEXT NOT NULL,            -- Log message
    ts TIMESTAMP DEFAULT NOW() -- Automatically sets to current timestamp
);


-- --------------------------------------------------------------------------------
--                            bnp.store_log_message
-- --------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bnp.store_log_message(
    p_job_id INT,
    p_message TEXT
)
RETURNS VOID AS $$
BEGIN
    INSERT INTO bnp.log (job_id, message)
    VALUES (p_job_id, p_message);
END;
$$ LANGUAGE plpgsql;

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
        pe.worker_id = p_worker_id;
END;
$$ LANGUAGE plpgsql;


-- --------------------------------------------------------------------------------
--                           bnp.products_view
-- --------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bnp.get_logs_from_product_src(
    p_l1c_source TEXT
)
RETURNS TABLE (ts TIMESTAMP, message TEXT, worker_id TEXT) AS $$
    SELECT
        l.ts,
        l.message,
        pe.worker_id
    FROM
        bnp.log l
    INNER JOIN
        bnp.process_executions pe ON l.job_id = pe.id
    INNER JOIN
        agdc.dataset_location dl ON pe.src_product_id = dl.id
    WHERE
        dl.uri_scheme || ':' || dl.uri_body = p_l1c_source;
$$ LANGUAGE sql;

-- --------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bnp.get_logs_from_job_id(
    p_job_id INTEGER
)
RETURNS TABLE (ts TIMESTAMP, message TEXT) AS $$
    SELECT
        l.ts,
        l.message
    FROM
        bnp.log l
    WHERE
        l.job_id = p_job_id;
$$ LANGUAGE sql;

-- --------------------------------------------------------------------------------
--                                   bnp.products_view
-- --------------------------------------------------------------------------------
CREATE OR REPLACE VIEW bnp.products_view AS
SELECT
    pe.id AS job_id,
    worker_id,
    dl.uri_scheme || '://' || dl.uri_body AS source_path,
    lg.total_execution_time,
    pe.status AS state
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


-- --------------------------------------------------------------------------------
--                             bnp.workers_view
-- --------------------------------------------------------------------------------
CREATE OR REPLACE VIEW bnp.workers_view AS
WITH worker_stats AS (
    SELECT
        pe.worker_id,
        COUNT(*) AS total_jobs,
        SUM(CASE WHEN pe.status = 'pending' THEN 1 ELSE 0 END) AS pending_jobs,
        SUM(CASE WHEN pe.status = 'running' THEN 1 ELSE 0 END) AS running_jobs,
        SUM(CASE WHEN pe.status = 'completed' THEN 1 ELSE 0 END) AS completed_jobs,
        SUM(CASE WHEN pe.status = 'failed' THEN 1 ELSE 0 END) AS failed_jobs,
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
    ws.pending_jobs,
    ws.running_jobs,
    ws.completed_jobs,
    ws.failed_jobs,
    GREATEST(ws.last_updated_at, wll.last_log_time) AS last_seen
FROM
    worker_stats ws
LEFT JOIN
    worker_last_logs wll ON ws.worker_id = wll.worker_id;
