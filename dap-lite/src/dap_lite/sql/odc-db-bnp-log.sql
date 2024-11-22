CREATE TABLE IF NOT EXIST bnp.log (
    id SERIAL PRIMARY KEY,           -- Unique identifier for each log entry
    worker_id TEXT NOT NULL,          -- Worker identifier
    baseline INT NOT NULL,            -- Baseline version for processing
    job_id INT NOT NULL,              -- Associated job ID
    l1c_source TEXT NOT NULL,         -- Source L1C product information
    message TEXT NOT NULL,            -- Log message
    timestamp TIMESTAMP DEFAULT NOW() -- Automatically sets to current timestamp
);


CREATE OR REPLACE FUNCTION bnp.store_log_message(
    p_worker_id TEXT,
    p_baseline INT,
    p_job_id INT,
    p_l1c_source TEXT,
    p_message TEXT
)
RETURNS VOID AS $$
BEGIN
    INSERT INTO bnp.log (worker_id, baseline, job_id, l1c_source, message)
    VALUES (p_worker_id, p_baseline, p_job_id, p_l1c_source, p_message);
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION bnp.get_processed_products_by_worker(
    p_worker_id TEXT
)
RETURNS TABLE (job_id INT, l1c_source TEXT) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT job_id, l1c_source
    FROM bnp.log
    WHERE worker_id = p_worker_id;
END;
$$ LANGUAGE plpgsql;

 
 CREATE OR REPLACE FUNCTION bnp.get_logs_for_product(
    p_l1c_source TEXT
)
RETURNS TABLE (timestamp TIMESTAMP, worker_id TEXT, message TEXT) AS $$
BEGIN
    RETURN QUERY
    SELECT timestamp, worker_id, message
    FROM bnp.log
    WHERE l1c_source = p_l1c_source
    ORDER BY timestamp ASC;
END;
$$ LANGUAGE plpgsql;


