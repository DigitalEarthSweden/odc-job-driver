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
--                          get_logs_from_job_id
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


