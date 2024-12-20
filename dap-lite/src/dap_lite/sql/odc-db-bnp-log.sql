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


-- --------------------------------------------------------------------------------
--                          bnp.cloud_skips
-- --------------------------------------------------------------------------------
CREATE OR REPLACE VIEW bnp.cloud_skips AS
SELECT 
    log.job_id,
    (regexp_matches(log.message, 'dc:\s*([\d.]+)%'))[1]::float AS dc,
    (regexp_matches(log.message, 'wc:\s*([\d.]+)%'))[1]::float AS wc,
    (regexp_matches(log.message, 'sc:\s*([\d.]+)%'))[1]::float AS sc,
    (regexp_matches(log.message, 'cc:\s*([\d.]+)%'))[1]::float AS cc,
    bnp.acquisition_date_from_s1c_uri(dl.uri_body) AS acquisition_date
FROM 
    bnp.log AS log
JOIN 
    bnp.process_executions AS pe ON log.job_id = pe.id
JOIN 
    bnp.dataset_location AS dl ON pe.src_product_id = dl.id
WHERE 
    log.message LIKE '%Skip%';
