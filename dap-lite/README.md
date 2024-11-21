
# DAP-Lite

DAP-Lite is a lightweight version of the DAP system designed to manage processing pipelines efficiently using an Open Data Cube (ODC) index. It allows users to process indexed products, update previously processed jobs, and manage deletions in a structured and scalable way.

## Overview

The overarching idea is to leverage the ODC index to find and process products efficiently. The system comprises:

1. **Processors**: Defined processors are responsible for processing specific products into new outputs. <span style="background-color:yellow; padding: 5px; border-radius: 3px;">For current MVP,  processors are defined in `constants.py` rather than in a real db table. </span> 
2. **Executions**: Tracks processing attempts, status, and metadata for each product.

The general flow includes:
1. A processor requests the next available product to process (`get_next_processing_job`).
2. The processor processes the product.
3. The processor reports success or failure (`report_finished_processing` or `report_processing_failure`).
4. Products can also be updated or deleted via specialized job functions (`get_job_for_update`, `get_job_for_deletion_of_product`).

## Key Tables

### 1. **`bnp.process_executions`**
Tracks each processing attempt, including its status, worker information, and error messages.

| Column          | Type                | Description                                     |
|------------------|---------------------|-------------------------------------------------|
| `id`            | SERIAL PRIMARY KEY  | Unique identifier for each execution.          |
| `processor_id`  | INTEGER             | The processor handling the job.                |
| `worker_id`     | TEXT                | Identifies the worker or process handling it.  |
| `action`        | ENUM                | `process`, `update`, or `delete`.              |
| `src_product_id`| INTEGER             | Foreign key to the ODC product.                |
| `dst_path`      | TEXT                | Path of the output product.                    |
| `status`        | ENUM                | `running`, `failed`, `canceled`, `finished`.   |
| `err_msg`       | TEXT                | Error messages for failed jobs.                |

### 2. **`agdc.dataset_location`**
This is defined by open datacube and is only read.

| Column          | Type                | Description                                     |
|------------------|---------------------|-------------------------------------------------|
| ... || 
| `uri_body`    |  character varying | S3 path to the `stac_item.json` file of the product |
| ... || 

Only the column `uri_body` is used. Me match the MSIL1C to find the row as 
 `//d49b125f138b4dd9b225925950e638bc:eodata-models/NMD/2018/Basskikt_ogeneraliserad_Sverige_v1_1/stac_item.json` 
> **NOTE!**  
> <span style="background-color:yellow; padding: 5px; border-radius: 3px;">
> If this implementation is mainly targeted to L1C products. To work with other products, you need to differentiate the way we determine the path from above STAC file path.
> </span> 

 

## Key Functions

### **1. `get_next_processing_job`**
Retrieves the next product to process from the ODC index. Locks the row to ensure no two workers pick the same job.

**SQL Gist**:
```sql
SELECT id, 's3:' || REPLACE(uri_body, '.stac.json', '.SAFE') AS uri
FROM agdc.dataset_location
WHERE uri_body LIKE '%' || p_src_pattern || '%MSIL1C%'
  AND NOT EXISTS (
      SELECT 1 FROM bnp.process_executions WHERE src_product_id = dataset_location.id
  )
LIMIT 1 FOR UPDATE SKIP LOCKED;
```

### **2. `report_finished_processing`**
Marks a job as successfully completed and updates the execution record.

### **3. `report_processing_failure`**
Logs a failure with an optional error message.

### **4. `get_job_for_update`**
Fetches a job requiring an update from `bnp.process_executions`.

### **5. `get_job_for_deletion_of_product`**
Fetches a job requiring deletion from `bnp.process_executions`. On finish, the job row is removed.

## Minimal Example

```python
from dap_lite.bnpdriver import BNPDriver

driver = BNPDriver()
worker_id = "local-12345"

# Get the next job for processing
job_id, src_uri = driver.get_next_job(processor_id=1, worker_id=worker_id)

if job_id:
    # Process the product (placeholder logic)
    success = True

    if success:
        driver.report_finished(processor_id=1, job_id=job_id)
    else:
        driver.report_failure(processor_id=1, job_id=job_id, error_message="Processing failed.")
else:
    print("No jobs available.")
```

## Installation
### Conda environment example (production)
        name: force-eo-env
        channels:
        - conda-forge
        - defaults
        dependencies:
        - python=3.9
        - pip
        - psycopg2-binary
        - other-dependencies
        - pip:
            - git+ssh://git@gitlab.ice.ri.se:ssdl-core/des-bnp.git#egg=dap-lite

### Development 
As above but use path instead, as:

         - pip:
             - -e ../dap-lite

## Testing

Run tests with:

```bash
poetry run pytest
```

## Files and Dirs of Special Interest

| Name | Description|
|------------------ |-------------------------------------------------|
| `./pyproject.toml` | Dependent packages etc |
| `./examples`    | Some example code on how to write processor, reprocessor, deletor etc |
|`./src/dap_lite/bnpdriver.py` | The module containing the driver you need |
|`./src/dap_lite/constants.py` | Here we define all available processors |
|`./src/dap_lite/sql/odc-db-additions.sql` | This is the complete db additions with stored procedures that makes dap-lite work. This should be added to the `datacube` database|
