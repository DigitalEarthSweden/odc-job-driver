import time
import sys
import os
from dap_lite import get_driver, DriverType
from dap_lite.workflow_step import WorkflowStep, WorkFlowStepSkippedException
import random
import logging


def setup_logging(worker_id: str):
    log_format = f"%(asctime)s - {worker_id} - %(levelname)s - %(message)s"
    formatter = logging.Formatter(
        log_format, datefmt="%Y-%m-%d %H:%M:%S"  # Customize the date format
    )

    # Create a handler
    stream_handler = logging.StreamHandler()  # Log to console
    stream_handler.setFormatter(formatter)  # Attach formatter to the handler

    # Get or create the logger
    log_main = logging.getLogger("MainLoop")
    log_main.setLevel(logging.DEBUG)  # Set the logging level
    log_main.addHandler(stream_handler)  # Add handler to the logger

    return log_main


# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Add script directory to PYTHONPATH if not already present
if script_dir not in sys.path:
    sys.path.append(script_dir)

# Constants
MY_PROCESSOR_ID = 1

# Initialize the driver (mock or real based on configuration)
driver = get_driver(DriverType.DB)
log = setup_logging(driver.worker_id)
try:
    while True:
        # Measure time for `get_next_job`
        job_id, next_s3 = driver.get_next_job(src_pattern="MSIL1C")

        if not job_id:
            log.debug("No jobs available or system starved/busy. Sleeping...")
            time.sleep(3)  # Avoid tight loops if no jobs are available
            continue

        log.debug(f"Processing job {job_id} for product: {next_s3}")

        try:
            # Simulate some processing work
            # time.sleep(0.01)  # Placeholder for actual work logic
            all_ok = True  # Assume the process succeeded for testing
            with WorkflowStep(name="Downloading", bnp_driver=driver, logger=log):
                time.sleep(random.randint(0, 4))

            with WorkflowStep(name="Piffing", bnp_driver=driver, logger=log):
                time.sleep(random.randint(0, 4))

            with WorkflowStep(name="Force Processing", bnp_driver=driver, logger=log):
                time.sleep(random.randint(0, 4))
                if random.randint(0, 100) > 80:
                    raise ValueError("Ouch, something went very wrong here")

            with WorkflowStep(
                name="Packing final product", bnp_driver=driver, logger=log
            ):
                time.sleep(random.randint(0, 4))

            with WorkflowStep(
                name="Uploading final product", bnp_driver=driver, logger=log
            ):
                time.sleep(random.randint(0, 4))

            log.debug(f"Job {job_id} finished successfully.")
            driver.report_finished("s3://dummy/path")
        except WorkFlowStepSkippedException:
            driver.report_skipped("Skipped for some good reasons")
        except ValueError as e:
            log.info(
                f"Now we have handled the error and continue execution. Error info:{type(e)} {e}"
            )
        except Exception as e:
            # Log unexpected errors
            driver.report_failure(f"Unexpected error: {type(e).__name__} - {e}")
            print(f"Error processing job {job_id}: {type(e).__name__} - {e}")
            break

except KeyboardInterrupt:
    print("Shutting down gracefully.")
finally:
    # Close the driver connection
    driver.close()
