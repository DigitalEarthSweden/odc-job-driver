import time
import sys
import os
import csv
from datetime import datetime
from dap_lite import get_driver, DriverType

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Add script directory to PYTHONPATH if not already present
if script_dir not in sys.path:
    sys.path.append(script_dir)

# Constants
MY_PROCESSOR_ID = 1

# Open the timings log file in append mode
with open("timings.log", "a", newline="") as logfile:
    # Initialize CSV writer
    csv_writer = csv.writer(logfile)

    # Write the header if the file is empty
    if logfile.tell() == 0:
        csv_writer.writerow(
            ["timestamp", "method", "duration_seconds", "job_id", "details"]
        )

    # Initialize the driver (mock or real based on configuration)
    driver = get_driver(DriverType.DB)

    try:
        while True:
            # Measure time for `get_next_job`
            start_time = time.time()
            job_id, next_s3 = driver.get_next_job(src_pattern="MSIL1C")
            get_job_duration = time.time() - start_time

            # Log the timing for `get_next_job`
            csv_writer.writerow(
                [
                    datetime.now().isoformat(),
                    "get_next_job",
                    f"{get_job_duration:.2f}",
                    job_id or "None",
                    next_s3 or "None",
                ]
            )
            logfile.flush()  # Ensure data is written immediately

            if not job_id:
                print("No jobs available. Sleeping...")
                time.sleep(10)  # Avoid tight loops if no jobs are available
                continue

            print(f"Processing job {job_id} for product: {next_s3}")

            try:
                # Simulate some processing work
                # time.sleep(0.01)  # Placeholder for actual work logic
                all_ok = True  # Assume the process succeeded for testing

                if all_ok:
                    # Measure time for `report_finished`
                    start_time = time.time()
                    driver.store_log_message("Successful execution of somethin")
                    driver.report_finished(dst_path="s3://dummy/path")

                    report_finished_duration = time.time() - start_time

                    # Log the timing for `report_finished`
                    csv_writer.writerow(
                        [
                            datetime.now().isoformat(),
                            "report_finished",
                            f"{report_finished_duration:.2f}",
                            job_id,
                            "success",
                        ]
                    )
                    logfile.flush()  # Ensure data is written immediately

                    print(f"Job {job_id} finished successfully.")
                else:
                    # Handle failure scenario
                    driver.report_failure("Processing failed for unknown reasons.")
                    print(f"Job {job_id} failed. Logged for further analysis.")
            except Exception as e:
                # Log unexpected errors
                driver.report_failure(f"Unexpected error: {type(e).__name__} - {e}")
                print(f"Error processing job {job_id}: {type(e).__name__} - {e}")

    except KeyboardInterrupt:
        print("Shutting down gracefully.")
    finally:
        # Close the driver connection
        driver.close()
