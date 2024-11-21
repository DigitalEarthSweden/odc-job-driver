import time
import sys
import os
import csv
from datetime import datetime
from dap_lite.driver import BNPDriver

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Add script directory to PYTHONPATH if not already present
if script_dir not in sys.path:
    sys.path.append(script_dir)


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

    driver = BNPDriver()
    MY_PROCESSOR_ID = 1

    try:
        while True:
            # Measure time for get_next_job
            start_time = time.time()
            job_id, next_s3 = driver.get_next_job(MY_PROCESSOR_ID)
            get_job_duration = time.time() - start_time

            # Log the timing for get_next_job
            csv_writer.writerow(
                [
                    datetime.now().isoformat(),
                    "get_next_job",
                    get_job_duration,
                    job_id,
                    next_s3,
                ]
            )
            logfile.flush()  # Ensure data is written immediately

            if not job_id:
                print("No jobs available. Sleeping...")
                time.sleep(10)  # Avoid tight loops if no jobs are available
                continue

            print(f"Processing job {job_id} for product: {next_s3}")

            # Simulate some processing work
            # time.sleep(0.01)  # Placeholder for actual work logic

            allOK = True  # Assume the process succeeded for testing

            if allOK:
                # Measure time for report_finished
                start_time = time.time()
                driver.report_finished(MY_PROCESSOR_ID, job_id)
                report_finished_duration = time.time() - start_time

                # Log the timing for report_finished
                csv_writer.writerow(
                    [
                        datetime.now().isoformat(),
                        "report_finished",
                        report_finished_duration,
                        job_id,
                        "success",
                    ]
                )
                logfile.flush()  # Ensure data is written immediately

                print(f"Job {job_id} finished successfully.")
            else:
                driver.report_failure(MY_PROCESSOR_ID, job_id)
                print(f"Job {job_id} failed. Logged for further analysis.")

    except KeyboardInterrupt:
        print("Shutting down gracefully.")
    finally:
        driver.close()
