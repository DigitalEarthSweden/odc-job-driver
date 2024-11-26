from enum import Enum
import os
from typing import Tuple, Optional


def get_worker_id():
    # Use Kubernetes pod ID if available
    pod_id = os.getenv("POD_ID")
    if pod_id:
        return pod_id
    return f"local-{os.getpid()}"


class JobAction(Enum):
    PROCESS = "process"
    UPDATE = "update"  # aka reprocess
    DELETE = "delete"


class BNPDriver:

    def __init__(self):
        # Mock data to simulate database records
        self.mock_jobs = [
            {
                "job_id": idx + 1,  # Assign unique job IDs starting from 1
                "src_uri": src_uri,
                "status": "pending",
            }
            for idx, src_uri in enumerate(
                [
                    "s3://eodata-sentinel2-s2msi1c-2024/9/15/S2B_MSIL1C_20240915T102559_N0511_R108_T33WXQ_20240915T123129.SAFE",
                    "s3://eodata-sentinel2-s2msi1c-2024/9/15/S2B_MSIL1C_20240915T102559_N0511_R108_T33VVH_20240915T123129.SAFE",
                    "s3://eodata-sentinel2-s2msi1c-2024/9/15/S2B_MSIL1C_20240915T102559_N0511_R108_T33VVJ_20240915T123129.SAFE",
                    "s3://eodata-sentinel2-s2msi1c-2024/9/15/S2B_MSIL1C_20240915T102559_N0511_R108_T33VWJ_20240915T123129.SAFE",
                    "s3://eodata-sentinel2-s2msi1c-2024/9/15/S2B_MSIL1C_20240915T102559_N0511_R108_T33VWH_20240915T123129.SAFE",
                    "s3://eodata-sentinel2-s2msi1c-2023/8/19/S2B_MSIL1C_20230819T101609_N0509_R065_T33VUC_20230819T123928.SAFE",
                    "s3://eodata-sentinel2-s2msi1c-2023/7/8/S2A_MSIL1C_20230708T102601_N0509_R108_T33VUC_20230708T141205.SAFE",
                    "s3://eodata-sentinel2-s2msi1c-2022/7/20/S2A_MSIL1C_20220720T101611_N0400_R065_T33VUC_20220720T140828.SAFE",
                    "s3://eodata-sentinel2-s2msi1c-2024/9/15/S2B_MSIL1C_20240915T102559_N0511_R108_T33WXR_20240915T123129.SAFE",
                    "s3://eodata-sentinel2-s2msi1c-2023/8/19/S2B_MSIL1C_20230819T101609_N0509_R065_T33VVC_20230819T123928.SAFE",
                    "s3://eodata-sentinel2-s2msi1c-2023/8/19/S2B_MSIL1C_20230819T101609_N0509_R065_T33VVD_20230819T123928.SAFE",
                    "s3://eodata-sentinel2-s2msi1c-2023/8/19/S2B_MSIL1C_20230819T101609_N0509_R065_T33VUD_20230819T123928.SAFE",
                    "s3://eodata-sentinel2-s2msi1c-2023/7/8/S2A_MSIL1C_20230708T102601_N0509_R108_T33VVC_20230708T141205.SAFE",
                    "s3://eodata-sentinel2-s2msi1c-2023/7/8/S2A_MSIL1C_20230708T102601_N0509_R108_T33VVD_20230708T141205.SAFE",
                    "s3://eodata-sentinel2-s2msi1c-2023/7/8/S2A_MSIL1C_20230708T102601_N0509_R108_T33VUD_20230708T141205.SAFE",
                    "s3://eodata-sentinel2-s2msi1c-2022/7/20/S2A_MSIL1C_20220720T101611_N0400_R065_T33VVC_20220720T140828.SAFE",
                    "s3://eodata-sentinel2-s2msi1c-2022/7/20/S2A_MSIL1C_20220720T101611_N0400_R065_T33VVD_20220720T140828.SAFE",
                    "s3://eodata-sentinel2-s2msi1c-2022/7/20/S2A_MSIL1C_20220720T101611_N0400_R065_T33VUD_20220720T140828.SAFE",
                ]
            )
        ]
        self.logs = []
        self.current_job_id = None
        self.current_src_path = None
        self.current_job_id_and_url = None, None
        self.worker_id = get_worker_id()

    def get_next_job(
        self,
        processor_id: int,
        src_pattern: str = "MSIL1C",
    ):
        """
        Fetch the next available job for the given processor.
        """
        for job in self.mock_jobs:
            if job["status"] == "pending":
                job["status"] = "processing"
                self.current_job_id = job["job_id"]
                self.current_src_path = job["src_uri"]
                print(
                    f"Mock get_next_job: Found job {job['job_id']} for processor {processor_id}"
                )
                self.current_job_id_and_url = job["job_id"], job["src_uri"]
            else:
                self.current_job_id_and_url = None, None
                print(
                    f"Mock get_next_job: No jobs available for processor {processor_id}"
                )
        return self.current_job_id_and_url

    @property
    def current_job(self) -> Tuple[Optional[int], Optional[str]]:
        return self.current_job_id_and_url

    @property
    def worker_id(self) -> str:
        return self.worker_id

    def report_finished(
        self, processor_id: str, job_id: int, dst_path: str = "s3://dummy/path"
    ):
        """
        Mark the job as finished.
        """
        if self.current_job_id != job_id:
            raise ValueError(
                f"Mock report_finished: Reporting finished for job {job_id} but the "
                f"current job is {self.current_job_id}"
            )
        for job in self.mock_jobs:
            if job["job_id"] == job_id:
                job["status"] = "finished"
                print(
                    f"Mock report_finished: Job {job_id} marked as finished. Output at {dst_path}"
                )
                self.current_job_id = None
                self.current_src_path = None
                return
        print(f"Mock report_finished: Job {job_id} not found.")

    def report_failure(self, processor_id: int, job_id: int, message: str):
        """
        Mark the job as failed.
        """
        for job in self.mock_jobs:
            if job["job_id"] == job_id:
                job["status"] = "failed"
                print(
                    f"Mock report_failure: Job {job_id} marked as failed. Reason: {message}"
                )
                return
        print(f"Mock report_failure: Job {job_id} not found.")

    def store_log_message(self, baseline, job_id, l1c_source, message):
        """Stores a log message."""
        self.logs.append(
            {
                "worker_id": self.worker_id,
                "baseline": baseline,
                "job_id": job_id,
                "l1c_source": l1c_source,
                "message": message,
            }
        )
        print(f"Mock store_log_message: Log message stored for job {job_id}: {message}")

    def get_processed_products_by_worker(self, worker_id):
        """Retrieves products processed by a specific worker."""
        processed = [
            job
            for job in self.mock_jobs
            if job.get("worker_id") == worker_id and job["status"] == "finished"
        ]
        print(
            f"Mock get_processed_products_by_worker: Found {len(processed)} products for worker {worker_id}"
        )
        return processed

    def get_logs_for_product(self, l1c_source):
        """Retrieves logs for a specific L1C product."""
        logs = [log for log in self.logs if log["l1c_source"] == l1c_source]
        print(
            f"Mock get_logs_for_product: Found {len(logs)} logs for product {l1c_source}"
        )
        return logs

    def close(self):
        """
        Close the mock database connection.
        """
        print("Mock close: Mock connection closed.")
