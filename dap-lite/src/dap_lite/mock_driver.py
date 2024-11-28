from enum import Enum
import os
import logging
from typing import Tuple, Optional, List, Dict

# Configure logging
from .logger import log


def get_worker_id() -> str:
    """Fetch the worker ID, using Kubernetes pod ID if available."""
    pod_id = os.getenv("POD_ID")
    if pod_id:
        return pod_id
    return f"local-{os.getpid()}"


class JobAction(Enum):
    PROCESS = "process"
    UPDATE = "update"  # aka reprocess
    DELETE = "delete"


class BNPDriver:

    def __init__(self, **kwargs):  # noqa
        """Initialize the mock driver with mock job data."""
        self.driver_type="MOCK"
        self.mock_jobs: List[Dict[str, Optional[str]]] = [
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
                ]
            )
        ]
        self.logs: List[Dict[str, Optional[str]]] = []
        self.current_job_id: Optional[int] = None
        self.current_src_path: Optional[str] = None
        self.current_job_id_and_url: Tuple[Optional[int], Optional[str]] = None, None
        self.current_worker_id: str = get_worker_id()
        self.current_processor_id: int = kwargs.get("processor_id", 1)

    def get_next_job(
        self, src_pattern: str = "MSIL1C"
    ) -> Tuple[Optional[int], Optional[str]]:
        """
        Fetch the next available job for the given processor.
        """
        for job in self.mock_jobs:
            if job["status"] == "pending":
                job["status"] = "processing"
                job["worker_id"] = self.current_worker_id
                self.current_job_id = job["job_id"]
                self.current_src_path = job["src_uri"]
                self.current_job_id_and_url = job["job_id"], job["src_uri"]

                log.info(
                    f"Mock get_next_job: Found job {job['job_id']} for processor {self.current_processor_id}"
                )
                return self.current_job_id_and_url

        self.current_job_id_and_url = None, None
        log.info(
            f"Mock get_next_job: No jobs available for processor {self.current_processor_id}. Total jobs: {len(self.mock_jobs)}"
        )
        return self.current_job_id_and_url

    @property
    def current_job(self) -> Tuple[Optional[int], Optional[str]]:
        return self.current_job_id_and_url

    @property
    def worker_id(self) -> str:
        return self.current_worker_id

    def report_finished(self, dst_path: str = "s3://dummy/path") -> None:
        """
        Mark the job as finished.
        """
        for job in self.mock_jobs:
            if job["job_id"] == self.current_job_id:
                job["status"] = "finished"
                log.info(
                    f"Mock report_finished: Job {self.current_job_id} marked as finished. Output at {dst_path}"
                )
                self.current_job_id = None
                self.current_src_path = None
                return

        log.warning(
            f"Mock report_finished: Job {self.current_job_id} not found. Unable to mark as finished."
        )

    def report_failure(self, message: str) -> None:
        """
        Mark the job as failed.
        """
        for job in self.mock_jobs:
            if job["job_id"] == self.current_job_id:
                job["status"] = "failed"
                log.warning(
                    f"Mock report_failure: Job {self.current_job_id} marked as failed. Reason: {message}"
                )
                return

        log.warning(
            f"Mock report_failure: Job {self.current_job_id} not found. Unable to mark as failed."
        )

    def store_log_message(self, message: str) -> None:
        """Stores a log message."""
        if not self.current_job_id:
            log.warning(
                "Mock store_log_message: Warning - No current job. Log message not associated with a job."
            )
        self.logs.append(
            {
                "job_id": self.current_job_id,
                "message": message,
                "l1c_source": self.current_src_path,
            }
        )
        log.info(
            f"Mock store_log_message: Log message stored for job {self.current_job_id}: {message}"
        )

    def get_processed_products_by_worker(self, worker_id: str) -> List[dict]:
        """
        Retrieves products processed by a specific worker.
        """
        processed = [
            job
            for job in self.mock_jobs
            if job.get("worker_id") == worker_id and job["status"] == "finished"
        ]
        if not processed:
            log.info(
                f"Mock get_processed_products_by_worker: No finished products found for worker {worker_id}"
            )
        else:
            log.info(
                f"Mock get_processed_products_by_worker: Found {len(processed)} products for worker {worker_id}"
            )
        return processed

    def get_logs_for_product(self, l1c_source: str) -> List[dict]:
        """
        Retrieves logs for a specific L1C product.
        """
        if not l1c_source:
            log.warning("Mock get_logs_for_product: Invalid l1c_source provided.")
            return []

        logs = [log for log in self.logs if log["l1c_source"] == l1c_source]
        log.info(
            f"Mock get_logs_for_product: Found {len(logs)} logs for product {l1c_source}"
        )
        return logs

    def close(self) -> None:
        """
        Close the mock database connection.
        """
        log.info("Mock close: Mock connection closed.")
