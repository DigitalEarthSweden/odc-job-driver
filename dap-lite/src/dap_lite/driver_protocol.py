from typing import Protocol, Tuple, List, Optional


class BNPDriverProtocol(Protocol):

    def __init__(self, **kwargs):
        """Flexible initialization for different drivers."""
        pass

    def get_next_job(self, src_pattern: str) -> Tuple[Optional[int], Optional[str]]:
        """Fetch the next available job for the given processor."""
        pass

    @property
    def current_job() -> Tuple[Optional[int], Optional[str]]:
        pass

    @property
    def worker_id() -> Optional[str]:
        pass

    def report_finished(self, dst_path: str) -> None:
        """Mark the job as finished."""
        pass

    def report_failure(self, message: str) -> None:
        """Mark the job as failed."""
        pass

    def report_skipped(self, message: str) -> None:
        """Mark the job as skipped"""
        pass

    def store_log_message(self, message: str) -> None:
        """Store a log message for the given job."""
        pass

    def get_processed_products_by_worker(self, worker_id: str) -> List[dict]:
        """Retrieve products processed by a specific worker."""
        pass

    def get_logs_for_product(self, l1c_source: str) -> List[dict]:
        """Retrieve logs for a specific L1C product."""
        pass

    def close(self) -> None:
        """Close the connection."""
        pass
