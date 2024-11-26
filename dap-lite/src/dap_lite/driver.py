from enum import Enum
import os
from typing import List, Optional, Tuple
import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.extras import RealDictCursor


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


class BNPDriverException(Exception):
    pass


class BNPDriver:

    def __init__(self, **kwargs):  # noqa
        self.db_host = os.getenv("BNP_DB_HOSTNAME", "datasource.main.rise-ck8s.com")
        self.db_port = os.getenv("BNP_DB_PORT", 30103)
        self.db_user = os.getenv("BNP_DB_USERNAME", "bnp_db_rw")
        self.db_password = os.getenv("BNP_DB_PASSWORD", "bnp_password")
        self.db_name = os.getenv("BNP_DB_DATABASE", "datacube")
        self.current_worker_id: str = get_worker_id()
        self.current_job_id = None
        self.current_src_path = None
        self.processor_id = kwargs.get("processor_id", 1)
        if not self.db_password:
            raise ValueError("BNP_DB_PASSWORD is not set in the environment variables")

        self.connection = psycopg2.connect(
            host=self.db_host,
            port=self.db_port,
            user=self.db_user,
            password=self.db_password,
            dbname=self.db_name,
            cursor_factory=DictCursor,
        )

    def get_next_job(
        self,
        src_pattern: str = "MSIL1C",
    ) -> Tuple[Optional[int], Optional[str]]:
        """
        Fetch the next available job for the given processor.
        """
        query = """
        SELECT * FROM bnp.get_next_processing_job(%s, %s, %s)
        """
        with self.connection.cursor() as cur:
            cur.execute(query, (self.processor_id, self.current_worker_id, src_pattern))
            result = cur.fetchone()
            if result:
                self.current_job_id = result["job_id"]
                self.current_src_path = result["src_uri"]
            else:
                self.current_job_id = None
                self.current_src_path = None
        return self.current_job_id, self.current_src_path

    @property
    def current_job(self) -> Tuple[Optional[int], Optional[str]]:
        return self.current_job_id, self.current_src_path

    @property
    def worker_id(self) -> str:
        return self.current_worker_id

    def report_finished(self, dst_path: str = "s3://dummy/path"):
        """
        Mark the job as finished.
        """
        query = """
        SELECT bnp.report_finished_processing(%s, %s, %s);
        """
        with self.connection.cursor() as cur:
            cur.execute(query, (self.processor_id, self.current_job_id, dst_path))
            self.connection.commit()
        self.current_job_id = None
        self.current_src_path = None

    def report_failure(self, message: str):
        """
        Mark the job as failed.
        """
        query = """
        SELECT bnp.report_processing_failure(%s, %s, %s);
        """
        with self.connection.cursor() as cur:
            cur.execute(query, (self.processor_id, self.current_job_id, message))
            self.connection.commit()

    # Tracing interface to get full traceability on the processing
    def store_log_message(self, message: str) -> None:
        """Stores a log message in the bnp.log table."""
        query = """
            SELECT bnp.store_log_message(%s, %s);
        """
        if not self.current_job_id:
            raise BNPDriverException(
                "Current Job is either already reported or never started."
            )
        with self.connection.cursor() as cur:
            cur.execute(query, (self.current_job_id, message))
            self.connection.commit()

    def get_processed_products_by_worker(self, worker_id: str) -> List[dict]:
        """Retrieves products processed by a specific worker."""
        query = """
            SELECT * FROM bnp.get_processed_products_by_worker(%s);
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (worker_id,))
            return cur.fetchall()

    def get_logs_for_product(self, l1c_source: str) -> List[dict]:
        """Retrieves logs for a specific L1C product."""
        query = """
            SELECT * FROM bnp.get_logs_for_product(%s);
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (l1c_source,))
            return cur.fetchall()

    def close(self) -> None:
        """
        Close the database connection.
        """
        self.connection.close()
