from enum import Enum
import os
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


class BNPDriver:

    def __init__(self):
        self.db_host = os.getenv("BNP_DB_HOSTNAME", "datasource.main.rise-ck8s.com")
        self.db_port = os.getenv("BNP_DB_PORT", 30103)
        self.db_user = os.getenv("BNP_DB_USERNAME", "bnp_db_rw")
        self.db_password = os.getenv("BNP_DB_PASSWORD", "bnp_password")
        self.db_name = os.getenv("BNP_DB_DATABASE", "datacube")

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
        processor_id: int,
        worker_id: str = get_worker_id(),
        src_pattern: str = "MSIL1C",
    ):
        """
        Fetch the next available job for the given processor.
        """
        query = """
        SELECT * FROM bnp.get_next_processing_job(%s, %s, %s)
        """
        with self.connection.cursor() as cur:
            cur.execute(query, (processor_id, worker_id, src_pattern))
            result = cur.fetchone()
            if result:
                self.current_job_id = result["job_id"]
                self.current_src_path = result["src_uri"]
                return result["job_id"], result["src_uri"]
            return None, None

    def report_finished(
        self, processor_id: str, job_id: int, dst_path: str = "s3://dummy/path"
    ):
        """
        Mark the job as finished.
        """
        query = """
        SELECT bnp.report_finished_processing(%s, %s, %s);
        """

        if self.current_job_id != job_id:
            raise ValueError(
                f"Reporting finished for job {job_id} but the "
                f"current job is {self.current_job_id}"
            )

        with self.connection.cursor() as cur:
            cur.execute(query, (processor_id, job_id, dst_path))
            self.connection.commit()
        self.current_job_id = None
        self.current_src_path = None

    def report_failure(self, processor_id: int, job_id: int, message: str):
        """
        Mark the job as failed.
        """
        query = """
        SELECT bnp.report_processing_failure(%s, %s, %s);
        """
        with self.connection.cursor() as cur:
            cur.execute(query, (processor_id, job_id, message))
            self.connection.commit()
    # Tracing interface to get full traceability on the processing
    def store_log_message(self, worker_id, baseline, job_id, l1c_source, message):
        """Stores a log message in the bnp.log table."""
        query = """
            SELECT bnp.store_log_message(%s, %s, %s, %s, %s);
        """
        with self.connection.cursor() as cur:
            cur.execute(query, (worker_id, baseline, job_id, l1c_source, message))
            self.connection.commit()

    def get_processed_products_by_worker(self, worker_id):
        """Retrieves products processed by a specific worker."""
        query = """
            SELECT * FROM bnp.get_processed_products_by_worker(%s);
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (worker_id,))
            return cur.fetchall()

    def get_logs_for_product(self, l1c_source):
        """Retrieves logs for a specific L1C product."""
        query = """
            SELECT * FROM bnp.get_logs_for_product(%s);
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (l1c_source,))
            return cur.fetchall()

    def close(self):
        """
        Close the database connection.
        """
        self.connection.close()
