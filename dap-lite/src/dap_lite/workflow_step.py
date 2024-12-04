import time
from typing import Optional, Callable
from dap_lite import BNPDriverProtocol


class WorkFlowStepSkippedException(Exception):
    pass


class WorkflowStep:
    def __init__(
        self,
        name: str,
        bnp_driver: Optional[BNPDriverProtocol] = None,
        logger: Callable[[str], None] = print,
    ):
        """
        Initialize the context manager.

        Args:
            name (str): Name of the step.
            bnp_driver (Optional[BNPDriverProtocol]): BNPDriver instance for reporting success or failure.
            logger (Callable[[str], None], optional): Logger function for debugging logs. Defaults to print.

            with WorkflowStep('Downloading product') as step:
                # Do your stuff
                step.log.debug("Extra debug information here")
                step.log.info("Generally useful info")

            Success and Fail will be automatically reported to the driver if given.

        """
        self.name = name
        self.bnp_driver = bnp_driver
        self.logger = logger or print  # Default to print if no logger is provided
        self.start_time = None  # Initialize in `__enter__`

    def __enter__(self):
        self.start_time = time.time()
        self.log.info(f"Step '{self.name}' started.")
        return self  # Return the context manager instance

    def __exit__(self, exc_type, exc_value, traceback):
        elapsed_time = time.time() - self.start_time
        if exc_type is None:
            # No exception occurred
            self._report_success(elapsed_time)
        elif exc_type is WorkFlowStepSkippedException:
            # Handle the skipped exception as acceptable
            self._report_skipped(elapsed_time, exc_value)
            return True  # Suppress the exception
        else:
            # An exception occurred
            self._report_failure(elapsed_time, exc_value)
            return False  # Re-raise the exception

    def _report_success(self, elapsed_time: float):
        """Handles success reporting."""
        message = f"Step '{self.name}' succeeded in {elapsed_time:.2f} seconds."
        if self.bnp_driver:
            self.bnp_driver.store_log_message(
                message=message,
            )
        else:
            self.log.info(message)

    def _report_skipped(self, elapsed_time: float):
        """Handles skipped reporting."""
        message = f"Step '{self.name}' skipped after {elapsed_time:.2f} seconds."
        if self.bnp_driver:
            self.bnp_driver.store_log_message(
                message=message,
            )
        else:
            self.log.info(message)

    def _report_failure(self, elapsed_time: float, e: Exception):
        """Handles failure reporting."""
        reason = f"{type(e).__name__},{str(e)}" if e else "Unknown error"
        message = f"Step '{self.name}' failed after {elapsed_time:.2f} seconds. Reason: {reason}"
        if self.bnp_driver:
            self.bnp_driver.store_log_message(
                message=message,
            )
            self.bnp_driver.report_failure(reason)
        else:
            self.log.error(message)

    @property
    def log(self):
        """Provides a logging interface."""

        class LogWrapper:
            def __init__(self, logger_func):
                self.logger_func = logger_func

            def debug(self, message: str):
                self.logger_func(f"DEBUG: {message}")

            def info(self, message: str):
                self.logger_func(f"INFO: {message}")

            def warning(self, message: str):
                self.logger_func(f"WARNING: {message}")

            def error(self, message: str):
                self.logger_func(f"ERROR: {message}")

        return LogWrapper(self.logger) if self.logger == print else self.logger


if __name__ == "__main__":
    import time  # noqa

    with WorkflowStep("My first baby steps should take half a second") as step:
        step.log.debug("Some extra debug logging here!")
        time.sleep(0.5)

    with WorkflowStep("My second baby steps that fails after 2.3 seconds") as step:
        time.sleep(2.3)
        raise ValueError("Horrible things happens here!")
