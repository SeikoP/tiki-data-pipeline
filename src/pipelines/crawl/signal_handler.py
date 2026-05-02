"""Graceful shutdown handler for crawl pipelines.

Handles SIGTERM/SIGINT to allow in-progress operations
to complete before exiting. Designed for Docker/Airflow
environments where tasks receive SIGTERM before SIGKILL.

Usage:
    from pipelines.crawl.signal_handler import GracefulShutdown

    shutdown = GracefulShutdown()

    for item in items:
        if shutdown.should_exit:
            logger.info("Shutting down gracefully...")
            break
        process(item)
"""

import logging
import signal
import threading
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)


class GracefulShutdown:
    """Thread-safe graceful shutdown coordinator.

    Registers signal handlers for SIGTERM and SIGINT.
    Workers check ``should_exit`` to stop processing new items
    while allowing current work to finish.
    """

    def __init__(self, cleanup_fn: Callable[[], Any] | None = None):
        """
        Args:
            cleanup_fn: Optional callback invoked once on first signal.
        """
        self._exit_event = threading.Event()
        self._cleanup_fn = cleanup_fn
        self._cleanup_done = False
        self._lock = threading.Lock()

        # Only register in main thread
        if threading.current_thread() is threading.main_thread():
            for sig in (signal.SIGTERM, signal.SIGINT):
                try:
                    signal.signal(sig, self._handle_signal)
                except (OSError, ValueError):
                    # Cannot set signal handler in non-main thread or
                    # when running under some test frameworks
                    pass

    @property
    def should_exit(self) -> bool:
        """Check whether a shutdown signal has been received."""
        return self._exit_event.is_set()

    def request_exit(self) -> None:
        """Programmatically request shutdown (e.g. from a watchdog)."""
        self._exit_event.set()
        self._run_cleanup()

    def _handle_signal(self, signum: int, _frame: Any) -> None:
        sig_name = signal.Signals(signum).name
        logger.warning("Received %s — initiating graceful shutdown", sig_name)
        self._exit_event.set()
        self._run_cleanup()

    def _run_cleanup(self) -> None:
        with self._lock:
            if self._cleanup_done or self._cleanup_fn is None:
                return
            self._cleanup_done = True
        try:
            self._cleanup_fn()
        except Exception:
            logger.exception("Error during shutdown cleanup")
