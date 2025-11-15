"""
Resilience patterns setup for Tiki crawl products DAG
"""
from pathlib import Path

from .config import DATA_DIR
from .imports import import_resilience_patterns
from .utils import Variable


def setup_resilience_patterns(dag_file_dir):
    """Setup resilience patterns (circuit breaker, DLQ, graceful degradation)"""
    resilience_classes = import_resilience_patterns(dag_file_dir)
    
    CircuitBreaker = resilience_classes["CircuitBreaker"]
    get_dlq = resilience_classes["get_dlq"]
    get_service_health = resilience_classes["get_service_health"]
    
    # Circuit breaker cho Tiki API
    tiki_circuit_breaker = CircuitBreaker(
        failure_threshold=int(Variable.get("TIKI_CIRCUIT_BREAKER_FAILURE_THRESHOLD", default_var="5")),
        recovery_timeout=int(Variable.get("TIKI_CIRCUIT_BREAKER_RECOVERY_TIMEOUT", default_var="60")),
        expected_exception=Exception,
        name="tiki_api",
    )
    
    # Dead Letter Queue
    try:
        # Thử dùng Redis nếu có
        redis_url = Variable.get("REDIS_URL", default_var="redis://redis:6379/3")
        tiki_dlq = get_dlq(storage_type="redis", redis_url=redis_url)
    except Exception:
        # Fallback về file-based
        try:
            dlq_path = DATA_DIR / "dlq"
            tiki_dlq = get_dlq(storage_type="file", storage_path=str(dlq_path))
        except Exception:
            # Nếu không tạo được, dùng default
            tiki_dlq = get_dlq()
    
    # Graceful Degradation cho Tiki service
    service_health = get_service_health()
    tiki_degradation = service_health.register_service(
        name="tiki",
        failure_threshold=int(Variable.get("TIKI_DEGRADATION_FAILURE_THRESHOLD", default_var="3")),
        recovery_threshold=int(Variable.get("TIKI_DEGRADATION_RECOVERY_THRESHOLD", default_var="5")),
    )
    
    return {
        "tiki_circuit_breaker": tiki_circuit_breaker,
        "tiki_dlq": tiki_dlq,
        "tiki_degradation": tiki_degradation,
        **resilience_classes,  # Include all resilience classes for use in tasks
    }

