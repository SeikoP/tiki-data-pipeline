"""
Shared state for Tiki crawl products DAG
Stores global objects that need to be accessed by tasks
"""
# These will be set by the main DAG file
tiki_circuit_breaker = None
tiki_dlq = None
tiki_degradation = None
crawl_category_products = None
get_page_with_requests = None
parse_products_from_html = None
get_total_pages = None
crawl_product_detail_with_selenium = None
extract_product_detail = None
CircuitBreakerOpenError = None
classify_error = None
DataAggregator = None
AISummarizer = None
DiscordNotifier = None


def set_shared_state(
    tiki_circuit_breaker_val=None,
    tiki_dlq_val=None,
    tiki_degradation_val=None,
    crawl_category_products_val=None,
    get_page_with_requests_val=None,
    parse_products_from_html_val=None,
    get_total_pages_val=None,
    crawl_product_detail_with_selenium_val=None,
    extract_product_detail_val=None,
    CircuitBreakerOpenError_val=None,
    classify_error_val=None,
    DataAggregator_val=None,
    AISummarizer_val=None,
    DiscordNotifier_val=None,
):
    """Set shared state values"""
    global (
        tiki_circuit_breaker,
        tiki_dlq,
        tiki_degradation,
        crawl_category_products,
        get_page_with_requests,
        parse_products_from_html,
        get_total_pages,
        crawl_product_detail_with_selenium,
        extract_product_detail,
        CircuitBreakerOpenError,
        classify_error,
        DataAggregator,
        AISummarizer,
        DiscordNotifier,
    )
    
    if tiki_circuit_breaker_val is not None:
        tiki_circuit_breaker = tiki_circuit_breaker_val
    if tiki_dlq_val is not None:
        tiki_dlq = tiki_dlq_val
    if tiki_degradation_val is not None:
        tiki_degradation = tiki_degradation_val
    if crawl_category_products_val is not None:
        crawl_category_products = crawl_category_products_val
    if get_page_with_requests_val is not None:
        get_page_with_requests = get_page_with_requests_val
    if parse_products_from_html_val is not None:
        parse_products_from_html = parse_products_from_html_val
    if get_total_pages_val is not None:
        get_total_pages = get_total_pages_val
    if crawl_product_detail_with_selenium_val is not None:
        crawl_product_detail_with_selenium = crawl_product_detail_with_selenium_val
    if extract_product_detail_val is not None:
        extract_product_detail = extract_product_detail_val
    if CircuitBreakerOpenError_val is not None:
        CircuitBreakerOpenError = CircuitBreakerOpenError_val
    if classify_error_val is not None:
        classify_error = classify_error_val
    if DataAggregator_val is not None:
        DataAggregator = DataAggregator_val
    if AISummarizer_val is not None:
        AISummarizer = AISummarizer_val
    if DiscordNotifier_val is not None:
        DiscordNotifier = DiscordNotifier_val

