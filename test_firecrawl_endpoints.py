import requests
import json

base_url = "http://localhost:3002"

# Test endpoints
endpoints = [
    "/health",
    "/v0/health",
    "/v1/health",
    "/api/health",
]

print("Testing Firecrawl endpoints:")
for endpoint in endpoints:
    try:
        r = requests.get(f"{base_url}{endpoint}", timeout=5)
        print(f"✓ {endpoint}: {r.status_code}")
        if r.status_code == 200:
            print(f"  Response: {r.text[:100]}")
    except Exception as e:
        print(f"✗ {endpoint}: {str(e)[:80]}")

# Try to get API info
print("\nTrying /v0/crawl/status to check API format:")
try:
    r = requests.get(f"{base_url}/v0/crawl/status/test", timeout=5)
    print(f"Status: {r.status_code}")
    print(f"Response: {r.text[:200]}")
except Exception as e:
    print(f"Error: {e}")

# Try scrape endpoint
print("\nTrying /v0/scrape:")
try:
    payload = {"url": "https://example.com"}
    r = requests.post(f"{base_url}/v0/scrape", json=payload, timeout=10)
    print(f"Status: {r.status_code}")
    print(f"Response: {r.text[:200]}")
except Exception as e:
    print(f"Error: {e}")

