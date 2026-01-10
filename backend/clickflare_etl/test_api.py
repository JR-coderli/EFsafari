"""
Clickflare API Test Script
Tests API connection and data retrieval
"""
import yaml
from cf_api import ClickflareAPI


def load_config(config_file="config.yaml"):
    """Load configuration from YAML file"""
    with open(config_file, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def test_connection():
    """Test API connection"""
    print("=" * 60)
    print("Clickflare API Connection Test")
    print("=" * 60)

    config = load_config()
    api = ClickflareAPI(config)

    success, message = api.test_connection()

    if success:
        print(f"[OK] {message}")
        return True
    else:
        print(f"[FAIL] {message}")
        return False


def test_fetch_report():
    """Test fetching report data"""
    print("\n" + "=" * 60)
    print("Clickflare API Report Fetch Test")
    print("=" * 60)

    config = load_config()
    api = ClickflareAPI(config)

    # Fetch data for a single day
    result = api.fetch_report(
        start_date="2026-01-09 00:00:00",
        end_date="2026-01-10 23:59:59",
        group_by=["date", "trafficSourceID", "offerID"],
        metrics=["cm_uniqueVisits", "cm_uniqueClicks", "cm_conversionsApprovedSales", "cm_revenue"],
        page=1,
        page_size=10
    )

    if result is None:
        print("[FAIL] No response from API")
        return False

    if result.get("error"):
        print(f"[FAIL] API Error: {result.get('message')}")
        return False

    items = result.get("items", [])
    totals = result.get("totals", {})

    print(f"[OK] Successfully fetched data")
    print(f"  - Items returned: {len(items)}")
    print(f"  - Sample data:")
    for item in items[:3]:
        print(f"    {item}")
    print(f"  - Totals: {totals}")

    return True


def test_fetch_all_pages():
    """Test fetching all pages"""
    print("\n" + "=" * 60)
    print("Clickflare API Pagination Test")
    print("=" * 60)

    config = load_config()
    api = ClickflareAPI(config)

    all_items, error = api.fetch_all_pages(
        start_date="2026-01-09 00:00:00",
        end_date="2026-01-10 23:59:59",
        group_by=["date", "trafficSourceID"],
        metrics=["cm_uniqueVisits", "cm_uniqueClicks"],
        page_size=100,
        max_pages=5
    )

    if error:
        print(f"[FAIL] Error: {error}")
        return False

    print(f"[OK] Successfully fetched all pages")
    print(f"  - Total items: {len(all_items)}")

    return True


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print(" Clickflare API Test Suite")
    print("=" * 60)

    tests = [
        ("Connection Test", test_connection),
        ("Report Fetch Test", test_fetch_report),
        ("Pagination Test", test_fetch_all_pages),
    ]

    passed = 0
    failed = 0

    for name, test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"[FAIL] {name} raised exception: {e}")
            failed += 1

    print("\n" + "=" * 60)
    print(f" Test Results: {passed} passed, {failed} failed")
    print("=" * 60)
