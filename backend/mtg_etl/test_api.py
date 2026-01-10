"""
Test MTG API only (without ClickHouse)
"""
import yaml
from mtg_api import MTGAPIClient
from logger import ETLLogger

# Load config
with open("config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

logger = ETLLogger(config["logging"])

# Pass full config to API client
api_config = {**config["api"], "retry": config["retry"], "poll": config["poll"]}
client = MTGAPIClient(api_config, logger)

# Test with a specific date
test_date = "2025-01-09"

print(f"Testing MTG API for date: {test_date}")
print("=" * 60)

success, data = client.get_parsed_data(test_date)

if success:
    print(f"SUCCESS! Got {len(data)} rows")
    if data:
        print("Sample row:")
        for k, v in list(data[0].items())[:10]:
            print(f"  {k}: {v}")
else:
    print(f"FAILED: {data}")
