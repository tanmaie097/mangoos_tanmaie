from pathlib import Path
import os
import logging
import requests
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv

# --- Load dotenv (global) ---
# Preference order:
# 1) env var GLOBAL_DOTENV_PATH (already exported in shell/run_fx.sh)
# 2) ~/.env
dotenv_path = os.getenv("GLOBAL_DOTENV_PATH", os.path.expanduser("~/.env"))
load_dotenv(dotenv_path)

# --- Config from environment ---
API_BASE = os.getenv("API_URL", "https://v6.exchangerate-api.com/v6")
API_KEY = os.getenv("API_KEY")
BASE_CURRENCY = os.getenv("BASE_CURRENCY", "USD")
OUT_BASE = Path(os.getenv("OUT_BASE", str(Path(__file__).parent.resolve())))
TIMEZONE = os.getenv("TIMEZONE", "")  # e.g. "Asia/Dubai" (optional)

# validate
if not API_KEY:
    raise RuntimeError("API_KEY not found in environment. Set in your global .env")

# prepare directories
LOG_DIR = OUT_BASE / "logs"
DATA_DIR = OUT_BASE / "data"
LOG_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

# setup logging
logging.basicConfig(
    filename=str(LOG_DIR / "fx.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def call_api():
    url = f"{API_BASE}/{API_KEY}/latest/{BASE_CURRENCY}"
    logging.info("Calling ExchangeRate API: %s", url)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()

def determine_local_date():
    # If TIMEZONE provided and zoneinfo is available, use it, otherwise fallback to UTC date.
    try:
        if TIMEZONE:
            # Python 3.9+ has zoneinfo
            from zoneinfo import ZoneInfo
            tz = ZoneInfo(TIMEZONE)
            local_dt = datetime.now(tz)
            return local_dt.date().isoformat()
    except Exception:
        logging.warning("Could not use zoneinfo for TIMEZONE=%s; falling back to UTC date", TIMEZONE)
    return datetime.utcnow().date().isoformat()

def transform_and_save(raw_json):
    conversion = raw_json.get("conversion_rates", {})
    updated_at = raw_json.get("time_last_update_utc", None)

    rows = []
    for tgt, rate in conversion.items():
        rows.append({
            "base": BASE_CURRENCY,
            "target": tgt,
            "rate": rate,
            "updated_at": updated_at,
            "ingested_at": datetime.now(timezone.utc).isoformat()
        })

    df = pd.DataFrame(rows)

    # file name by local date (uses TIMEZONE if available)
    date_str = determine_local_date()
    out_file = DATA_DIR / f"{date_str}.parquet"

    # save parquet (requires pyarrow or fastparquet)
    df.to_parquet(out_file, index=False)
    logging.info("Saved %d rows to %s", len(df), out_file)
    return out_file

def main():
    logging.info("=== FX pipeline start ===")
    try:
        raw = call_api()
        saved = transform_and_save(raw)
        print(f"Saved: {saved}")   # helpful for cron stdout logs
        logging.info("=== FX pipeline finished successfully ===")
    except Exception as e:
        logging.exception("FX pipeline failed: %s", e)
        raise

if __name__ == "__main__":
    main()
