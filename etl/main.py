import requests
import time
import json
import os
import logging
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import IntegrityError

from models.main import ForexDate, ForexRate, Base
from utils.main import safe_parse_date, get_yearly_date_ranges

from transform import transform
from extract import extract
from load import load

error_log = []

engine = create_engine("mysql+mysqlconnector://root:rootpassword@localhost/forex_db")
SessionLocal = sessionmaker(bind=engine)

# Directory for checkpoints/logs
Path("checkpoints").mkdir(exist_ok=True)
Path("logs").mkdir(exist_ok=True)


def etl_range(date_start, date_end):
    print(f"{date_start} and {date_end}")
    print(f"[{date_start}] Running ETL thread...")

    Base.metadata.create_all(engine)

    page = 1
    per_page = 100  # Max allowed
    has_next = True
    # date_start = "2012-01-01"
    # date_start = "2024-12-30"
    # date_end = "2024-12-30"
    year_tag = date_start[:4]
    checkpoint_file = f"checkpoints/{year_tag}.done"
    log_file = f"logs/{year_tag}.log"

    # Skip if already processed
    if os.path.exists(checkpoint_file):
        print(f"[{year_tag}] Already processed. Skipping.")
        return

    # Set up per-thread logger
    logging.basicConfig(filename=log_file, level=logging.INFO, filemode="w")
    logger = logging.getLogger(year_tag)

    with SessionLocal() as session:
        while has_next:
            payload, pagination = extract(date_start, date_end, page, per_page)

            if not payload:
                print("No data found or end reached.")
                break

            transformed, error = transform(payload)
            error_log.extend(error)
            print(error_log)
            load(transformed, session)
            logger.info(f"Page {page} loaded with {len(transformed)} records.")

            if pagination and pagination["links"].get("next") is None:
                has_next = False
            else:
                page += 1
                time.sleep(0.5)

    # Create a checkpoint marker
    with open(checkpoint_file, "w") as f:
        f.write("done")


def main():
    ranges = get_yearly_date_ranges()
    futures = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        for start, end in ranges:
            futures.append(executor.submit(etl_range, start, end))
            print(
                f"[MAIN] Submitted {len(futures)} ETL jobs for years {ranges[0][0][:4]} to {ranges[-1][0][:4]}"
            )

            # Wait and print results
        for future in futures:
            try:
                future.result()
            except Exception as e:
                print(f"[ERROR] A thread failed: {e}")
            # executor.submit(etl_range, start, end)

    with open(f"errors_{datetime.now().date()}.json", "w") as f:
        json.dump(error_log, f, indent=2, default=str)


if __name__ == "__main__":
    main()
