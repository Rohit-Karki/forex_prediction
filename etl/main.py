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


error_log = []

engine = create_engine("mysql+mysqlconnector://root:rootpassword@localhost/forex_db")
SessionLocal = sessionmaker(bind=engine)

# Directory for checkpoints/logs
Path("checkpoints").mkdir(exist_ok=True)
Path("logs").mkdir(exist_ok=True)


def extract(
    date_start,
    date_end,
    page_number=1,
    per_page_size=10,
):
    url = "https://nrb.org.np/api/forex/v1/rates"
    params = {
        "page": page_number,
        "per_page": per_page_size,
        "from": date_start,
        "to": date_end,
    }

    try:
        response = requests.get(url, params=params)
        data = response.json()

        payload = data.get("data", {}).get("payload", [])
        pagination = data.get("pagination", {})
        print(f"Page {page_number}: Retrieved {len(payload)} records")
        print(f"result is {payload} and pagination is {pagination}")
        return payload, pagination

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Page {page_number} failed: {e}")
        return [], {"links": {"next": None}}


def transform(payload):
    records = []

    for item in payload:

        date = safe_parse_date(item["date"])
        published_on = safe_parse_date(item["published_on"])
        modified_on = safe_parse_date(item["modified_on"])

        if not published_on:
            error_log.append(
                {
                    "error": "Invalid date format",
                    "field": "published_on",
                    "raw_value": item["published_on"],
                    "context": item,
                }
            )
            print(f"[SKIP] Skipping record with invalid main date: {item}")
            continue
        elif not modified_on:
            error_log.append(
                {
                    "error": "Invalid date format",
                    "field": "modified_on",
                    "raw_value": item["modified_on"],
                    "context": item,
                }
            )
            print(f"[SKIP] Skipping record with invalid main date: {item}")
            continue
        elif not date:
            error_log.append(
                {
                    "error": "Invalid date format",
                    "field": "modified_on",
                    "raw_value": item["date"],
                    "context": item,
                }
            )
            print(f"[SKIP] Skipping record with invalid main date: {item}")
            continue

        # Create ForexDate instance
        forex_date = ForexDate(
            date=date, published_on=published_on, modified_on=modified_on
        )

        for rate in item.get("rates", []):
            currency = rate.get("currency", {})

            iso3 = currency.get("iso3") or currency.get(
                "ISO3"
            )  # sometimes API uses lowercase
            if not iso3:
                print(
                    f"[WARNING] Skipping rate with missing ISO3 on date {item['date']}"
                )
                continue
            forex_rate = ForexRate(
                currency_name=currency.get("name", ""),
                currency_iso=iso3,
                currency_unit=currency.get("unit", 1),
                buy_rate=rate.get("buy"),
                sell_rate=rate.get("sell"),
            )

            forex_date.rates.append(forex_rate)

        records.append(forex_date)
    return records


def load(transformed_records, session: Session):
    for forex_date in transformed_records:
        try:
            # Check if this date already exists
            existing = session.execute(
                select(ForexDate).where(ForexDate.date == forex_date.date)
            ).scalar_one_or_none()

            if existing:
                print(f"Data for {forex_date.date} already exists. Skipping.")
                continue

            # Add the new ForexDate and its rates
            session.add(forex_date)
            session.commit()
            print(f"Inserted data for {forex_date.date}")

        except IntegrityError as e:
            session.rollback()
            print(f"[ERROR] Integrity issue on {forex_date.date}: {e}")
        except Exception as e:
            session.rollback()
            print(f"[ERROR] Unexpected error on {forex_date.date}: {e}")


def etl_range(date_start, date_end):
    print(f"{date_start} and {date_end}")
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

            transformed = transform(payload)
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
