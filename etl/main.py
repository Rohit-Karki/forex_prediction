import requests
import time
from datetime import datetime
from .models.main import ForexDate, ForexRate, Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


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
        date = datetime.strptime(item["date"], "%Y-%m-%d").date()
        published_on = datetime.strptime(item["published_on"], "%Y-%m-%d").date()
        modified_on = datetime.strptime(item["modified_on"], "%Y-%m-%d").date()

        # Create ForexDate instance
        forex_date = ForexDate(
            date=date, published_on=published_on, modified_on=modified_on
        )

        for rate in item.get("rates", []):
            currency = rate.get("currency", {})
            forex_rate = ForexRate(
                currency_name=currency.get("name", ""),
                currency_iso=currency.get("ISO3", ""),
                currency_unit=currency.get("unit", 1),
                buy_rate=rate.get("buy"),
                sell_rate=rate.get("sell"),
            )

            forex_date.rates.append(forex_rate)

        records.append(forex_date)
    return records

def load(transform, session):
    


def main():

    engine = create_engine("mysql+mysqlconnector://user:password@localhost/forex_db")
    SessionLocal = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    date_start = "2012-01-01"
    # date_start = "2024-12-30"
    date_end = "2024-12-30"
    per_page = 100  # Max allowed

    page = 1
    has_next = True
    
    with SessionLocal() as session:
        while has_next:
            payload, pagination = extract(date_start, date_end, page, per_page)

            transformed = transform(payload)
            load(transformed, session)

            if not payload:
                print("No data found or end reached.")
                break

            if pagination and pagination["links"].get("next") is None:
                has_next = False
            else:
                page += 1
                time.sleep(0.2)


if __name__ == "__main__":
    main()
