from models.main import ForexDate, ForexRate, Base
from utils.main import safe_parse_date, get_yearly_date_ranges


def transform(
    payload,
    error_log=[],
):
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
    return records, error_log or []
