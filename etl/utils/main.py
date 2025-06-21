from datetime import datetime, date, timedelta


def safe_parse_date(date_str):
    if not date_str:
        return None
    # Normalize separator
    date_str = date_str.replace(".", "-")

    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    print(f"[WARNING] Skipping invalid date: {date_str}")
    return None


def get_yearly_date_ranges(start_year=2022, end_year=None):
    if end_year is None:
        end_year = date.today().year + 1  # Include current year
    ranges = []

    for year in range(start_year, end_year):
        start = date(year, 1, 1)
        end = date(year + 1, 1, 1) - timedelta(days=1)  # inclusive end
        ranges.append((start.isoformat(), end.isoformat()))

    return ranges
