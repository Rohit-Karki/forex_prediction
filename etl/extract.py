import requests


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
        # print(f"result is {payload} and pagination is {pagination}")
        return payload, pagination

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Page {page_number} failed: {e}")
        return [], {"links": {"next": None}}
