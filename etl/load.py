from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from models.main import ForexDate, ForexRate, Base
from utils.main import safe_parse_date, get_yearly_date_ranges


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
            # print(f"Inserted data for {forex_date.date}")

        except IntegrityError as e:
            session.rollback()
            print(f"[ERROR] Integrity issue on {forex_date.date}: {e}")
        except Exception as e:
            session.rollback()
            print(f"[ERROR] Unexpected error on {forex_date.date}: {e}")
