from sqlalchemy import (
    Column,
    Integer,
    Float,
    String,
    Date,
    ForeignKey,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()


class ForexDate(Base):
    __tablename__ = "forex_dates"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False, unique=True)
    published_on = Column(Date, nullable=True)
    modified_on = Column(Date, nullable=True)

    # Relationship to ForexRate
    rates = relationship(
        "ForexRate", back_populates="forex_date", cascade="all, delete-orphan"
    )


class ForexRate(Base):
    __tablename__ = "forex_rates"

    id = Column(Integer, primary_key=True)
    forex_rate_id = Column(Integer, ForeignKey("forex_dates.id"), nullable=False)
    currency_name = Column(String, nullable=False)
    currency_iso = Column(String(3), nullable=False)  # ISO3 code
    currency_unit = Column(Integer, nullable=False)
    buy_rate = Column(Float, nullable=True)
    sell_rate = Column(Float, nullable=True)

    forex_date = relationship("ForexDate", back_populates="rates")

    # Avoid duplicate currency per day
    __table_args__ = (
        UniqueConstraint(
            "forex_date_id", "currency_iso", name="unique_currency_per_date"
        ),
    )

    def __repr__(self):
        return f"<ForexRate(currency={self.currency_iso}, buy={self.buy_rate}, sell={self.sell_rate})>"
