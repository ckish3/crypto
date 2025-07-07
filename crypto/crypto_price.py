"""
This module contains the CryptoPrice class, which represents a row in the cryptocurrency_price table

"""

from database_base import Base
import database_actions
from sqlalchemy import String, Date, Float, text, BigInteger
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from typing import Dict
import datetime


class CryptoPrice(Base):
    __tablename__ = "cryptocurrency_price"
    __table_args__ = {"schema": database_actions.DatabaseActions.raw_schema_name}

    id: Mapped[str] = mapped_column(primary_key=True)
    currency_name: Mapped[str] = mapped_column(String(10))
    date: Mapped[datetime.date] = mapped_column(Date)
    price: Mapped[float]
    inserted_at: Mapped[datetime.datetime] = mapped_column(default=datetime.datetime.now())

    def get_max_date_by_currency(self, db_actions: database_actions.DatabaseActions) -> Dict[str, datetime.date]:
        """
        Retrieves a dictionary of the maximum date for each symbol in the stock_price table.

        Args:
            db_actions (database_actions.DatabaseAction): An instance of DatabaseActions for connecting to the database

        Returns:
            Dict[str, datetime.date]: A dictionary where the keys are the stock symbols and the values are the
                maximum dates in the stock_price table

        """
        engine = db_actions.get_engine()
        query = f"""
            SELECT currency_name, MAX(date) AS max_date
            FROM {self.__table_args__['schema']}.{self.__tablename__}
            GROUP BY currency_name"""

        max_dates = {}
        with engine.connect() as conn:
            result = conn.execute(text(query))
            for r in result.all():
                max_dates[r[0]] = r[1]

        return max_dates
    def __repr__(self) -> str:
        return f"CryptoPrice(id={self.id}, currency_name={self.currency_name}, date={self.date}, price={self.price})"

