
import os
import logging
import sqlalchemy
from sqlalchemy.orm import Session
import datetime
import database_actions
import database_base
import crypto_price
import get_data


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def format_timestamp(timestamp: float) -> str:
    result = str(int(timestamp)) + '000'
    return result

def main():
    year_ago = format_timestamp((datetime.datetime.now() - datetime.timedelta(days=365)).timestamp())
    connection_string = os.getenv('DATABASE_URL')
    db = database_actions.DatabaseActions(connection_string)
    engine = db.get_engine()

    database_base.Base.metadata.create_all(bind=engine)

    api_key = os.getenv('COINCAP_KEY')

    currencies = ['bitcoin', 'ethereum']

    dates = crypto_price.CryptoPrice().get_max_date_by_currency(db)

    for currency in currencies:
        if currency in dates:
            start = format_timestamp(datetime.datetime.strptime(dates[currency], '%Y-%m-%d').date().timestamp())
        else:
            start = year_ago
        end = format_timestamp((datetime.datetime.now() - datetime.timedelta(days=1)).timestamp())
        logger.info(f'Start: {start}, End: {end}')
        new_entries = get_data.get_new_price_table_entries(start, end, api_key, currency)
        with Session(engine) as session:
            logger.info(f'Adding {len(new_entries)} new entries for {currency}')
            session.add_all(new_entries)
            session.commit()

if __name__ == '__main__':
    main()