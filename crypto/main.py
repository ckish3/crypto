
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


def main():
    year_ago = str((datetime.datetime.now() - datetime.timedelta(days=365)).timestamp())
    connection_string = os.getenv('DATABASE_URL')
    db = database_actions.DatabaseActions(connection_string)
    engine = db.get_engine()

    database_base.Base.metadata.create_all(bind=engine)

    api_key = os.getenv('COINCAP_KEY')

    curriencies = ['bitcoin', 'ethereum']

    dates = crypto_price.CryptoPrice.get_max_date_by_currency(db)

    for currency in curriencies:
        if currency in dates:
            start = str(datetime.datetime.strptime(dates[currency], '%Y-%m-%d').date().timestamp()) + '000'
        else:
            start = year_ago
        end = str((datetime.datetime.now() - datetime.timedelta(days=1)).timestamp())
        new_entries = get_data.get_new_price_table_entries(start, end, api_key, currency)
        with Session(engine) as session:
            logger.info(f'Adding {len(new_entries)} new entries for {currency}')
            session.add_all(new_entries)
            session.commit()

if __name__ == '__main__':
    main()