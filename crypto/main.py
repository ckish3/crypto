
import os
import logging
import sqlalchemy
from sqlalchemy.orm import Session
import database_actions
import database_base


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def main():
    connection_string = os.getenv('DATABASE_URL')
    db = database_actions.DatabaseActions(connection_string)
    engine = db.get_engine()

    database_base.Base.metadata.create_all(bind=engine)

  
if __name__ == '__main__':
    main()