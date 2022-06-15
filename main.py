import logging
import threading
import time
import requests


from config import Config
from db import DatabasePostgres
from scrapper import ScrapperGas


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s %(message)s"
)
LOGGER: logging.Logger = logging.getLogger(__name__)

def main() -> None:
    CONF=Config()
    DB_PSQL = DatabasePostgres(CONF.postgres())
    gas_scrapper: Scrapper = ScrapperGas(interval=5.0, name='EtherscanGas', db=DB_PSQL, ethscan_key=CONF.etherscan())
    gas_scrapper.start()

if __name__ == "__main__":
    main()

