import logging
import threading
import time
import requests

from db import DatabasePostgres


LOGGER: logging.Logger = logging.getLogger(__name__)


class ScrapperBase:
    def __init__(
            self,
            interval: float,
            name: str
            ) -> None:

        self._running_guard: threading.Condition = threading.Condition()
        self._running: bool = True
        self._interval: float = interval
        self._name = name

        self._thread: threading.Thread = threading.Thread(target=self._run)

    def do_thing(self) -> None:
        pass

    def trigger(self) -> None:
        """Force `do_thing` to be called immiedietely."""
        with self._running_guard:
            self._running_guard.notify_all()

    def is_running(self) -> None:
        with self._running_guard:
            return self._running

    def start(self) -> None:
        LOGGER.info(f'Scrapper {self._name} started')
        self._thread.start()

    def stop(self) -> None:
        with self._running_guard:
            self._running = False
            self._running_guard.notify_all()
        self._thread.join(timeout=10.0)

    def _run(self) -> None:
        while True:
            try:
                with self._running_guard:
                    if not self._running:
                        break
                    self._running_guard.wait(timeout=self._interval)
                    if not self._running:
                        break
                self.do_thing()
            except:
                LOGGER.exception(f'Scrapper {self._name} task failed')


class ScrapperGas(ScrapperBase):
    def __init__(
            self,
            interval: float,
            name: str,
            db: DatabasePostgres,
            ethscan_key: str
            ) -> None:
        super().__init__(interval, name)
        self._db = db
        self._key = ethscan_key


    def do_thing(self) -> None:
        gas_api = f'https://api.etherscan.io/api?module=gastracker&action=gasoracle&apikey={self._key}'
        gas_value = requests.post(gas_api)
        if gas_value.status_code == 200:
            gas_price = gas_value.json()['result']
            LOGGER.debug(f'Gas {gas_price}')
            self.add_gas_to_db(gas_price)
            LOGGER.info('Gas added to DB')
        else:
            LOGGER.warning(f'Failed with code {gas_value.status_code}')

    def add_gas_to_db(self, gas_result: dict) -> None:
        query = """
        INSERT INTO gas_price (gas_timestamp, last_block, safe_gas, norm_gas, fast_gas, base_fee, gas_used_ratio )
        VALUES (NOW(), %(LastBlock)s, %(SafeGasPrice)s, %(ProposeGasPrice)s, %(FastGasPrice)s, %(suggestBaseFee)s, %(gasUsedRatio)s)
        RETURNING id;
        """
        row_id = self._db.insert(query, gas_result)
        LOGGER.debug(f'Row: {row_id}')
