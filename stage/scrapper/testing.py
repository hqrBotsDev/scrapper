import datetime
import logging
import threading
import time
import requests
from db import DatabasePostgres, DatabaseMongo
from config import Config
from opensea_api import OpenseaAPI

logging.basicConfig(
            format='%(asctime)s - %(funcName)s - %(levelname)s - %(message)s',
                level=logging.ERROR
                )
LOGGER = logging.getLogger('statsScrapper')

#file_log = logging.FileHandler('/root/CGC/WorkieTemp/api/stats_scrapper.log')
#file_log.setLevel(logging.DEBUG)
#LOGGER.addHandler(file_log)

CONF=Config()

DB_PSQL = DatabasePostgres(CONF.postgres())
DB_MONGO = DatabaseMongo(CONF.mongo())
OS_API = OpenseaAPI(CONF.opensea_api())


class Event():
    def __init__(self, data: dict) -> None:
        self.data = data

    def asset_id(self) -> str:
        field = self.data["asset"]["id"]
        return str(field)

    def asset_name(self) -> str:
        field = self.data["asset"]["name"]
        return field

    def asset_contract(self) -> str:
        field = self.data["asset"]["asset_contract"]["address"]
        return field

    def event_type(self) -> str:
        field = self.data["event_type"]
        return field

    def event_timestamp(self) -> str:
        field = self.data["event_timestamp"]
        return field

    def event_id(self) -> str:
        field = self.data["id"]
        return field

    def starting_price(self) -> float:
        field = self.data["starting_price"]
        if isinstance(field, str):
            field = float(field) * pow(10, -18)
        return field

    def listing_time(self) -> datetime.datetime:
        field = self.data["listing_time"]
        return field

    def listing_duration(self) -> str:
        field = self.data["duration"]
        if isinstance(field, str):
            field = int(field)
        return field

    def listing_end(self) -> str:
        dur = self.listing_duration()
        if isinstance(dur, int):
            d = self.listing_time() 
            try:
                fulldate = datetime.datetime.strptime(d, '%Y-%m-%dT%H:%M:%S')
            except:
                print(f'DATE -> {d}')
                fulldate = datetime.datetime.strptime(d, '%Y-%m-%dT%H:%M:%S.%f')
            fulldate = fulldate + datetime.timedelta(seconds=dur)
            return str(fulldate)
        else:
            return None

    def bid_amount(self) -> float:
        field = self.data["bid_amount"]
        if  isinstance(field, str):
            field = float(field) * pow(10, -18)
        return field

    def total_price(self) -> float:
        field = self.data["total_price"]
        if  isinstance(field, str):
            field = float(field) * pow(10, -18)
        return field

    def as_dict(self) -> dict:
        result = {
                "asset_id":         self.asset_id(),
                "asset_name":       self.asset_name(),
                "asset_contract":   self.asset_contract(),
                "event_type":       self.event_type(),
                "event_timestamp":  self.event_timestamp(),
                "event_id":         self.event_id(),
                "starting_price":   self.starting_price(),
                "listing_time":     self.listing_time(),
                "listing_end":      self.listing_end(),
                "bid_amount":       self.bid_amount(),
                "total_price":      self.total_price()
                }
        return result

    def raw(self) -> dict:
        return self.data

    def __str__(self) -> str:
        return f'{self.asset_contract()} - {self.asset_id()} -> {self.total_price()} - {self.starting_price()} - {self.bid_amount()}'
        #return f'{self.asset_contract()} - {self.asset_id()} -> {self.bid_amount()}'


def init_contract() -> None:

    #start_at = datetime(2021, 10, 5, 3, 25, tzinfo=timezone.utc)
    start_at = datetime.datetime.now()
    ev = OS_API.events_backfill(asset_contract_address='0x7a1eb86c35136143dda358d4a2d8ac25c4902388', start=start_at)
    #ev = OS_API.contract('0x5423856728612f358c84a37805799755be2722c8')
    i = 0
    for x in ev:
        print(f'Type -> {type(x)}')
        print(f'i -> {i}')
        if x is not None:
            for e in x["asset_events"]:
                try: 
                    event = Event(e)
                    event_dict = event.as_dict()
                    vals = str(tuple(event_dict.values())).replace("None", "NULL")
                    print(f'Mongo')
                    DB_MONGO.insert(collection='events_0x7a1eb86c35136143dda358d4a2d8ac25c4902388', asset=event.raw())

                    query = f"""
                    INSERT INTO events_0x7a1eb86c35136143dda358d4a2d8ac25c4902388 (asset_id, asset_name, asset_contract, event_type, event_timestamp, event_id, starting_price, listing_time, listing_end, bid_amount, total_price) 
                    VALUES {vals} RETURNING *;
                    """
                    #print(query)
                    print(f'Postgress')
                    DB_PSQL.insert(query)
                except Exception as ex:
                    print('Sth WRONG')
                    print(ex)

        i+=1

    print(f'Total -> {i}')

def gather_contract() -> None:

    #start_at = datetime(2021, 10, 5, 3, 25, tzinfo=timezone.utc)
    query = f"""
    SELECT event_timestamp  FROM events_0x7a1eb86c35136143dda358d4a2d8ac25c4902388 ORDER BY event_timestamp DESC LIMIT 1;
    """
    result = DB_PSQL.get_rows(query)
    start_at = result[0]['event_timestamp']
    ev = OS_API.events(asset_contract_address='0x7a1eb86c35136143dda358d4a2d8ac25c4902388', start=start_at)
    #ev = OS_API.contract('0x5423856728612f358c84a37805799755be2722c8')
    i = 0
    for x in ev:
        if x is not None:
            for e in x["asset_events"]:
                try: 
                    event = Event(e)
                    event_dict = event.as_dict()
                    vals = str(tuple(event_dict.values())).replace("None", "NULL")
                    DB_MONGO.insert(collection='events_0x7a1eb86c35136143dda358d4a2d8ac25c4902388', asset=event.raw())

                    query = f"""
                    INSERT INTO events_0x7a1eb86c35136143dda358d4a2d8ac25c4902388 (asset_id, asset_name, asset_contract, event_type, event_timestamp, event_id, starting_price, listing_time, listing_end, bid_amount, total_price) 
                    VALUES {vals} RETURNING *;
                    """
                    #print(query)
                    DB_PSQL.insert(query)
                except Exception as ex:
                    print('Sth WRONG')
                    print(ex)
                i+=1

    print(f'Total -> {i}')


if __name__ == "__main__":
    gather_contract()
    #init_contract()
