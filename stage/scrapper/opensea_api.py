import time
import requests
from datetime import datetime
from requests.adapters import HTTPAdapter, Retry

import utils


class OpenseaAPI:

    MAX_EVENT_ITEMS = 300

    def __init__(self, config: dict) -> None:
        """Class to interact with OpenSea API

        """

        self.base_url = config['OPENSEA_API_BASE_URL']
        self.version = config['OPENSEA_API_VERSION']
        self.key = config['OPENSEA_API_KEY']

    def _make_request(self, endpoint=None, params=None, return_response=False):
        """Makes a request to the OpenSea API and returns either a response
        object or dictionary.

        Args:
            endpoint (str, optional): API endpoint to use for the request.
            params (dict, optional): Query parameters to include in the
            request. Defaults to None.
            export_file_name (str, optional): In case you want to download the
            data into a file,
            specify the filename here. Eg. 'export.json'. Be default, no file
            is created.
            return_response (bool, optional): Set it True if you want it to
            return the actual response object.
            By default, it's False, which means a dictionary will be returned.
            next_url (str, optional): If you want to paginate, provide the
            `next` value here (this is a URL) OpenSea provides in the response.
            If this argument is provided, `endpoint` will be ignored.

        Raises:
            ValueError: returns the error message from the API in case one
            (or more) of your request parameters are incorrect.
            HTTPError: Unauthorized access. Try using an API key.
            ConnectionError: your request got blocked by the server (try
            using an API key if you keep getting this error)
            TimeoutError: your request timed out (try rate limiting)

        Returns:
            Data sent back from the API. Either a response or dict object
            depending on the `return_response` argument.
        """
        if endpoint is None:
            raise ValueError(
                """You need to define an `endpoint` when
                             making a request!"""
            )

        headers = {"X-API-KEY": self.key}
        url = utils.urljoin(self.base_url, self.version, endpoint)


        session = requests.Session()

        retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 408, 429, 502, 503, 504 ])
        adapter = HTTPAdapter(max_retries=retries)
        for prefix in "http://", "https://":
           session.mount(prefix, adapter)


        response = session.get(url, headers=headers, params=params)
        if response.status_code == 400:
            raise ValueError(response.text)
        elif response.status_code == 401:
            raise requests.exceptions.HTTPError(response.text)
        elif response.status_code == 403:
            raise ConnectionError("The server blocked access.")
        elif response.status_code == 495:
            raise requests.exceptions.SSLError("SSL certificate error")
        elif response.status_code == 504:
            raise TimeoutError("The server reported a gateway time-out error.")

        if return_response:
            return response
        return response.json()

    def events(
            self,
            start,
            rate_limiting=0.3,
            asset_contract_address=None,
            collection_slug=None,
            token_id=None,
            account_address=None,
            event_type=None,
            only_opensea=False,
            auction_type=None,
            limit=None,
            collection_editor=None,
        ):
            """
            Get all event from given time. 
            """
            if not isinstance(start, datetime):
                raise ValueError("`start` must be datetime objects")

            query_params = {
                "asset_contract_address": asset_contract_address,
                "collection_slug": collection_slug,
                "token_id": token_id,
                "account_address": account_address,
                "event_type": event_type,
                "only_opensea": only_opensea,
                "auction_type": auction_type,
                "limit": self.MAX_EVENT_ITEMS if limit is None else limit,
                "occurred_after": start,
                "collection_editor": collection_editor,
            }

            # make the first request to get the `next` cursor value
            first_request = self._make_request("events", query_params)
            yield first_request
            next_cur = first_request["next"]
            query_params["cursor"] = next_cur

            # paginate
            while True:
                time.sleep(rate_limiting)
                data = self._make_request("events", query_params)

                # update the `next` parameter for the upcoming request
                next_cur = data["next"]
                query_params["cursor"] = next_cur

                if next_cur is not None:
                    yield data
                else:
                    break
            yield None

    def events_backfill(
            self,
            start,
            rate_limiting=0.3,
            asset_contract_address=None,
            collection_slug=None,
            token_id=None,
            account_address=None,
            event_type=None,
            only_opensea=False,
            auction_type=None,
            limit=None,
            collection_editor=None,
        ):
            """
            EXPERIMENTAL FUNCTION!
            Expected behaviour:
            Download events and paginate over multiple pages until the given
            time is reached. Pagination happens **backwards** (so you can use this
            function to **backfill** events data eg. into a database) from `start`
            until `until`.
            The function returns a generator.
            Args:
                start (datetime): A point in time where you want to start
                downloading data from.
                until (datetime): How much data do you want?
                How much do you want to go back in time? This datetime value will
                provide that threshold.
                rate_limiting (int, optional): Seconds to wait between requests.
                Defaults to 2.
                Other parameters are available (all of the `events` endpoint
                parameters) and they are documented in the OpenSea docs
                https://docs.opensea.io/reference/retrieving-asset-events
            Yields:
                dictionary: event data
            """
            if not isinstance(start, datetime):
                raise ValueError("`start` must be datetime objects")

            query_params = {
                "asset_contract_address": asset_contract_address,
                "collection_slug": collection_slug,
                "token_id": token_id,
                "account_address": account_address,
                "event_type": event_type,
                "only_opensea": only_opensea,
                "auction_type": auction_type,
                "limit": self.MAX_EVENT_ITEMS if limit is None else limit,
                "occurred_before": start,
                "collection_editor": collection_editor,
            }

            # make the first request to get the `next` cursor value
            first_request = self._make_request("events", query_params)
            yield first_request
            next_cur = first_request["next"]
            query_params["cursor"] = next_cur

            # paginate
            while True:
                time.sleep(rate_limiting)
                data = self._make_request("events", query_params)

                # update the `next` parameter for the upcoming request
                next_cur = data["next"]
                query_params["cursor"] = next_cur

                if next_cur is not None:
                    yield data
                else:
                    break
            yield None

    def contract(self, asset_contract_address):
        """Fetches asset contract data from the API.
        OpenSea API Asset Contract query parameters:
        https://docs.opensea.io/reference/retrieving-a-single-contract
        Args:
            asset_contract_address (str): Contract address of the NFT.
            export_file_name (str, optional): Exports the JSON data into a the
            specified file.
        Returns:
            [dict]: Single asset contract data
        """
        endpoint = f"asset_contract/{asset_contract_address}"
        return self._make_request(endpoint)

    def collection(self, collection_slug):
        """Fetches collection data from the API.
        OpenSea API Collection query parameters:
        https://docs.opensea.io/reference/retrieving-a-single-collection
        Args:
            collection_slug (str): Collection slug (unique identifer)
            export_file_name (str, optional): Exports the JSON data into a the
            specified file.
        Returns:
            [dict]: Single collection data
        """
        endpoint = f"collection/{collection_slug}"
        return self._make_request(endpoint)

    def collection_stats(self, collection_slug, export_file_name=""):
        """Fetches collection stats data from the API.
        OpenSea API Collection Stats query parameters:
        https://docs.opensea.io/reference/retrieving-collection-stats
        Args:
            export_file_name (str, optional): Exports the JSON data into the
            specified file.
        Returns:
            [dict]: Collection stats
        """
        endpoint = f"collection/{collection_slug}/stats"
        return self._make_request(endpoint)
