"""REST client handling, including AppmetricaStream base class."""

from __future__ import annotations

import csv
import sys
from pathlib import Path
from typing import Any, Callable, Iterable, Generator
import datetime

import pendulum
import requests
import backoff
from singer_sdk import metrics
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import SimpleAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.helpers._util import utc_now

if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

# See https://stackoverflow.com/questions/15063936/csv-error-field-larger-than-field-limit-131072
csv.field_size_limit(sys.maxsize)


class AppmetricaStream(RESTStream):
    """Appmetrica stream class."""

    _LOG_REQUEST_METRIC_URLS = True

    url_base = "https://api.appmetrica.yandex.ru"

    extra_retry_statuses = [202] + RESTStream.extra_retry_statuses

    @cached_property
    def authenticator(self) -> _Auth:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return SimpleAuthenticator(
            self, {"Authorization": f"OAuth {self.config['token']}"}
        )

    def backoff_wait_generator(self) -> Generator[float, None, None]:
        return backoff.constant(120)

    def backoff_max_tries(self) -> int:
        return 50

    @property
    def requests_session(self) -> requests.Session:
        if not self._requests_session:
            self._requests_session = requests.Session()
            self._requests_session.stream = True
        return self._requests_session

    def request_records(self, context: dict | None) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.
        """

        page_date = pendulum.parse(self.get_starting_replication_key_value(context))

        decorated_request = self.request_decorator(self._request)

        now = utc_now()

        with metrics.http_request_counter(self.name, self.path) as request_counter:
            request_counter.context = context

            while page_date < now:
                prepared_request = self.prepare_request(
                    context,
                    next_page_token=page_date,
                )
                resp = decorated_request(prepared_request, context)
                request_counter.increment()
                self.update_sync_costs(prepared_request, resp, context)
                yield from self.parse_response(resp)

                self.finalize_state_progress_markers()
                self._write_state_message()
                page_date += datetime.timedelta(days=self.config["chunk_days"])

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}

        assert next_page_token is not None

        params["application_id"] = self.config["application_id"]

        params["date_dimension"] = "receive"
        params["date_since"] = next_page_token.strftime("%Y-%m-%d %H:%M:%S")
        params["date_until"] = (
            next_page_token + datetime.timedelta(days=self.config["chunk_days"])
        ).strftime("%Y-%m-%d %H:%M:%S")

        if limit := self.config.get("limit") is not None:
            params["limit"] = limit

        params["fields"] = ",".join(self.fields)

        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        reader = csv.DictReader(response.iter_lines(decode_unicode=True))
        yield from reader


class AppmetricaStatStream(RESTStream):
    url_base = "https://api.appmetrica.yandex.ru/stat/v1/data"
    records_jsonpath = "$.data[*]"

    @cached_property
    def authenticator(self) -> _Auth:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return SimpleAuthenticator(
            self, {"Authorization": f"OAuth {self.config['token']}"}
        )
    
    @property
    def get_metrics(self) -> str:
        return ''

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {
            'id': self.config['application_id'],
            'metrics': self.get_metrics,
            'dimensions': 'ym:i:date',
            'date1': self.config['start_date'].split()[0],
            'group': 'day'
        }
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        # TODO: Parse response body and return a set of records.
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def post_process(
        self,
        row: dict,
        context: dict | None = None,
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        # TODO: Delete this method if not needed.
        return row
