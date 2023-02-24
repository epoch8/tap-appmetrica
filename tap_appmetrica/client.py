"""REST client handling, including AppmetricaStream base class."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Callable, Iterable, Generator
import datetime

import pendulum
import requests
import backoff
from singer_sdk import metrics
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import SimpleAuthenticator
from singer_sdk.pagination import BaseAPIPaginator

if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class AppmetricaStream(RESTStream):
    """Appmetrica stream class."""

    url_base = "https://api.appmetrica.yandex.ru"

    records_jsonpath = "$.data[*]"  # Or override `parse_response`.

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
        return 30

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def request_records(self, context: dict | None) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.
        """

        if starting_replication_value := self.get_starting_replication_key_value(
            context
        ):
            page_date = pendulum.parse(
                starting_replication_value
            ).date() + datetime.timedelta(days=1)
        else:
            page_date = datetime.date.today() - datetime.timedelta(days=7)

        decorated_request = self.request_decorator(self._request)

        with metrics.http_request_counter(self.name, self.path) as request_counter:
            request_counter.context = context

            while page_date < datetime.date.today():
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
                page_date += datetime.timedelta(days=1)

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
        if not next_page_token:
            next_page_token = self.get_starting_replication_key_value(
                context
            ) or datetime.date.today().strftime("%Y-%m-%d")

        params["application_id"] = self.config["application_id"]

        params["date_dimension"] = "receive"
        params["date_since"] = next_page_token
        params["date_until"] = next_page_token

        params["limit"] = 10

        params["fields"] = self.fields

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

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        row["event_receive_date"] = pendulum.parse(row["event_receive_datetime"]).date()
        return row
