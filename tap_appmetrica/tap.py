"""Appmetrica tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_appmetrica import streams


class TapAppmetrica(Tap):
    """Appmetrica tap class."""

    name = "tap-appmetrica"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "application_id",
            th.StringType,
            required=True,
        ),
        th.Property(
            "token",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "limit",
            th.StringType,
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.AppmetricaStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.EventsStream(self),
        ]


if __name__ == "__main__":
    TapAppmetrica.cli()
