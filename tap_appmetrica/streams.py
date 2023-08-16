"""Stream type classes for tap-appmetrica."""
from __future__ import annotations

from typing import Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_appmetrica.client import AppmetricaStream

import csv

import requests


class EventsStream(AppmetricaStream):
    name = "events"
    path = "/logs/v1/export/events.csv"

    primary_keys = None
    replication_key = "event_receive_datetime"

    fields = (
        "event_datetime",
        "event_json",
        "event_name",
        "event_receive_datetime",
        "event_receive_timestamp",
        "event_timestamp",
        "session_id",
        "installation_id",
        "appmetrica_device_id",
        "city",
        "connection_type",
        "country_iso_code",
        "device_ipv6",
        "device_locale",
        "device_manufacturer",
        "device_model",
        "device_type",
        "google_aid",
        "ios_ifa",
        "ios_ifv",
        "mcc",
        "mnc",
        "operator_name",
        "original_device_model",
        "os_name",
        "os_version",
        "profile_id",
        "windows_aid",
        "app_build_number",
        "app_package_name",
        "app_version_name",
        "application_id",
    )

    schema = th.PropertiesList(
        *[th.Property(i, th.StringType) for i in fields]
    ).to_dict()


class InstallationsStream(AppmetricaStream):
    name = "installations"
    path = "/logs/v1/export/installations.csv"

    primary_keys = None
    replication_key = "install_receive_datetime"

    fields = [
        "application_id",
        "click_datetime",
        "click_id",
        "click_ipv6",
        "click_timestamp",
        "click_url_parameters",
        "click_user_agent",
        "profile_id",
        "publisher_id",
        "publisher_name",
        "tracker_name",
        "tracking_id",
        "install_datetime",
        "install_ipv6",
        "install_receive_datetime",
        "install_receive_timestamp",
        "install_timestamp",
        "is_reattribution",
        "is_reinstallation",
        "match_type",
        "appmetrica_device_id",
        "city",
        "connection_type",
        "country_iso_code",
        "device_locale",
        "device_manufacturer",
        "device_model",
        "device_type",
        "google_aid",
        "ios_ifa",
        "ios_ifv",
        "mcc",
        "mnc",
        "operator_name",
        "os_name",
        "os_version",
        "windows_aid",
        "app_package_name",
        "app_version_name",
    ]

    schema = th.PropertiesList(
        *[th.Property(i, th.StringType) for i in fields]
    ).to_dict()

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        reader = csv.DictReader(response.iter_lines(decode_unicode=True))
        yield from (
            obj
            for obj in reader
            if obj.get("install_receive_datetime")
            and obj.get("install_receive_datetime") not in ["identifier", "fingerprint"]
        )
