"""Stream type classes for tap-appmetrica."""

from __future__ import annotations

from pathlib import Path

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_appmetrica.client import AppmetricaStream


class EventsStream(AppmetricaStream):
    name = "events"
    path = "/logs/v1/export/events.json"

    primary_keys = None
    replication_key = "event_receive_date"

    fields = "event_datetime,event_json,event_name,event_receive_datetime,event_receive_timestamp,event_timestamp,session_id,installation_id,appmetrica_device_id,city,connection_type,country_iso_code,device_ipv6,device_locale,device_manufacturer,device_model,device_type,google_aid,ios_ifa,ios_ifv,mcc,mnc,operator_name,original_device_model,os_name,os_version,profile_id,windows_aid,app_build_number,app_package_name,app_version_name,application_id"

    schema = th.PropertiesList(
        # Synthetic property for sync purposes
        th.Property("event_receive_date", th.DateType),
        ###
        # Fields from Appmetrica
        th.Property("app_build_number", th.StringType),
        th.Property("app_package_name", th.StringType),
        th.Property("app_version_name", th.StringType),
        th.Property("application_id", th.StringType),
        th.Property("appmetrica_device_id", th.StringType),
        th.Property("city", th.StringType),
        th.Property("connection_type", th.StringType),
        th.Property("country_iso_code", th.StringType),
        th.Property("device_ipv6", th.StringType),
        th.Property("device_locale", th.StringType),
        th.Property("device_manufacturer", th.StringType),
        th.Property("device_model", th.StringType),
        th.Property("device_type", th.StringType),
        th.Property("event_datetime", th.DateType),
        th.Property("event_json", th.StringType),
        th.Property("event_name", th.StringType),
        th.Property("event_name", th.StringType),
        th.Property("event_receive_datetime", th.StringType),
        th.Property("event_receive_timestamp", th.StringType),
        th.Property("event_timestamp", th.StringType),
        th.Property("google_aid", th.StringType),
        th.Property("installation_id", th.StringType),
        th.Property("ios_ifa", th.StringType),
        th.Property("ios_ifv", th.StringType),
        th.Property("mcc", th.StringType),
        th.Property("mnc", th.StringType),
        th.Property("operator_name", th.StringType),
        th.Property("original_device_model", th.StringType),
        th.Property("os_name", th.StringType),
        th.Property("os_version", th.StringType),
        th.Property("profile_id", th.StringType),
        th.Property("session_id", th.StringType),
        th.Property("windows_aid", th.StringType),
    ).to_dict()
