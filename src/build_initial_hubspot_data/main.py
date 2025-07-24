"""Project main entry point."""

import logging
import os
import re
import sys
import urllib.parse
from dataclasses import dataclass
from typing import NewType, NoReturn, Optional
import pandas as pd

import requests
from dotenv import load_dotenv

logger = logging.getLogger()
logger.setLevel('INFO')

load_dotenv()

ControlCenterEndpoint = urllib.parse.ParseResult
ControlCenterToken = NewType('ControlCenterToken', str)


@dataclass(frozen=True)
class ControlCenterCredentials:
    endpoint: ControlCenterEndpoint
    token: ControlCenterToken

    @classmethod
    def from_env(cls) -> 'ControlCenterCredentials':
        """Initialize credentials from environment variables.

        Returns
        -------
        ControlCenterCredentials
            Credentials initialized from CONTROL_CENTER_ENDPOINT and CONTROL_CENTER_TOKEN env vars

        Raises
        ------
        ValueError
            If required environment variables are not set
        """
        endpoint_url = os.getenv('CONTROL_CENTER_ENDPOINT')
        token = os.getenv('CONTROL_CENTER_TOKEN')

        if not endpoint_url:
            msg = 'CONTROL_CENTER_ENDPOINT environment variable is required'
            raise ValueError(msg)
        if not token:
            msg = 'CONTROL_CENTER_TOKEN environment variable is required'
            raise ValueError(msg)

        parsed_endpoint = urllib.parse.urlparse(endpoint_url)
        return cls(
            endpoint=parsed_endpoint,
            token=ControlCenterToken(token),
        )


class ControlCenterApi:

    def __init__(self, credentials: ControlCenterCredentials) -> None:
        self.endpoint = credentials.endpoint
        self.headers = {'Authorization': f'Bearer {credentials.token}'}

    def get(self, url, **params):
        print(params)
        response = requests.get(
            url,
            headers=self.headers,
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def get_sensors(self, status: str = 'all', args=None) -> dict:
        url = urllib.parse.urljoin(self.endpoint.geturl(),
                                   '/api/get-sensors')
        return self.get(url, status=status, args=args)

    def active_sensors(self):
        url = urllib.parse.urljoin(self.endpoint.geturl(),
                                   '/api/activeSensors')
        return self.get(url)

    def list_thing(self):
        url = urllib.parse.urljoin(self.endpoint.geturl(),
                                   '/api/listthing')
        return self.get(url)


def things_to_dataframe(things: list) -> pd.DataFrame:
    """Transform things data into a pandas DataFrame.

    Parameters
    ----------
    things : list
        List of thing dictionaries from the API

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: prod_name, serial_number, production_date, latitude, longitude
    """
    data = []

    for thing in things:
        # Extract data from the thing
        prod_name = thing.get('id')
        production_date = thing.get('created_at')

        # Get coordinates and serial number from current_value.desired or current_value.reported
        current_value = thing.get('current_value', {})
        desired = current_value.get('desired', {})
        reported = current_value.get('reported', {})

        # Try to get values from desired first, then reported as fallback
        serial_number = desired.get(
            'SERIAL_NUMBER') or reported.get('SERIAL_NUMBER')
        latitude = desired.get(
            'COORDINATES_LAT') or reported.get('COORDINATES_LAT')
        longitude = desired.get(
            'COORDINATES_LON') or reported.get('COORDINATES_LON')

        data.append({
            'prod_name': prod_name,
            'numero_de_s_rie': serial_number,
            'date_de_producton': production_date,
            'latitude': latitude,
            'longitude': longitude
        })

    return pd.DataFrame(data)


def extract_version_from_serial(serial_number: str) -> Optional[str]:
    """Extract version from serial number.

    Parameters
    ----------
    serial_number : str
        Serial number in format like 'muvtx_012_fr_sys_000004'

    Returns
    -------
    str
        Version in format 'X.Y' (e.g., '1.2') or None if not found
    """
    if not serial_number:
        return None

    # Pattern to match 3 digits after 'muvtx_'
    pattern = r'muvtx_(\d{3})_'
    match = re.search(pattern, serial_number)

    if match:
        version_digits = match.group(1)
        # Convert '012' to '1.2'
        major = str(int(version_digits[1]))  # Remove leading zero
        minor = version_digits[2]
        return f"{major}.{minor}"

    return None


def add_version_column(df: pd.DataFrame) -> pd.DataFrame:
    """Add version column to DataFrame based on serial_number.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with serial_number column

    Returns
    -------
    pd.DataFrame
        DataFrame with added version column
    """
    df['version'] = df['numero_de_s_rie'].apply(extract_version_from_serial)


def main() -> NoReturn:  # pragma: no cover
    """Entry point."""
    credentials = ControlCenterCredentials.from_env()
    control_center = ControlCenterApi(credentials)
    things = control_center.list_thing()

    things_df = things_to_dataframe(things)
    add_version_column(things_df)
    print(things_df)

    sys.exit(0)


if __name__ == '__main__':  # pragma: no cover
    main()
