"""Project main entry point."""

import logging
import os
import re
import sys
import urllib.parse
from dataclasses import dataclass
from typing import NewType, NoReturn, Optional
import boto3
import pandas as pd
from datetime import datetime
from boto3.dynamodb.conditions import Key

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
        updated_at = thing.get('updated_at')

        # Get coordinates and serial number from current_value.desired or current_value.reported
        current_value = thing.get('current_value', {})
        reported = current_value.get('reported', {})

        # Try to get values from desired first, then reported as fallback
        serial_number = reported.get('SERIAL_NUMBER')
        latitude = reported.get('COORDINATES_LAT')
        longitude = reported.get('COORDINATES_LON')

        data.append({
            'prod_name': prod_name,
            'numero_de_s_rie': serial_number,
            'date_de_producton': production_date,
            'updated_at': updated_at,
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


def add_version_column(df: pd.DataFrame) -> None:
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


def add_feature_columns(df: pd.DataFrame) -> None:
    """Add feature columns (hauteur, temperature, image) based on version.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with version column

    Returns
    -------
    pd.DataFrame
        DataFrame with added feature columns
    """

    def get_features(version_str):
        if version_str is None:
            return 'Non', 'Non', 'Non'

        try:
            # Convert version string to float for comparison
            version_float = float(version_str)

            if version_float >= 2.1:
                return 'Oui', 'Oui', 'Oui'  # hauteur, temperature, image
            else:
                return 'Oui', 'Non', 'Oui'  # hauteur, temperature, image
        except (ValueError, TypeError):
            # If version can't be converted to float, default to all 'Non'
            return 'Non', 'Non', 'Non'

    # Apply the function to create the three columns
    df[['hauteur', 'temperature', 'image']] = df['version'].apply(
        lambda x: pd.Series(get_features(x))
    )


def active_sensors_to_dataframe(active_sensors: dict) -> pd.DataFrame:
    """Transform active sensors data into a pandas DataFrame.

    Parameters
    ----------
    active_sensors : dict
        Dictionary containing active sensors data from API

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: thing_id, sensor_human_name, etat
    """
    data = []

    if 'data' in active_sensors:
        for sensor in active_sensors['data']:
            data.append({
                'thing_id': sensor.get('thing_id'),
                'sensor_human_name': sensor.get('sensor_human_name'),
                'etat': 'Opérationnel'
            })

    return pd.DataFrame(data)


def add_status_column(df: pd.DataFrame, active_sensors: dict) -> pd.DataFrame:
    """Add status column using DataFrame join for better performance.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with prod_name column
    active_sensors : dict
        Dictionary containing active sensors data from API

    Returns
    -------
    pd.DataFrame
        DataFrame with added 'etat' column
    """
    # Convert active sensors to DataFrame
    active_df = active_sensors_to_dataframe(active_sensors)

    # Left join on prod_name = thing_id
    df_with_status = df.merge(
        active_df[['thing_id', 'etat']],
        left_on='prod_name',
        right_on='thing_id',
        how='left'
    )

    # Fill NaN values with 'Non opérationnel'
    df_with_status['etat'] = df_with_status['etat'].fillna('Non opérationnel')

    # Drop the extra thing_id column from the join
    df_with_status = df_with_status.drop('thing_id', axis=1)

    return df_with_status


def add_activation_date_column(df: pd.DataFrame) -> None:
    """Add activation date column based on status and updated_at.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'updated_at' and 'etat' columns

    Returns
    -------
    pd.DataFrame
        DataFrame with added 'date_activation_sur_pm_actuel' column
    """

    # Add the activation date column
    df['date_activation_sur_pm_actuel'] = df.apply(
        lambda row: row['updated_at'] if row['etat'] == 'Opérationnel' else None,
        axis=1
    )


def add_deactivation_date_column(df: pd.DataFrame) -> None:
    """Add deactivation date column based on status and updated_at.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'updated_at' and 'etat' columns

    Returns
    -------
    pd.DataFrame
        DataFrame with added 'date_desactivation' column
    """

    # Add the deactivation date column using np.where for efficiency
    df['date_desactivation'] = df.apply(
        lambda row: row['updated_at'] if row['etat'] == 'Non opérationnel' else None,
        axis=1
    )


PAYLOAD_DATABASE_TABLE = 'metrics_prod'
Sensor = NewType('Sensor', str)
StartDate = NewType('StartDate', datetime)
EndDate = NewType('EndDate', datetime)
RawPayload = NewType('RawPayload', dict)
RawPayloads = list[RawPayload]


class PayloadDatabase:

    def __init__(self) -> None:
        # session = boto3.Session(profile_name=PAYLOAD_DATABASE_AWS_PROFILE)
        dynamodb = boto3.resource('dynamodb')
        self.table = dynamodb.Table(PAYLOAD_DATABASE_TABLE)

    def get(self, sensor: Sensor, prod_number: str) -> RawPayloads:
        response = self.table.query(
            KeyConditionExpression=Key('human_name').eq(sensor) & Key('id').eq(prod_number),
            # True = ordre croissant (plus ancien en premier)
            ScanIndexForward=True,
            Limit=1
        )
        return response['Items'][0] if response['Items'] else None


def main() -> NoReturn:  # pragma: no cover
    """Entry point."""
    # credentials = ControlCenterCredentials.from_env()
    # control_center = ControlCenterApi(credentials)
    # things = control_center.list_thing()

    # things = things_to_dataframe(things)
    # add_version_column(things)
    # add_feature_columns(things)

    # active_sensors = control_center.active_sensors()
    # things = add_status_column(things, active_sensors)
    # add_activation_date_column(things)
    # add_deactivation_date_column(things)
    # # print(active_sensors)

    # print(things)
    # print(things.dtypes)

    payload_db = PayloadDatabase()
    payloads = payload_db.get(Sensor('balma_2'), 'prod_1000000026046db9')
    print(payloads)
    sys.exit(0)


if __name__ == '__main__':  # pragma: no cover
    main()
