"""Project main entry point."""

import logging
import os
import re
import sys
import urllib.parse
from dataclasses import dataclass
from typing import NewType, NoReturn

import boto3
import pandas as pd
import requests
from boto3.dynamodb.conditions import Key
from dotenv import load_dotenv

logger = logging.getLogger()
logger.setLevel('INFO')

load_dotenv()


@dataclass(frozen=True)
class ControlCenterCredentials:
    Endpoint = urllib.parse.ParseResult
    Token = NewType('Token', str)

    endpoint: Endpoint
    token: Token

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
            token=ControlCenterCredentials.Token(token),
        )


class ControlCenterApi:
    """API client for Control Center."""

    Sensor = NewType('Sensor', dict)
    Sensors = NewType('Sensors', list[Sensor])
    Thing = NewType('Thing', dict)
    Things = NewType('Things', list[Thing])
    Headers = NewType('Headers', dict)

    def __init__(self, credentials: ControlCenterCredentials) -> None:
        """Initialize the API client with credentials.

        Parameters
        ----------
        credentials : ControlCenterCredentials
            Credentials for authentication
        """
        self.endpoint: ControlCenterCredentials.Endpoint = credentials.endpoint
        self.headers: ControlCenterApi.Headers = ControlCenterApi.Headers(
            {'Authorization': f'Bearer {credentials.token}'})

    def get(self, url: str, **params: str) -> dict:
        """Perform a GET request to the specified URL with authorization headers.

        Parameters
        ----------
        url : str
            The URL to send the GET request to
        params : dict
            Additional parameters for the request

        Returns
        -------
        Any
            The JSON response from the API
        """
        response = requests.get(
            url,
            headers=self.headers,
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def get_sensors(self, status: str = 'all', args: str | None = None) -> 'ControlCenterApi.Sensors':
        """Retrieve sensors from the Control Center API.

        Parameters
        ----------
        status : str
            Status filter for sensors (default is 'all')
        args : Any, optional
            Additional arguments for the API request

        Returns
        -------
        Sensors
            List of sensors data from the API
        """
        url = urllib.parse.urljoin(self.endpoint.geturl(), '/api/get-sensors')
        response = self.get(url, status=status, args=args)
        sensors_data = response.get('data', [])
        return ControlCenterApi.Sensors(sensors_data)

    def get_sensor(self, human_name: str, args: str | None = None) -> 'ControlCenterApi.Sensor':
        """Retrieve a specific sensor by its human-readable name.

        Parameters
        ----------
        sensor_name : str
            Human-readable name of the sensor
        args : Any, optional
            Additional arguments for the API request

        Returns
        -------
        Sensor
            Sensor data retrieved from the API as a custom type
        """
        url = urllib.parse.urljoin(
            self.endpoint.geturl(), f'/api/v1/sensor/{human_name}')
        return ControlCenterApi.Sensor(self.get(url, args=args))

    def get_active_sensors(self) -> 'ControlCenterApi.Sensors':
        """Retrieve active sensors from the Control Center API.

        Returns
        -------
        Sensors
            List of active sensors from the API
        """
        url = urllib.parse.urljoin(
            self.endpoint.geturl(), '/api/activeSensors')
        response = self.get(url)
        sensors_data = response.get('data', [])
        return ControlCenterApi.Sensors(sensors_data)

    def list_things(self) -> 'ControlCenterApi.Things':
        """List all things.

        Returns
        -------
        Things
            List of things from the API
        """
        url = urllib.parse.urljoin(self.endpoint.geturl(), '/api/listthing')
        return ControlCenterApi.Things(self.get(url))

    def get_sensor_all_things(self, human_name: str) -> 'ControlCenterApi.Things':
        """Retrieve all things associated with a given sensor.

        Parameters
        ----------
        human_name : str
            Human-readable name of the sensor

        Returns
        -------
        Things
            List of all things for the sensor as a custom type
        """
        response = self.get_sensor(human_name, args='all_things')
        all_things = response.get('sensor', {}).get('all_things', [])
        return ControlCenterApi.Things(all_things)


Things = NewType('Things', pd.DataFrame)


def things_to_dataframe(things: ControlCenterApi.Things) -> Things:
    """Transform things data into a pandas DataFrame.

    Parameters
    ----------
    things : RawThings
        List of thing dictionaries from the API

    Returns
    -------
    ThingsDataFrame
        DataFrame with columns: prod_name, serial_number, production_date, latitude, longitude
    """
    data: list[dict] = []

    for thing in things:
        prod_name = thing.get('id')
        production_date = thing.get('created_at')
        updated_at = thing.get('updated_at')
        current_value = thing.get('current_value', {})
        reported = current_value.get('reported', {})
        serial_number = reported.get('SERIAL_NUMBER')
        latitude = reported.get('COORDINATES_LAT')
        longitude = reported.get('COORDINATES_LON')

        data.append({
            'prod_name': prod_name,
            'serial_number': serial_number,
            'production_date': production_date,
            'updated_at': updated_at,
            'latitude': latitude,
            'longitude': longitude,
        })

    return Things(pd.DataFrame(data))


def extract_version_from_serial(serial_number: str) -> str | None:
    """Extract version from serial number.

    Parameters
    ----------
    serial_number : str
        Serial number in format like 'muvtx_012_fr_sys_000004'

    Returns
    -------
    Optional[str]
        Version in format 'X.Y' (e.g., '1.2') or None if not found
    """
    if not serial_number:
        return None

    pattern = r'muvtx_(\d{3})_'
    match = re.search(pattern, serial_number)

    if match:
        version_digits = match.group(1)

        major = str(int(version_digits[1]))
        minor = version_digits[2]
        return f'{major}.{minor}'

    return None


def add_version_column(things: Things) -> None:
    """Add version column to DataFrame based on serial_number.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with serial_number column

    Returns
    -------
    None
    """
    things['version'] = things['serial_number'].apply(
        extract_version_from_serial)


FEATURE_VERSION_THRESHOLD = 2.1  # Magic value for version threshold


def add_feature_columns(things: Things) -> None:
    """Add feature columns (hauteur, temperature, image) based on version.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with version column

    Returns
    -------
    None
    """
    def get_features(version_str: str | None) -> tuple[str, str, str]:
        if version_str is None:
            return 'Non', 'Non', 'Non'
        try:
            version_float = float(version_str)
            if version_float >= FEATURE_VERSION_THRESHOLD:
                return 'Oui', 'Oui', 'Oui'  # hauteur, temperature, image
            return 'Oui', 'Non', 'Oui'  # hauteur, temperature, image
        except (ValueError, TypeError):
            # If version can't be converted to float, default to all 'Non'
            return 'Non', 'Non', 'Non'

    things[['hauteur', 'temperature', 'image']] = things['version'].apply(
        lambda x: pd.Series(get_features(x)),
    )


Sensors = NewType('Sensors', pd.DataFrame)


def sensors_to_dataframe(sensors: ControlCenterApi.Sensors) -> Sensors:
    """Transform sensors data into a pandas DataFrame.

    Parameters
    ----------
    sensors : RawSensors
        List containing sensors data from API

    Returns
    -------
    SensorsDataFrame
        DataFrame with columns: thing_id, human_name, etat
    """
    data: list[dict] = []

    for sensor in sensors:
        thing_id = sensor.get('thing_active', {}).get('id')
        data.append({
            'prod_name': thing_id,
            'human_name': sensor.get('human_name'),
        })
    return Sensors(pd.DataFrame(data))


def add_human_name_column(things: Things, sensors: Sensors) -> Things:
    """Add human_name column using DataFrame join for better performance.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with prod_name column
    sensors : pd.DataFrame
        DataFrame containing active sensors data from API

    Returns
    -------
    pd.DataFrame
        DataFrame with added 'status' column
    """

    # Left join on prod_name = thing_id
    return things.merge(
        sensors,
        on='prod_name',
        how='left',
    )


def add_deactivation_date_column(things: Things) -> None:
    """Add deactivation date column based on status and updated_at.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'updated_at' and 'status' columns

    Returns
    -------
    None
    """

    # Add the deactivation date column using np.where for efficiency
    things['deactivation_date'] = things.apply(
        lambda row: row['updated_at'] if row['status'] == 'Out of order' else None,
        axis=1,
    )


class PayloadDatabase:
    """Database client for payloads."""

    HumanName = NewType('HumanName', str)
    ProdNumber = NewType('ProdNumber', str)
    RawPayload = NewType('RawPayload', dict)
    PAYLOAD_DATABASE_TABLE = 'metrics_prod'

    def __init__(self) -> None:
        """Initialize the PayloadDatabase client."""
        dynamodb = boto3.resource('dynamodb')
        self.table = dynamodb.Table(PayloadDatabase.PAYLOAD_DATABASE_TABLE)

    def get_first_payload(self, human_name: 'PayloadDatabase.HumanName', prod_number: 'PayloadDatabase.ProdNumber') -> RawPayload | None:
        """Get the first payload for a sensor and prod_number.

        Parameters
        ----------
        sensor : Sensor
            The sensor human name as a custom type
        prod_number : str
            The production number

        Returns
        -------
        Optional[RawPayload]
            The first payload dictionary if found, otherwise None
        """
        response = self.table.query(
            IndexName='id-human_name-index',
            KeyConditionExpression=Key('id').eq(
                prod_number) & Key('human_name').eq(human_name),
            ScanIndexForward=True,
            Limit=1,
        )
        return response['Items'][0] if response['Items'] else None

    def get_last_payload(self, human_name: 'PayloadDatabase.HumanName', prod_number: 'PayloadDatabase.ProdNumber') -> RawPayload | None:
        """Get the last payload for a sensor and prod_number.

        Parameters
        ----------
        sensor : Sensor
            The sensor human name as a custom type
        prod_number : str
            The production number

        Returns
        -------
        Optional[RawPayload]
            The last payload dictionary if found, otherwise None
        """
        response = self.table.query(
            IndexName='id-human_name-index',
            KeyConditionExpression=Key('id').eq(
                prod_number) & Key('human_name').eq(human_name),
            ScanIndexForward=False,
            Limit=1,
        )
        return response['Items'][0] if response['Items'] else None


def get_timestamp(payload: PayloadDatabase.RawPayload) -> pd.Timestamp | None:
    """Get the link date for a specific sensor.

    Parameters
    ----------
    human_name : str
        The human-readable name of the sensor

    Returns
    -------
    Optional[pd.Timestamp]
        The link date if found, otherwise None
    """
    return payload['time_stamp']


PlmnCode = NewType('PlmnCode', str)
OperatorName = NewType('OperatorName', str)
OperatorPlmnMappings = NewType('OperatorPlmnMappings', dict)


def extract_plmn_code_from_qnwinfo(response: str) -> PlmnCode | None:
    """Extract the PLMN code from a +QNWINFO response string.

    Parameters
    ----------
    response : str
        The +QNWINFO response string, e.g. '+QNWINFO: "FDD LTE","20820","LTE BAND 103",73A'

    Returns
    -------
    PlmnCode | None
        The extracted PLMN code, or None if not found
    """
    pattern = r'\+QNWINFO:\s*"[^"]*","(\d{5})",'
    match = re.search(pattern, response)
    if match:
        return PlmnCode(match.group(1))
    return None


def get_operator_plmn_mappings() -> OperatorPlmnMappings:
    """Return the mapping of PLMN codes to operator names.

    Returns
    -------
    OperatorPlmnMappings
        Dictionary mapping PLMN code to operator name
    """
    return OperatorPlmnMappings({
        PlmnCode('20801'): OperatorName('Orange'),
        PlmnCode('20810'): OperatorName('SFR'),
        PlmnCode('20815'): OperatorName('Free'),
        PlmnCode('20820'): OperatorName('Bouygues Telecom'),
        PlmnCode('21407'): OperatorName('Movistar'),
        PlmnCode('21901'): OperatorName('T-Mobile'),
        PlmnCode('21902'): OperatorName('Telemach / Tele2'),
        PlmnCode('21910'): OperatorName('A1 / VIP'),
        PlmnCode('22210'): OperatorName('Vodafone'),
        PlmnCode('22288'): OperatorName('WindTre / WIND'),
        PlmnCode('23420'): OperatorName('3'),
        PlmnCode('26201'): OperatorName('T-mobile'),
        PlmnCode('50501'): OperatorName('Telstra'),
        PlmnCode('60400'): OperatorName('Orange'),
        PlmnCode('61701'): OperatorName('my.t mobile'),
        PlmnCode('61710'): OperatorName('Emtel'),
    })


def plmn_code_to_operator_name(plmn_code: PlmnCode) -> OperatorName | None:
    """Convert a PLMN code to its operator name.

    Parameters
    ----------
    plmn_code : PlmnCode
        The PLMN code to look up

    Returns
    -------
    OperatorName | None
        The operator name if found, otherwise None
    """
    operator_plmn_mappings = get_operator_plmn_mappings()
    return operator_plmn_mappings.get(plmn_code, plmn_code)


def extract_operator_name_from_qnwinfo(qnwinfo: str) -> OperatorName | None:
    """Extract the operator name from a +QNWINFO response string.

    Parameters
    ----------
    response : str
        The +QNWINFO response string, e.g. '+QNWINFO: "FDD LTE","20820","LTE BAND 103",73A'

    Returns
    -------
    OperatorName | None
        The extracted operator name, or None if not found
    """
    plmn_code = extract_plmn_code_from_qnwinfo(qnwinfo)
    if plmn_code:
        return plmn_code_to_operator_name(plmn_code)
    return None


def get_link_operator_from_qnwinfo(payload: PayloadDatabase.RawPayload) -> OperatorName | None:
    """Get the link operator info from a +QNWINFO field in the payload.

    Parameters
    ----------
    payload : RawPayload
        The payload dictionary containing sensor data

    Returns
    -------
    OperatorName | None
        The operator name if found, otherwise None
    """
    qnwinfo = payload.get('qnwinfo')
    match qnwinfo:
        case [str() as v]:
            return extract_operator_name_from_qnwinfo(v)
        case str() as v:
            return extract_operator_name_from_qnwinfo(v)
        case _:
            return None


LinkDates = NewType('LinkDates', pd.DataFrame)


def get_link_dates(sensors: Sensors, control_center: ControlCenterApi) -> LinkDates:
    """Build a DataFrame of link dates for sensors and things.

    Parameters
    ----------
    sensors : dict
        Dictionary containing sensors data from API
    control_center : ControlCenterApi
        Control center API client

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: human_name, prod_name, link_date
    """
    link_dates: list[dict] = []
    payload_db = PayloadDatabase()
    for human_name in sensors['human_name']:
        all_things = control_center.get_sensor_all_things(human_name)
        for thing in all_things:
            prod_number = thing['id']
            first_payload = payload_db.get_first_payload(
                human_name, prod_number)
            last_payload = payload_db.get_last_payload(
                human_name, prod_number)

            link_dates.append({
                'human_name': human_name,
                'prod_name': prod_number,
                'link_date': get_timestamp(first_payload) if first_payload else None,
                'link_operator': get_link_operator_from_qnwinfo(
                    first_payload) if first_payload else None,
                'unlink_date': get_timestamp(last_payload) if last_payload else None,
            })

    dataframe = pd.DataFrame(link_dates)
    dataframe = dataframe.sort_values(by='human_name')
    return LinkDates(dataframe)


def add_sensor_link_date(things: Things, link_dates: LinkDates) -> Things:
    """Add link date column to things DataFrame using link_dates.

    Parameters
    ----------
    things : Things
        DataFrame containing things data
    link_dates : LinkDates
        DataFrame containing link dates for sensors and things

    Returns
    -------
    Things
        DataFrame with activation date column added
    -------
    None
    """
    latest_link_dates = (
        link_dates
        .sort_values('link_date', ascending=False)
        .drop_duplicates(subset=['human_name'], keep='first')
        .drop(columns=['human_name', 'unlink_date'])
    )


    things_merged = things.merge(
        latest_link_dates, on='prod_name', how='left')
    return things_merged.rename(columns={'link_operator': 'last_link_operator', 'link_date': 'last_link_date'})


def add_last_unlink_date(things: Things, link_dates: LinkDates) -> Things:
    """Add last link date column to things DataFrame using the latest link date per prod_name.

    Parameters
    ----------
    things : Things
        DataFrame containing things data
    link_dates : LinkDates
        DataFrame containing link dates for sensors and things

    Returns
    -------
    Things
        DataFrame with last link date column added
    """
    # Keep only the latest unlink_date for each prod_name
    latest_unlink_dates = (
        link_dates
        .sort_values('unlink_date', ascending=False)
        .drop_duplicates(subset=['prod_name'], keep='first')
        .rename(columns={'unlink_date': 'last_unlink_date'})
    )

    return things.merge(
        latest_unlink_dates[['prod_name', 'last_unlink_date']],
        on='prod_name',
        how='left',
    )


def add_first_link_date(things: Things, link_dates: LinkDates) -> Things:
    """Add first link date column to things DataFrame using the oldest link date per prod_name.

    Parameters
    ----------
    things : Things
        DataFrame containing things data
    link_dates : LinkDates
        DataFrame containing link dates for sensors and things

    Returns
    -------
    Things
        DataFrame with first link date column added
    """
    # Keep only the oldest link_date for each prod_name
    oldest_link_dates = (
        link_dates
        .sort_values('link_date')
        .drop_duplicates(subset=['prod_name'], keep='first')
        .rename(columns={'link_date': 'first_link_date'})
    )

    return things.merge(
        oldest_link_dates[['prod_name', 'first_link_date']],
        on='prod_name',
        how='left',
    )


def main() -> NoReturn:  # pragma: no cover
    """Entry point."""
    credentials = ControlCenterCredentials.from_env()
    control_center = ControlCenterApi(credentials)

    things = things_to_dataframe(control_center.list_things())
    add_version_column(things)
    add_feature_columns(things)
    things.to_csv('things.csv', index=False, sep=';', encoding='utf-8')

    sensors = sensors_to_dataframe(
        control_center.get_sensors(args='thing,human_name'))

    things = add_human_name_column(things, sensors)

    # sensors = Sensors(sensors.head(20))
    link_dates = get_link_dates(sensors, control_center)
    link_dates.to_csv('link_dates.csv', index=False, sep=';', encoding='utf-8')

    things = add_sensor_link_date(things, link_dates)
    things = add_first_link_date(things, link_dates)
    things = add_last_unlink_date(things, link_dates)
    things.to_csv('things.csv', index=False, sep=';', encoding='utf-8')

    sys.exit(0)


if __name__ == '__main__':  # pragma: no cover
    main()
