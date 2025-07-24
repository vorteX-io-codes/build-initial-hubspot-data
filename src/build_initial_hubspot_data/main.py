"""Project main entry point."""

from dataclasses import dataclass
import logging
import os
import sys
from typing import NewType, NoReturn
from urllib import request
from dotenv import load_dotenv


import requests

import urllib.parse

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
            raise ValueError(
                "CONTROL_CENTER_ENDPOINT environment variable is required")
        if not token:
            raise ValueError(
                "CONTROL_CENTER_TOKEN environment variable is required")

        parsed_endpoint = urllib.parse.urlparse(endpoint_url)
        return cls(
            endpoint=parsed_endpoint,
            token=ControlCenterToken(token)
        )


class ControlCenterApi:

    def __init__(self, credentials: ControlCenterCredentials) -> None:
        self.endpoint = credentials.endpoint
        self.headers = {'Authorization': f'Bearer {credentials.token}'}

    def get_sensors(self, status: str = 'all'):
        url = urllib.parse.urljoin(self.endpoint.geturl(),
                                   '/api/get-sensors')
        params = {'status': status}
        response = requests.get(
            url,
            headers=self.headers,
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()


def main() -> NoReturn:  # pragma: no cover
    """Entry point."""
    credentials = ControlCenterCredentials.from_env()
    control_center = ControlCenterApi(credentials)
    sensors = control_center.get_sensors(status='active')
    print(sensors)

    sys.exit(0)


if __name__ == '__main__':  # pragma: no cover
    main()
