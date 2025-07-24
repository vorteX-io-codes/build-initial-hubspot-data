"""Project main entry point."""

import logging
import sys
from typing import NoReturn

logger = logging.getLogger()
logger.setLevel('INFO')

def say_hello_world() -> str:
    """Return 'Hello World' string.

    Returns
    -------
    str
        the 'Hello World' string

    """
    return 'Hello World'

def main() -> NoReturn: # pragma: no cover
    """Entry point."""
    logger.info(say_hello_world())

    sys.exit(0)

if __name__ == '__main__': # pragma: no cover
    main()
