import argparse
import sys
from importlib.metadata import version
from . import republisher


def print_to_stderr_and_exit(e: Exception, exit_code: int) -> None:
    print(f"Error: {e}", file=sys.stderr)
    exit(exit_code)


def run() -> None:
    __version__: str = version("kafka-republisher")

    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Republish kafka message from one topic to another"
    )

    parser.add_argument(
        "-v", "--version", action="version", version=f"%(prog)s {__version__}"
    )

    parser.parse_args()

    republisher.run()
