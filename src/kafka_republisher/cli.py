import argparse
from importlib.metadata import version
from . import republisher


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
