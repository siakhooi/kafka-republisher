import argparse
import logging
from importlib.metadata import version
from . import republisher
from .config import get_log_level_from_env


def run() -> None:
    logging.basicConfig(
        level=get_log_level_from_env(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    __version__: str = version("kafka-republisher")

    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Republish kafka message from one topic to another"
    )

    parser.add_argument(
        "-v", "--version", action="version", version=f"%(prog)s {__version__}"
    )

    parser.parse_args()

    republisher.run()


if __name__ == "__main__":
    run()
