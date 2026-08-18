import logging


def setup_logging(level=logging.INFO):
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(threadName)s - %(message)s",
    )
    return logging.getLogger("edge-collector")


logger = logging.getLogger("edge-collector")
