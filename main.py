from edge_collector.logutil import logger, setup_logging
from edge_collector.service import CollectorService


def main():
    setup_logging()
    logger.info("Starting Edge Collector...")
    service = CollectorService("config.json")
    try:
        service.run()
    except KeyboardInterrupt:
        logger.info("Stopping...")
        service.stop()


if __name__ == "__main__":
    main()
