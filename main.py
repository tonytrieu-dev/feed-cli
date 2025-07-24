import sys
from cli import cli
from logger import setup_logging

logger = setup_logging('main')


def main():
    logger.info("Starting Tech News Aggregator...")
    
    try:
        # Run the CLI
        cli()
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()