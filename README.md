# Flexible CLI

A Python-based CLI for aggregating RSS feeds and pulling job postings from HackerNews.

## Tech Stack

- **Python 3.13** - Core runtime
- **feedparser** - RSS feed parsing
- **aiohttp** - Async HTTP client
- **Redis** - Caching and data storage
- **PostgreSQL** - Database (psycopg2)

## Features

- RSS feed monitoring and parsing
- Async operations for performance
- Redis caching
- PostgreSQL database support

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Run the bot:
   ```bash
   python main.py
   ```

## Requirements

- Python 3.13 or higher
- Redis server
- PostgreSQL database

## Development

This project uses:
- `pyproject.toml` for project configuration
- `pytest` for testing
- Async/await patterns for performance

## License

Add your license information here.
