import click
import logging
from tabulate import tabulate
from typing import List, Tuple
from functools import wraps
from db import get_db_connection, with_database
from aggregator import fetch_rss_feeds, add_to_db, get_article_stats
from config import Config
from redis import Redis
import json
import hashlib

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def handle_errors(func):
    """Decorator to handle CLI errors gracefully."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}")
            click.echo(f"Error: {e}", err=True)
            return None
    return wrapper


def normalize_url(url):
    """Helper function to normalize URLs by removing protocol, trailing slashes, and /feed."""
    # Remove protocol (http:// or https://) and trailing slashes
    normalized_url = url
    normalized_url = normalized_url.replace("http://", "").replace("https://", "").rstrip("/")
    # Remove /feed if present
    if normalized_url.endswith("/feed"):
        normalized_url = normalized_url[:-5]
    return normalized_url


def generate_cache_key(source_urls: Tuple[str], limit: int) -> str:
    """Generate a unique cache key based on the query parameters."""
    # Sort URLs to ensure consistent cache keys regardless of order
    sorted_urls = sorted([normalize_url(url) for url in source_urls]) if source_urls else []
    key_data = f"news:{':'.join(sorted_urls)}:{limit}"
    # Create a hash for a more compact key
    key_hash = hashlib.md5(key_data.encode()).hexdigest()
    return f"news_cache:{key_hash}"

redis_client = Redis(host='localhost', port=6379, db=0)


@click.group()
def cli():
    """Tech News Aggregator CLI"""
    pass

@cli.command()
@click.argument("user_source_urls", nargs=-1)
@click.option(
    "--count",
    "-c",
    default=5,
    type=int,
    help="Number of articles to display per feed (default: 5).",
)
@click.option(
    "--format",
    type=click.Choice(['grid', 'simple', 'json', 'html']),
    default='grid',
    help="Output format for articles."
)
@handle_errors
def news(user_source_urls, count, format):
    """Fetches articles from the database and then displays them."""
    articles = list(_fetch_articles_generator(user_source_urls, count))
    
    if not articles:
        click.echo("No articles were found in the database.")
        return
    
    if format == 'json':
        click.echo(json.dumps([dict(zip(['title', 'url', 'source'], article)) for article in articles], indent=2))
    else:
        headers = ["Title", "URL", "Source"]
        table = tabulate(articles, headers=headers, tablefmt=format)
        click.echo("\n--- Tech News Articles ---")
        click.echo(table)
        click.echo(f"\nTotal articles displayed: {len(articles)}")


def _fetch_articles_generator(source_urls: Tuple[str], limit: int):
    """Generator that yields articles from the database based on the source."""
    # Generate cache key
    cache_key = generate_cache_key(source_urls, limit)
    
    # Try to get from cache first
    try:
        cached_data = redis_client.get(cache_key)
        if cached_data:
            logger.info(f"Cache hit for key: {cache_key}")
            articles = json.loads(cached_data)
            # Convert back to tuples to maintain compatibility
            for article in articles:
                yield tuple(article)
            return
    except Exception as e:
        logger.warning(f"Redis cache read failed: {e}. Falling back to database.")
    
    # Cache miss or error - fetch from database
    logger.info(f"Cache miss for key: {cache_key}. Fetching from database.")
    articles = []
    
    with get_db_connection() as connection:
        cursor = connection.cursor()
        
        if source_urls:
            for url in source_urls:
                normalized_url = normalize_url(url)
                cursor.execute(
                    "SELECT title, url, source FROM articles WHERE source = %s LIMIT %s",
                    (normalized_url, limit)
                )
                results = cursor.fetchall()
                articles.extend(results)
                yield from results
        else:
            cursor.execute("SELECT title, url, source FROM articles LIMIT %s", (limit,))
            results = cursor.fetchall()
            articles.extend(results)
            yield from results
    
    # Store in cache for future use
    try:
        # Convert tuples to lists for JSON serialization
        articles_list = [list(article) for article in articles]
        redis_client.setex(
            cache_key,
            Config.CACHE_EXPIRY,  # 1 hour expiry from config
            json.dumps(articles_list)
        )
        logger.info(f"Cached {len(articles)} articles with key: {cache_key}")
    except Exception as e:
        logger.warning(f"Redis cache write failed: {e}. Continuing without caching.")


@cli.command()
@click.option('--feeds', '-f', multiple=True, default=Config.DEFAULT_FEED_URLS, help='RSS feed URLs to fetch')
@handle_errors
def fetch(feeds):
    """Fetches articles from the user's inputted RSS feeds and stores them in the database."""
    click.echo(f"Fetching articles from {len(feeds)} feeds...")
    
    with click.progressbar(feeds, label='Processing feeds') as bar:
        articles_from_generator = fetch_rss_feeds(list(feeds))
        inserted = add_to_db(articles_from_generator)
        for _ in bar:
            pass  # Progress bar will iterate through feeds
    
    click.echo(f"Successfully inserted {inserted} new articles.")


@cli.command()
@handle_errors
def stats():
    """Display statistics about stored articles."""
    stats = get_article_stats()
    
    click.echo("\n--- Article Statistics ---")
    click.echo(f"Total articles: {stats['total']}")
    click.echo("\nArticles by source:")
    
    if stats['by_source']:
        table = tabulate(
            stats['by_source'],
            headers=['Source', 'Count'],
            tablefmt='simple'
        )
        click.echo(table)


@cli.command()
@click.confirmation_option(prompt='Are you sure you want to clear all articles?')
@with_database
def clear(connection):
    """Clear all articles from the database."""
    cursor = connection.cursor()
    cursor.execute("DELETE FROM articles")
    click.echo(f"Cleared {cursor.rowcount} articles from the database.")


@cli.command()
def ping():
    """Test to see if the CLI works."""
    click.echo("Pong! CLI is running.")
