import feedparser
import logging
from datetime import datetime
from typing import Generator, List, Dict, Any
from urllib.parse import urlparse
from db import get_db_connection, with_database
from config import Config
from decorators import timeit, validate_feed_url, with_retry

logger = logging.getLogger(__name__)


def article_generator(feed_url: str) -> Generator[Dict[str, Any], None, None]:
    """Generator that yields articles from a single RSS feed."""
    try:
        feed = feedparser.parse(feed_url)
        if feed.bozo:
            logger.error(f"Failed to parse feed: {feed_url} - {feed.bozo_exception}")
            return
        
        source = urlparse(feed_url).netloc
        for entry in feed.entries:
            yield {
                "title": entry.get('title', 'No title'),
                "content": entry.get('summary', entry.get('description', 'No content')),
                "source": source,
                "url": entry.get('link', ''),
                "published": entry.get('published_parsed', None),
                "fetched_at": datetime.now()
            }
    except Exception as e:
        logger.error(f"Error processing feed {feed_url}: {e}")


@timeit
@validate_feed_url
def fetch_rss_feeds(feed_urls: List[str]) -> Generator[Dict[str, Any], None, None]:
    """Fetch articles from multiple RSS feeds using generators."""
    for url in feed_urls:
        logger.info(f"Fetching articles from: {url}")
        yield from article_generator(url)


@with_retry(max_attempts=3)
def add_to_db(articles_generator: Generator[Dict[str, Any], None, None]) -> int:
    """Add articles to database using batch operations."""
    inserted_count = 0
    batch_size = 100
    batch = []
    
    with get_db_connection() as connection:
        cursor = connection.cursor()
        
        for article in articles_generator:
            # Skip articles without URLs
            if not article.get('url'):
                continue
                
            batch.append(article)
            
            if len(batch) >= batch_size:
                inserted_count += _insert_batch(cursor, batch)
                batch = []
        
        # Insert remaining articles
        if batch:
            inserted_count += _insert_batch(cursor, batch)
    
    logger.info(f"Inserted {inserted_count} new articles")
    return inserted_count


def _insert_batch(cursor, articles: List[Dict[str, Any]]) -> int:
    """Insert a batch of articles, skipping duplicates."""
    inserted = 0
    
    # Get existing URLs in batch, filtering out None URLs
    urls = [article['url'] for article in articles if article.get('url')]
    
    if urls:  # Only query if we have URLs to check
        cursor.execute(
            "SELECT url FROM articles WHERE url = ANY(%s)",
            (urls,)
        )
        existing_urls = {row[0] for row in cursor.fetchall()}
    else:
        existing_urls = set()  # Empty set if no URLs
    
    # Insert new articles
    for article in articles:
        if article['url'] not in existing_urls:
            cursor.execute(
                "INSERT INTO articles (title, content, source, url) VALUES (%s, %s, %s, %s)",
                (article['title'], article['content'], article['source'], article['url'])
            )
            inserted += 1
    
    return inserted


@with_database
def get_article_stats(connection) -> Dict[str, Any]:
    """Get statistics about articles in the database."""
    cursor = connection.cursor()
    
    # Total articles
    cursor.execute("SELECT COUNT(*) FROM articles")
    total = cursor.fetchone()[0]
    
    # Articles by source
    cursor.execute("""
        SELECT source, COUNT(*) as count 
        FROM articles 
        GROUP BY source 
        ORDER BY count DESC
    """)
    by_source = cursor.fetchall()
    
    return {
        'total': total,
        'by_source': by_source
    }
