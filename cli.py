import click
import logging
from tabulate import tabulate
from typing import Tuple
from db import get_db_connection, with_database
from aggregator import fetch_rss_feeds, add_to_db, get_article_stats
from config import Config
import json
import hashlib
import schedule
import time
from datetime import datetime
from hn_jobs import HNJobScraper, search_jobs, get_job_stats, get_yc_cohort_stats
from utils import get_redis_client
from decorators import handle_errors


logger = logging.getLogger(__name__)


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


redis_client = get_redis_client()


@click.group()
def cli():
    """Multi-purpose CLI"""
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
def fetch_feeds(feeds):
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
@click.option('--interval', '-i', default=60, type=int, help='Interval in minutes')
@click.option('--once', is_flag=True, help='Run once and exit')
@handle_errors
def run_aggregator(interval, once):
    """Run periodic content aggregation."""
    if once:
        click.echo("🚀 Starting single aggregation run...")
        aggregate_periodically()
        click.echo("✅ Single aggregation run completed!")
    else:
        click.echo(f"🔄 Starting periodic aggregation every {interval} minutes...")
        click.echo("Press Ctrl+C to stop.")
        
        schedule.every(interval).minutes.do(aggregate_periodically)
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            click.echo("\n🛑 Stopping aggregator...")
            click.echo("Aggregator stopped.")


def aggregate_periodically():
    """Main aggregation function."""
    click.echo(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running aggregation...")
    
    # Fetch RSS feeds
    try:
        click.echo("📰 Fetching RSS feeds...")
        articles = fetch_rss_feeds(Config.DEFAULT_FEED_URLS)
        inserted = add_to_db(articles)
        
        click.echo(f"✅ Inserted {inserted} new articles from {len(Config.DEFAULT_FEED_URLS)} RSS feeds")
    except Exception as e:
        click.echo(f"❌ RSS feed error: {e}")
        logger.error(f"Error fetching RSS feeds: {e}")
        logger.exception("RSS feed error details:")
    
    # Fetch HackerNews jobs
    try:
        click.echo("🔍 Fetching HackerNews jobs...")
        
        scraper = HNJobScraper()
        stats = scraper.fetch_and_save_latest_jobs(posts_limit=1)
        
        click.echo(f"✅ Fetched {stats['jobs_parsed']} jobs ({stats['jobs_inserted']} new, {stats['jobs_updated']} updated)")
    except Exception as e:
        click.echo(f"❌ HN job error: {e}")
        logger.error(f"Error fetching HN jobs: {e}")
        logger.exception("HN job error details:")
    
    click.echo(f"🎉 Aggregation completed at {datetime.now().strftime('%H:%M:%S')}")
    click.echo("---")


@cli.command()
@click.option('--posts', '-p', default=1, type=int, help='Number of hiring posts to fetch')
@click.option('--force', is_flag=True, help='Force refresh, ignore cache')
@click.option('--clear-old', is_flag=True, help='Clear jobs older than 2025 before fetching')
@handle_errors
def fetch_jobs(posts, force, clear_old):
    """Fetch job postings from HackerNews Who's Hiring threads."""
    if force:
        # Clear cache
        redis_client.delete("hn:whos_hiring_posts")
    
    if clear_old:
        # Clear jobs older than 2025
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM jobs WHERE EXTRACT(YEAR FROM posted_at) < 2025")
            deleted_count = cursor.rowcount
            click.echo(f"🗑️ Cleared {deleted_count} jobs older than 2025")
    
    scraper = HNJobScraper()
    click.echo(f"Fetching jobs from {posts} most recent 'Who is hiring?' posts...")
    
    with click.progressbar(length=100, label='Fetching jobs') as bar:
        bar.update(10)
        stats = scraper.fetch_and_save_latest_jobs(posts_limit=posts)
        bar.update(90)
    
    click.echo(f"\n--- Job Fetch Summary ---")
    click.echo(f"Posts processed: {stats['posts_found']}")
    click.echo(f"Comments fetched: {stats['comments_fetched']}")
    click.echo(f"Jobs parsed: {stats['jobs_parsed']}")
    click.echo(f"New jobs: {stats['jobs_inserted']}")
    click.echo(f"Updated jobs: {stats['jobs_updated']}")


@cli.command()
@click.option('--internship', '-i', is_flag=True, help='Show only internships')
@click.option('--new-grad', '-n', is_flag=True, help='Show only new grad positions')
@click.option('--remote', '-r', is_flag=True, help='Show only remote positions')
@click.option('--company', '-c', help='Filter by company name')
@click.option('--location', '-l', help='Filter by location')
@click.option('--keyword', '-k', multiple=True, help='Filter by keywords (can use multiple)')
@click.option('--days', '-d', default=90, type=int, help='Show jobs from last N days (default: 90 for 2025+ jobs)')
@click.option('--limit', default=20, type=int, help='Maximum number of jobs to show')
@click.option('--format', type=click.Choice(['table', 'detailed', 'json']), default='table')
@click.option('--year', type=int, help='Filter by job posting year (e.g., 2025, 2026)')
@click.option('--yc-cohort', type=int, help='Filter by YC cohort year (e.g., 2025, 2026)')
@handle_errors
def jobs(internship, new_grad, remote, company, location, keyword, days, limit, format, year, yc_cohort):
    """Display job postings with various filters."""
    filters = {
        'internship': internship,
        'new_grad': new_grad,
        'remote': remote,
        'company': company,
        'location': location,
        'keywords': list(keyword) if keyword else None,
        'days': days,
        'limit': limit,
        'year': year,
        'yc_cohort_year': yc_cohort
    }
    
    jobs = search_jobs(filters)
    
    if not jobs:
        click.echo("No jobs found matching your criteria.")
        return
    
    if format == 'json':
        # Convert datetime objects to strings for JSON serialization
        for job in jobs:
            job['posted_at'] = job['posted_at'].isoformat() if job['posted_at'] else None
            job['created_at'] = job['created_at'].isoformat() if job['created_at'] else None
            job['updated_at'] = job['updated_at'].isoformat() if job['updated_at'] else None
        click.echo(json.dumps(jobs, indent=2))
    elif format == 'detailed':
        for job in jobs:
            click.echo(f"\n{'='*80}")
            click.echo(f"Company: {job['company'] or 'N/A'}")
            click.echo(f"Role: {job['role'] or 'N/A'}")
            click.echo(f"Location: {job['location'] or 'N/A'}")
            if job['salary_info']:
                click.echo(f"Salary: {job['salary_info']}")
            click.echo(f"Remote: {'Yes' if job['is_remote'] else 'No'}")
            click.echo(f"Type: {'Internship' if job['is_internship'] else 'New Grad' if job['is_new_grad'] else 'Full-time'}")
            click.echo(f"Posted: {job['posted_at'].strftime('%Y-%m-%d %H:%M') if job['posted_at'] else 'N/A'}")
            click.echo(f"Posted by: {job['posted_by'] or 'N/A'}")
            click.echo(f"HN Link: {job['url'] or 'N/A'}")
            if job['keywords']:
                click.echo(f"Keywords: {', '.join(job['keywords'])}")
            click.echo(f"\nDescription preview:")
            click.echo(job['text'][:500] + '...' if len(job['text']) > 500 else job['text'])
    else:  # table format
        # Prepare data for table
        table_data = []
        for job in jobs:
            job_type = 'Internship' if job['is_internship'] else 'New Grad' if job['is_new_grad'] else 'Full-time'
            remote_str = '🌍' if job['is_remote'] else ''
            table_data.append([
                job['company'] or 'N/A',
                (job['role'] or 'N/A')[:40] + '...' if job['role'] and len(job['role']) > 40 else job['role'] or 'N/A',
                job['location'] or 'N/A',
                f"{remote_str} {job_type}",
                job['posted_at'].strftime('%Y-%m-%d') if job['posted_at'] else 'N/A'
            ])
        
        headers = ['Company', 'Role', 'Location', 'Type', 'Posted']
        table = tabulate(table_data, headers=headers, tablefmt='grid')
        click.echo(f"\n--- Job Listings ({len(jobs)} jobs) ---")
        click.echo(table)


@cli.command()
@handle_errors
def job_stats():
    """Display statistics about job postings."""
    stats = get_job_stats()
    
    click.echo("\n--- Job Statistics ---")
    click.echo(f"Total jobs in database: {stats['total_jobs']}")
    click.echo(f"Internships: {stats['internships']}")
    click.echo(f"New grad positions: {stats['new_grad']}")
    click.echo(f"Remote positions: {stats['remote']}")
    
    if stats['top_companies']:
        click.echo("\n--- Top Companies ---")
        company_table = tabulate(
            stats['top_companies'][:10],
            headers=['Company', 'Jobs'],
            tablefmt='simple'
        )
        click.echo(company_table)
    
    if stats['top_keywords']:
        click.echo("\n--- Top Keywords/Technologies ---")
        keyword_table = tabulate(
            stats['top_keywords'][:15],
            headers=['Keyword', 'Count'],
            tablefmt='simple'
        )
        click.echo(keyword_table)
    
    if stats['jobs_by_day']:
        click.echo("\n--- Recent Activity (Last 7 Days) ---")
        recent_data = stats['jobs_by_day'][:7]
        day_table = tabulate(
            [(row[0].strftime('%Y-%m-%d'), row[1]) for row in recent_data],
            headers=['Date', 'Jobs Posted'],
            tablefmt='simple'
        )
        click.echo(day_table)


@cli.command()
@handle_errors
def yc_cohorts():
    """Display YC cohort statistics."""
    stats = get_yc_cohort_stats()
    
    click.echo("\n--- YC Cohort Statistics ---")
    click.echo(f"Total jobs from YC companies: {stats['total_yc_jobs']}")
    
    if stats['yc_cohorts']:
        click.echo("\n--- Jobs by YC Cohort Year ---")
        cohort_table = tabulate(
            [(row[0], row[1]) for row in stats['yc_cohorts']],
            headers=['YC Cohort Year', 'Jobs'],
            tablefmt='simple'
        )
        click.echo(cohort_table)
    else:
        click.echo("No YC companies found in database.")


@cli.command()
def ping():
    """Test to see if the CLI works."""
    click.echo("Pong! CLI is running.")
