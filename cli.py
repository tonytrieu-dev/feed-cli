import click
from tabulate import tabulate
from db import get_db_connection
# from dotenv import load_dotenv
# import os

# load_dotenv()

def normalize_url(url):
    """Helper function to normalize URLs by removing protocol, trailing slashes, and /feed."""
    # Remove protocol (http:// or https://) and trailing slashes
    normalized_url = url
    normalized_url = normalized_url.replace("http://", "").replace("https://", "").rstrip("/")
    # Remove /feed if present
    if normalized_url.endswith("/feed"):
        normalized_url = normalized_url[:-5]
    return normalized_url

@click.group()
def cli():
    """Tech News Aggregator CLI"""
    pass


@cli.command()
@click.argument("user_source_urls", nargs=-1)
@click.option(
    "--count",
    "-c",
    default=4,
    type=int,
    help="Number of articles to display per feed (default: 4).",
)
def news(user_source_urls, count):
    """Fetches and displays up to a specified number of articles from the database for the given source URLs."""
    db_articles_to_display = []

    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                if user_source_urls:
                    for user_source_url in user_source_urls:
                        normalized_url = normalize_url(user_source_url)
                        click.echo(f"Querying for source: {normalized_url} with limit {count}")
                        cursor.execute(
                            "SELECT title, url FROM articles WHERE source = %s LIMIT %s",
                            (normalized_url, count)
                        )
                        articles = cursor.fetchall()
                        click.echo(f"Found {len(articles)} articles for {normalized_url}")
                        db_articles_to_display.extend(articles)
                else:
                    click.echo(f"Fetching up to {count} articles from all sources")
                    cursor.execute("SELECT title, url FROM articles LIMIT %s", (count,))
                    db_articles_to_display = cursor.fetchall()
    except Exception as e:
        click.echo(f"Error fetching articles from database: {e}")
        return

    if db_articles_to_display:
        headers = ["Title", "URL"]
        table = tabulate(db_articles_to_display, headers=headers, tablefmt="grid")
        click.echo("\n--- Fetched Articles from News Database ---")
        click.echo(table)
    else:
        click.echo("No articles found in the database for the specified sources.")


@cli.command()
def ping():
    """Test CLI responsiveness"""
    click.echo("Pong! CLI is running.")
