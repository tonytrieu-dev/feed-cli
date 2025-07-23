import click
from tabulate import tabulate
from db import get_db_connection
# from dotenv import load_dotenv
# import os

# load_dotenv()

@click.group()
def cli():
    """Tech News Aggregator CLI"""
    pass


def normalize_url(url):
    """Normalize URLs"""
    # Remove protocol (http:// or https://) and trailing slashes
    normalized_url = url
    normalized_url = normalized_url.replace("http://", "").replace("https://", "").rstrip("/")
    # Remove /feed if present
    if normalized_url.endswith("/feed"):
        normalized_url = normalized_url[:-5]
    return normalized_url


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
    """Fetches and displays all the articles from the user's inputted URLs"""
    # https://addyo.substack.com/feed https://blog.bytemonk.io/feed
    db_articles = []

    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                if user_source_urls:
                    for user_source_url in user_source_urls:
                        like_pattern = f"%{normalize_url(user_source_url)}%"
                        cursor.execute(f"SELECT DISTINCT title, url FROM articles WHERE source LIKE %s LIMIT %s", (like_pattern, count))
                        db_articles.extend(cursor.fetchall())
                else:
                    cursor.execute("SELECT DISTINCT title, url FROM articles LIMIT %s", (count,))
                    db_articles.extend(cursor.fetchall())
    except Exception as e:
        click.echo(f"Error fetching articles from database: {e}")
        return
    
    if db_articles:
        headers = ["Title", "URL"]
        table = tabulate(db_articles, headers=headers, tablefmt="grid")
        click.echo("\n--- Fetched Articles from News Database ---")
        click.echo(table)
    else:
        click.echo("No articles found in the database for the specified feeds.")


@cli.command()
def ping():
    """Test CLI responsiveness"""
    click.echo("Pong! CLI is running.")
