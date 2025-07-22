import feedparser
from db import get_db_connection

def fetch_rss_feeds(feed_urls):
    all_articles = []
    for url in feed_urls:
        feed = feedparser.parse(url)
        articles = [
            {"title": entry.title, "content": entry.summary, "source": url.split('//')[1].split('/')[0], "url": entry.link}
            for entry in feed.entries
        ]
        all_articles.extend(articles)
    return all_articles


def add_to_db(articles):
    connection = get_db_connection()
    cursor = connection.cursor()
    for article in articles:
        # Check for duplicates based on url
        cursor.execute(
            "SELECT url FROM articles WHERE url = %s",
            (article["url"],)
        )
        if not cursor.fetchone():
            cursor.execute(
                "INSERT INTO articles (title, content, source, url) VALUES (%s, %s, %s, %s)",
                (article["title"], article["content"], article["source"], article["url"])
            )
    connection.commit()
    cursor.close()
    connection.close()


def check_articles():
    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT title, url FROM articles")
    articles = cursor.fetchall()
    for article in articles:
        print(f"Title: {article[0]}, URL: {article[1]}")
    cursor.close()
    connection.close()


if __name__ == "__main__":
    feed_urls = ["https://addyo.substack.com/feed", "https://blog.bytemonk.io/feed"]
    articles = fetch_rss_feeds(feed_urls)
    add_to_db(articles)
    check_articles()