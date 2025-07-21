import feedparser
from db import get_db_connection

def fetch_substack():
    feed = feedparser.parse("https://addyo.substack.com/feed")
    articles = [
        {"title": entry.title, "content": entry.summary, "source": "Substack", "url": entry.link}
        for entry in feed.entries
    ]
    return articles

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
    articles = fetch_substack()
    articles_list = list(articles)  # Convert generator to list
    add_to_db(articles_list)
    check_articles()