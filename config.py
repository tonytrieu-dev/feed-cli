import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Configuration class with validation."""
    DB_NAME = os.getenv("DB_NAME", "news")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "secret")
    DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
    DB_PORT = int(os.getenv("DB_PORT", "5433"))
    
    # RSS Feed configurations
    DEFAULT_FETCH_LIMIT = 10
    CACHE_EXPIRY = 3600  # 1 hour
    
    # Feed URLs
    DEFAULT_FEED_URLS = [
        "https://addyo.substack.com/feed",
        "https://blog.bytemonk.io/feed",
        "https://blog.codingconfessions.com/feed",
        "https://betterengineers.substack.com/feed",
        "https://newsletter.pragmaticengineer.com/feed",
        "https://refactoring.fm/feed",
        "https://blog.bytebytego.com/feed"
    ]
    
    # HackerNews Job Posting settings
    HN_BASE_URL = "https://hacker-news.firebaseio.com/v0"
    HN_JOB_KEYWORDS = ["intern", "internship", "new grad", "junior", "entry level", 
                       "university", "student", "recent graduate"]
    HN_JOB_FETCH_LIMIT = 50
    HN_LOOKBACK_DAYS = 7  # Check last 7 days of "Who's Hiring" posts
    AGGREGATION_INTERVAL_MINUTES = 60
    
    # HackerNews API rate limiting
    HN_API_DELAY = 0.5  # Delay between API calls in seconds
    HN_MAX_COMMENTS_PER_POST = 500  # Maximum comments to fetch per post
    HN_CACHE_TTL = 3600  # Cache TTL for HN posts (1 hour)
    
    # Redis configuration
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB = int(os.getenv("REDIS_DB", "0"))
    
    # Job filtering configuration
    JOB_MIN_TEXT_LENGTH = 100  # Minimum text length to consider as valid job posting
    JOB_MAX_KEYWORDS = 20  # Maximum keywords to extract per job

# require_config decorator moved to decorators.py for centralized use
