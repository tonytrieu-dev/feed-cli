import os
from functools import wraps
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
        "https://blog.bytemonk.io/feed"
    ]

def require_config(*config_keys):
    """Decorator to ensure required configuration values are present."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            missing = []
            for key in config_keys:
                if not getattr(Config, key, None):
                    missing.append(key)
            if missing:
                raise ValueError(f"Missing required configuration: {', '.join(missing)}")
            return func(*args, **kwargs)
        return wrapper
    return decorator