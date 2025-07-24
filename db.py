import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
from functools import wraps
import logging
from config import Config, require_config

logger = logging.getLogger(__name__)

# Connection pool for better performance
connection_pool = None


@require_config('DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT')
def init_connection_pool(minconn=1, maxconn=10):
    """Initialize the connection pool."""
    global connection_pool
    if not connection_pool:
        connection_pool = psycopg2.pool.SimpleConnectionPool(
            minconn,
            maxconn,
            dbname=Config.DB_NAME,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            host=Config.DB_HOST,
            port=Config.DB_PORT
        )
    return connection_pool


@contextmanager
def get_db_connection():
    """Context manager for database connections."""
    pool = init_connection_pool()
    connection = pool.getconn()
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        pool.putconn(connection)


def with_database(func):
    """Decorator to automatically handle database connections."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        with get_db_connection() as connection:
            return func(connection, *args, **kwargs)
    return wrapper


def with_retry(max_attempts=3):
    """Decorator to retry database operations on failure."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except psycopg2.OperationalError as e:
                    if attempt == max_attempts - 1:
                        raise
                    logger.warning(f"Database operation failed, retrying... (attempt {attempt + 1}/{max_attempts})")
            return None
        return wrapper
    return decorator


if __name__ == '__main__':
    try:
        connection = get_db_connection()
        print("Connection successful with psycopg2!")
        connection.close()
    except Exception as e:
        print(f"Connection failed: {e}")
