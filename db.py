import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
import logging
from config import Config
from decorators import require_config, with_database

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


# with_database decorator now imported from decorators.py
# Note: with_retry decorator moved to utils.py for centralized use
# Import from utils.py when needed for database-specific retry logic
if __name__ == '__main__':
    try:
        connection = get_db_connection()
        print("Connection successful with psycopg2!")
        connection.close()
    except Exception as e:
        print(f"Connection failed: {e}")
