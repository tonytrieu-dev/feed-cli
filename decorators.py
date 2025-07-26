"""
Centralized decorators module for the Flexible CLI.

This module contains all decorators used throughout the application with comprehensive type hints and documentation.
"""

import time
import logging
import functools
from typing import Callable, TypeVar, ParamSpec, Any, Tuple, Optional, Union, cast
from contextlib import contextmanager

# Optional dependency handling
try:
    import psycopg2
except ImportError:
    psycopg2 = None

# Type variables for better type hints
P = ParamSpec('P')
R = TypeVar('R')
F = TypeVar('F', bound=Callable[..., Any])

logger = logging.getLogger(__name__)


def timeit(func: Callable[P, R]) -> Callable[P, R]:
    """
    Measure and log the execution time of a function.
    
    This decorator wraps a function to measure its execution time and logs
    the duration using the application's logger at INFO level.
    
    Args:
        func: The function to be timed
        
    Returns:
        A wrapped function that logs execution time
        
    Example:
        @timeit
        def fetch_data():
            # Some time-consuming operation
            pass
            
        # Logs: "fetch_data took 1.23 seconds to execute"
    """
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            end_time = time.time()
            duration = end_time - start_time
            logger.info(f"{func.__name__} took {duration:.2f} seconds to execute")
    return wrapper


def with_retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    exceptions: Tuple[type[Exception], ...] = (Exception,)
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Retry a function call with exponential backoff.
    
    This decorator retries a function if it raises an exception, with an
    exponentially increasing delay between attempts.
    
    Args:
        max_attempts: Maximum number of attempts (default: 3)
        delay: Initial delay in seconds between retries (default: 1.0)
        exceptions: Tuple of exception types to catch (default: all exceptions)
        
    Returns:
        A decorator that adds retry logic to a function
        
    Example:
        @with_retry(max_attempts=3, delay=1.0)
        def fetch_from_api():
            # API call that might fail
            pass
            
        @with_retry(exceptions=(ConnectionError, TimeoutError))
        def connect_to_service():
            # Only retry on specific exceptions
            pass
    """
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            last_exception: Optional[Exception] = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        sleep_time = delay * (2 ** attempt)
                        logger.warning(
                            f"{func.__name__} failed (attempt {attempt + 1}/{max_attempts}). "
                            f"Retrying in {sleep_time:.1f}s... Error: {e}"
                        )
                        time.sleep(sleep_time)
                    else:
                        logger.error(f"{func.__name__} failed after {max_attempts} attempts: {e}")
            
            if last_exception:
                raise last_exception
            
            # This should never be reached, but satisfies type checker
            raise RuntimeError("Unexpected state in retry decorator")
        
        return wrapper
    return decorator


def handle_errors(func: Callable[P, R]) -> Callable[P, Optional[R]]:
    """
    Handle CLI errors gracefully.
    
    This decorator catches exceptions in CLI commands and logs them appropriately,
    preventing stack traces from being shown to users while still logging the
    full error for debugging.
    
    Args:
        func: The CLI command function to wrap
        
    Returns:
        A wrapped function that handles errors gracefully
        
    Example:
        @click.command()
        @handle_errors
        def fetch_news():
            # CLI command that might fail
            pass
    """
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> Optional[R]:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}", exc_info=True)
            # Import click here to avoid circular imports
            import click
            click.echo(f"Error: {e}", err=True)
            return None
    return wrapper


def validate_feed_url(func: Callable[..., R]) -> Callable[..., R]:
    """
    Validate RSS feed URLs before processing.
    
    This decorator ensures that all feed URLs start with http:// or https://
    before passing them to the decorated function. Invalid URLs are logged
    and filtered out.
    
    Args:
        func: Function that accepts feed URLs as first parameter
        
    Returns:
        A wrapped function that validates URLs
        
    Example:
        @validate_feed_url
        def process_feeds(feed_urls: list[str]):
            # All URLs are guaranteed to be valid
            pass
    """
    @functools.wraps(func)
    def wrapper(feed_urls: list[str], *args: Any, **kwargs: Any) -> R:
        validated_urls = []
        for url in feed_urls:
            if url.startswith(('http://', 'https://')):
                validated_urls.append(url)
            else:
                logger.warning(f"Invalid URL skipped: {url}")
        return func(validated_urls, *args, **kwargs)
    return wrapper


def with_database(func: Callable[..., R]) -> Callable[..., R]:
    """
    Automatically handle database connections.
    
    This decorator provides a database connection as the first argument
    to the decorated function and handles connection lifecycle, including
    proper cleanup and transaction management.
    
    Args:
        func: Function that requires a database connection as first parameter
        
    Returns:
        A wrapped function with automatic connection handling
        
    Example:
        @with_database
        def get_articles(connection, limit=10):
            cursor = connection.cursor()
            # Database operations
            pass
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> R:
        # Import here to avoid circular imports
        from db import get_db_connection
        
        with get_db_connection() as connection:
            return func(connection, *args, **kwargs)
    return wrapper


def require_config(*config_keys: str) -> Callable[[F], F]:
    """
    Ensure required configuration values are present.
    
    This decorator validates that specified configuration keys exist and have
    values before allowing the decorated function to execute. Missing configs
    result in a ValueError.
    
    Args:
        *config_keys: Names of required configuration keys
        
    Returns:
        A decorator that validates configuration
        
    Example:
        @require_config('DB_NAME', 'DB_USER', 'DB_PASSWORD')
        def connect_to_database():
            # All required configs are guaranteed to exist
            pass
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Import here to avoid circular imports
            from config import Config
            
            missing = []
            for key in config_keys:
                if not getattr(Config, key, None):
                    missing.append(key)
            if missing:
                raise ValueError(f"Missing required configuration: {', '.join(missing)}")
            return func(*args, **kwargs)
        return cast(F, wrapper)
    return decorator


def log_function_call(
    logger_instance: logging.Logger,
    log_level: int = logging.DEBUG,
    log_result: bool = True,
    log_args: bool = True
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Log function calls with arguments and results.
    
    This decorator logs function calls, including arguments, keyword arguments,
    return values, and any exceptions raised. Useful for debugging and monitoring.
    
    Args:
        logger_instance: Logger instance to use for logging
        log_level: Logging level (default: DEBUG)
        log_result: Whether to log return values (default: True)
        log_args: Whether to log arguments (default: True)
        
    Returns:
        A decorator that logs function calls
        
    Example:
        @log_function_call(logger)
        def process_data(data: dict, validate: bool = True):
            return {"processed": True}
            
        # Logs:
        # DEBUG - Calling process_data({'key': 'value'}, validate=True)
        # DEBUG - process_data returned {'processed': True}
    """
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            if log_args:
                func_args = ', '.join([repr(arg) for arg in args])
                func_kwargs = ', '.join([f"{k}={v!r}" for k, v in kwargs.items()])
                all_args = ', '.join(filter(None, [func_args, func_kwargs]))
                logger_instance.log(log_level, f"Calling {func.__name__}({all_args})")
            else:
                logger_instance.log(log_level, f"Calling {func.__name__}")
            
            try:
                result = func(*args, **kwargs)
                if log_result:
                    logger_instance.log(log_level, f"{func.__name__} returned {result!r}")
                return result
            except Exception as e:
                logger_instance.error(f"{func.__name__} raised {e.__class__.__name__}: {e}")
                raise
        
        return wrapper
    return decorator


def database_retry(
    max_attempts: int = 3,
    delay: float = 0.5
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Specialized retry decorator for database operations.
    
    This decorator specifically handles database-related exceptions with
    appropriate retry logic for transient failures like connection drops
    or lock timeouts.
    
    Args:
        max_attempts: Maximum number of retry attempts (default: 3)
        delay: Initial delay between retries in seconds (default: 0.5)
        
    Returns:
        A decorator that adds database-specific retry logic
        
    Example:
        @database_retry(max_attempts=5)
        def update_articles(connection, articles):
            # Database operation that might face transient failures
            pass
    """
    if psycopg2 is None:
        # Fallback to generic retry if psycopg2 not available
        return with_retry(max_attempts=max_attempts, delay=delay)
    
    return with_retry(
        max_attempts=max_attempts,
        delay=delay,
        exceptions=(psycopg2.OperationalError, psycopg2.InterfaceError)
    )
