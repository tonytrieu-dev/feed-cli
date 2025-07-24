import logging
import logging.handlers
import os
from functools import wraps
from typing import Callable

def setup_logging(name: str = None, level: str = 'INFO') -> logging.Logger:
    """Set up a logger with console and file handlers."""
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter('%(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    
    # File handler with rotation
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(log_dir, 'tech_news_aggregator.log'),
        maxBytes=10485760,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(file_format)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

def log_function_call(logger: logging.Logger):
    """Decorator to log function calls with arguments."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            func_args = ', '.join([repr(arg) for arg in args])
            func_kwargs = ', '.join([f"{k}={v!r}" for k, v in kwargs.items()])
            all_args = ', '.join(filter(None, [func_args, func_kwargs]))
            
            logger.debug(f"Calling {func.__name__}({all_args})")
            try:
                result = func(*args, **kwargs)
                logger.debug(f"{func.__name__} returned {result!r}")
                return result
            except Exception as e:
                logger.error(f"{func.__name__} raised {e.__class__.__name__}: {e}")
                raise
        return wrapper
    return decorator