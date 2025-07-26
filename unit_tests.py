import pytest
from unittest.mock import Mock, patch, MagicMock
from click.testing import CliRunner
import json

# Import your modules
from aggregator import article_generator, fetch_rss_feeds, add_to_db
from cli import cli, news, fetch_feeds
from db import get_db_connection

# Test Fixtures for Mock Feed Data
@pytest.fixture
def valid_feed_data():
    """Create a valid mock feed object."""
    mock_feed = MagicMock()
    mock_feed.bozo = False
    
    # Create mock entry object
    mock_entry = MagicMock()
    mock_entry.get = Mock(side_effect=lambda key, default=None: {
        'title': 'Test Article 1',
        'summary': 'Test content 1', 
        'link': 'https://example.com/1',
        'published_parsed': None
    }.get(key, default))
    
    mock_feed.entries = [mock_entry]
    return mock_feed


@pytest.fixture
def invalid_feed_data():
    """Create an invalid mock feed object (bozo=True)."""
    mock_feed = MagicMock()
    mock_feed.bozo = True
    mock_feed.bozo_exception = Exception("Invalid XML")
    mock_feed.entries = []  # No entries for invalid feed
    return mock_feed


@pytest.fixture
def test_feed_url():
    """Common test URL for consistency."""
    return "https://example.com/feed"


class TestArticleParsing:
    """Test article parsing functions."""
    
    @patch('aggregator.feedparser.parse')
    def test_article_generator_valid_feed(self, mock_parse, valid_feed_data, test_feed_url):
        """Test parsing a valid RSS feed using fixtures."""
        # Use fixture for mock data
        mock_parse.return_value = valid_feed_data
        
        # Use fixture for test URL
        articles = list(article_generator(test_feed_url))
        
        # Verify we got the expected article
        assert len(articles) == 1, "Should return exactly 1 article"
        
        # Check article structure and content
        article = articles[0]
        assert article['title'] == 'First Test Article'
        assert article['content'] == 'First piece of test content' 
        assert article['url'] == 'https://example.com/first'
        assert article['source'] == 'example.com'  # Extracted from URL
        assert 'fetched_at' in article  # Should add timestamp
        
        # Verify mock was called correctly
        mock_parse.assert_called_once_with(test_feed_url)
    
    @patch('aggregator.feedparser.parse')
    def test_article_generator_invalid_feed(self, mock_parse, invalid_feed_data, test_feed_url):
        """Test handling of invalid RSS feed using fixtures."""
        # Use fixture for mock data
        mock_parse.return_value = invalid_feed_data
        
        # Use fixture for test URL
        articles = list(article_generator(test_feed_url))
        
        # Assert no articles are yielded from invalid feed
        assert len(articles) == 0
        
        # Verify the mock was called correctly
        mock_parse.assert_called_once_with(test_feed_url)


class TestDatabase:
    """Test database operations."""
    
    def test_add_to_db_success(self):
        """Test successful article insertion."""
        # TODO: Mock get_db_connection()
        # TODO: Create mock articles generator
        # TODO: Call add_to_db()
        # TODO: Assert correct number inserted
        pass
    
    def test_add_to_db_duplicates(self):
        """Test duplicate URL handling."""
        # TODO: Mock database with existing URLs
        # TODO: Try to insert duplicate articles
        # TODO: Assert duplicates are skipped
        pass


class TestCLI:
    """Test CLI commands."""
    
    def test_news_command(self):
        """Test news display command."""
        runner = CliRunner()
        # TODO: Mock database connection
        # TODO: Run: result = runner.invoke(news, ['--count', '5'])
        # TODO: Assert result.exit_code == 0
        # TODO: Assert expected output in result.output
        pass
    
    def test_fetch_feeds_command(self):
        """Test RSS feed fetching command."""
        runner = CliRunner()
        # TODO: Mock fetch_rss_feeds() and add_to_db()
        # TODO: Run: result = runner.invoke(fetch_feeds, ['--feeds', 'test.com'])
        # TODO: Assert success message in output
        pass
