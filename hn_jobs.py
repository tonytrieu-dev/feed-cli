"""
HackerNews Job Scraper Module

This module fetches and parses job postings from HackerNews "Who's Hiring" threads,
with a focus on internship and new grad positions.
"""

import re
import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timezone
from urllib.parse import urljoin
import requests
import time

try:
    from bs4 import BeautifulSoup
except ImportError:
    raise ImportError("BeautifulSoup4 is required. Install with: pip install beautifulsoup4")

from db import get_db_connection
from config import Config
from decorators import timeit, with_retry
from utils import get_redis_client

logger = logging.getLogger(__name__)


class HNJobScraper:
    """Scraper for HackerNews job postings."""
    
    BASE_URL = "https://hacker-news.firebaseio.com/v0"
    HN_ITEM_URL = "https://news.ycombinator.com/item?id={}"
    
    def __init__(self):
        self.session = requests.Session()
        self.redis_client = get_redis_client()
        self.config = Config()
        
    @with_retry(max_attempts=3, delay=1)
    def _fetch_json(self, url: str) -> Optional[Dict]:
        """Fetch JSON data from HN API with retry logic."""
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    def find_whos_hiring_posts(self, limit: int = 3) -> List[Dict]:
        """Find recent 'Who is hiring?' posts and individual job postings."""
        logger.info("Searching for hiring posts (both community threads and individual job postings)")
        
        # Check cache first
        cache_key = "hn:whos_hiring_posts"
        cached = self.redis_client.get(cache_key)
        if cached:
            import json
            cached_data = json.loads(cached)
            logger.info(f"Using cached data: {len(cached_data)} posts from cache")
            return cached_data
        
        hiring_posts = []
        
        # Phase 1: Try to find traditional "Who is hiring?" posts from Ask HN
        logger.info("Phase 1: Searching Ask HN stories for 'Who is hiring?' posts")
        ask_stories = self._fetch_json(f"{self.BASE_URL}/askstories.json")
        
        if ask_stories:
            logger.info(f"Retrieved {len(ask_stories)} Ask HN stories, checking first 100 for hiring posts")
            
            # Check first 100 stories for hiring posts
            for story_id in ask_stories[:100]:
                story = self._fetch_json(f"{self.BASE_URL}/item/{story_id}.json")
                if not story:
                    continue
                    
                title = story.get('title', '').lower()
                if 'who is hiring?' in title or 'who\'s hiring?' in title:
                    # Extract month/year from title
                    month_year = self._extract_month_year(story.get('title', ''))
                    story['month_year'] = month_year
                    story['url'] = self.HN_ITEM_URL.format(story_id)
                    story['source_type'] = 'community_thread'
                    hiring_posts.append(story)
                    logger.info(f"Found community hiring thread: {story.get('title')}")
                    
                    if len(hiring_posts) >= limit:
                        break
        
        # Phase 2: If no community threads found, use individual job postings as fallback
        if not hiring_posts:
            logger.info("No community hiring threads found. Falling back to individual job postings")
            job_stories = self._fetch_json(f"{self.BASE_URL}/jobstories.json")
            
            if job_stories:
                logger.info(f"Retrieved {len(job_stories)} individual job postings, processing first {limit}")
                
                # Process individual job postings
                for story_id in job_stories[:limit]:
                    story = self._fetch_json(f"{self.BASE_URL}/item/{story_id}.json")
                    if not story:
                        continue
                    
                    # Format job posting as a pseudo hiring post
                    story['month_year'] = 'Individual Postings'
                    story['url'] = self.HN_ITEM_URL.format(story_id)
                    story['source_type'] = 'individual_job'
                    # For individual jobs, we'll treat them as posts with a single "comment" (the job itself)
                    story['kids'] = [story_id]  # Self-reference for processing
                    hiring_posts.append(story)
                    
                    logger.info(f"Added individual job posting: {story.get('title', 'No title')[:60]}...")
        
        # Cache results for 1 hour
        if hiring_posts:
            import json
            self.redis_client.setex(cache_key, 3600, json.dumps(hiring_posts))
            logger.info(f"Cached {len(hiring_posts)} posts for future use")
        
        total_posts = len(hiring_posts)
        source_type = hiring_posts[0]['source_type'] if hiring_posts else 'none'
        logger.info(f"Found {total_posts} hiring posts (source: {source_type})")
        
        return hiring_posts
    
    def _extract_month_year(self, title: str) -> Optional[str]:
        """Extract month and year from post title."""
        # Pattern: "Ask HN: Who is hiring? (December 2024)"
        match = re.search(r'\((\w+)\s+(\d{4})\)', title)
        if match:
            return f"{match.group(1)} {match.group(2)}"
        return None
    
    @timeit
    def fetch_job_comments(self, post_id: int, max_comments: int = 500) -> List[Dict]:
        """Fetch all top-level comments from a hiring post or return individual job posting."""
        logger.info(f"Fetching job content from post {post_id}")
        
        post = self._fetch_json(f"{self.BASE_URL}/item/{post_id}.json")
        if not post:
            logger.warning(f"Could not fetch post {post_id}")
            return []
        
        # Check if this is an individual job posting (has 'url' field) vs community thread
        if post.get('url') and not post.get('kids'):
            # This is an individual job posting - treat the post itself as the job content
            logger.info(f"Processing individual job posting: {post.get('title', 'No title')[:50]}...")
            
            # Create a pseudo-comment from the job posting
            job_comment = {
                'id': post_id,
                'by': post.get('by'),
                'time': post.get('time'),
                'text': post.get('text', '') if post.get('text') else post.get('title', ''),
                'parent_id': post_id,
                'url': post.get('url', self.HN_ITEM_URL.format(post_id))
            }
            
            logger.info(f"Created job entry from individual posting")
            return [job_comment]
        
        # Traditional community thread processing
        if 'kids' not in post:
            logger.info(f"Post {post_id} has no comments")
            return []
        
        comments = []
        comment_ids = post['kids'][:max_comments]
        logger.info(f"Processing {len(comment_ids)} comments from community thread")
        
        # Fetch comments in batches to avoid overwhelming the API
        batch_size = 30
        for i in range(0, len(comment_ids), batch_size):
            batch = comment_ids[i:i + batch_size]
            
            for comment_id in batch:
                comment = self._fetch_json(f"{self.BASE_URL}/item/{comment_id}.json")
                if comment and not comment.get('deleted') and comment.get('text'):
                    comment['parent_id'] = post_id
                    comment['url'] = self.HN_ITEM_URL.format(comment_id)
                    comments.append(comment)
            
            # Rate limiting
            if i + batch_size < len(comment_ids):
                time.sleep(0.5)
        
        logger.info(f"Fetched {len(comments)} comments from community thread")
        return comments
    
    def parse_job_posting(self, comment: Dict) -> Optional[Dict]:
        """Parse a comment to extract job details."""
        text = comment.get('text', '')
        if not text:
            logger.debug(f"Comment {comment.get('id', 'unknown')} has no text")
            return None
        
        # Clean HTML
        soup = BeautifulSoup(text, 'html.parser')
        clean_text = soup.get_text(separator='\n').strip()
        
        # Debug: Log the text length and first 200 chars for analysis
        logger.info(f"Parsing comment {comment.get('id', 'unknown')}: length={len(clean_text)}, preview='{clean_text[:200]}...'")
        
        # Reduce minimum length requirement - some job posts are shorter but valid
        if len(clean_text) < 50:
            logger.info(f"Comment {comment.get('id', 'unknown')} too short ({len(clean_text)} chars), skipping")
            return None
        
        job = {
            'hn_id': comment['id'],
            'parent_id': comment.get('parent_id'),
            'posted_by': comment.get('by'),
            'posted_at': datetime.fromtimestamp(comment.get('time'), tz=timezone.utc),
            'text': clean_text,
            'html_text': text,
            'url': comment.get('url'),
            'company': None,
            'role': None,
            'location': None,
            'salary_info': None,
            'is_remote': False,
            'is_internship': False,
            'is_new_grad': False,
            'keywords': []
        }
        
        # Extract company name
        company = self._extract_company(clean_text)
        if company:
            job['company'] = company
        
        # Extract role/position
        role = self._extract_role(clean_text)
        if role:
            job['role'] = role
        
        # Extract location
        location = self._extract_location(clean_text)
        if location:
            job['location'] = location
        
        # Extract salary info
        salary = self._extract_salary(clean_text)
        if salary:
            job['salary_info'] = salary
        
        # Check for remote work
        remote_keywords = ['remote', 'work from home', 'wfh', 'distributed', 'anywhere']
        job['is_remote'] = any(kw in clean_text.lower() for kw in remote_keywords)
        
        # Check for internship
        intern_keywords = ['intern', 'internship', 'co-op', 'summer position']
        job['is_internship'] = any(kw in clean_text.lower() for kw in intern_keywords)
        
        # Check for new grad
        newgrad_keywords = ['new grad', 'entry level', 'junior', 'fresh graduate', 'recent graduate']
        job['is_new_grad'] = any(kw in clean_text.lower() for kw in newgrad_keywords)
        
        # Extract keywords
        job['keywords'] = self._extract_keywords(clean_text)
        
        # Debug: Log extraction results
        logger.info(f"Comment {comment.get('id', 'unknown')} parsed - company: '{job['company']}', role: '{job['role']}'")
        
        # More flexible validation - accept if we have company, role, OR if it looks like a job posting
        has_job_indicators = any(keyword in clean_text.lower() for keyword in [
            'hiring', 'job', 'position', 'role', 'engineer', 'developer', 'intern', 
            'remote', 'salary', 'looking for', 'seeking', 'apply'
        ])
        
        if job['company'] or job['role'] or (has_job_indicators and len(clean_text) > 30):
            logger.info(f"Comment {comment.get('id', 'unknown')} accepted as valid job posting")
            return job
        
        logger.info(f"Comment {comment.get('id', 'unknown')} rejected - no company, role, or job indicators found")
        return None
    
    def _extract_company(self, text: str) -> Optional[str]:
        """Extract company name from job posting."""
        lines = text.split('\n')
        
        # Often the first line contains company name
        if lines:
            first_line = lines[0].strip()
            # Clean common patterns
            first_line = re.sub(r'^(YC\s*[SW]\d+\s*\|?\s*)', '', first_line)
            first_line = re.sub(r'\s*\|.*$', '', first_line)
            first_line = re.sub(r'\s*\(.*\)$', '', first_line)
            
            if len(first_line) > 2 and len(first_line) < 100:
                return first_line.strip()
        
        # Look for patterns like "at Company" or "Company is hiring"
        patterns = [
            r'(?:at|@)\s+([A-Z][\w\s&,\.\-]+?)(?:\s*\||$|\n)',
            r'^([A-Z][\w\s&,\.\-]+?)\s+(?:is|are)\s+(?:hiring|looking|seeking)',
            r'(?:Company|We are|About)\s*:\s*([A-Z][\w\s&,\.\-]+?)(?:\s*\||$|\n)',
            # More flexible patterns for job posts
            r'([A-Z][a-zA-Z0-9\s&,\.\-]{2,30})\s*(?:-|–|is|are)\s*(?:hiring|looking|seeking)',
            r'(?:^|\n)([A-Z][a-zA-Z0-9\s&,\.\-]{2,50})\s*(?:\||$)',
            # Pattern for "We at Company" or "Company team"
            r'(?:we\s+at|team\s+at)\s+([A-Z][\w\s&,\.\-]+?)(?:\s|$|\n)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.MULTILINE | re.IGNORECASE)
            if match:
                company = match.group(1).strip()
                # Clean up common suffixes
                company = re.sub(r'\s*(Inc\.?|LLC|Ltd\.?|Corp\.?)\s*$', '', company, flags=re.IGNORECASE)
                # Filter out common false positives
                if len(company) > 2 and not any(word in company.lower() for word in ['http', 'www', 'email', 'looking', 'hiring']):
                    return company
        
        return None
    
    def _extract_role(self, text: str) -> Optional[str]:
        """Extract job role/position from posting."""
        patterns = [
            r'(?:hiring|seeking|looking for|need)\s+(?:a|an)?\s*([A-Za-z\s\-/]+?)(?:\s*\||$|\n|\.)',
            r'(?:Position|Role|Title)\s*:\s*([A-Za-z\s\-/]+?)(?:\s*\||$|\n)',
            r'(?:Software|Senior|Junior|Staff|Principal)\s+([A-Za-z\s\-/]+?)(?:\s*\||$|\n|\.)',
            # More flexible patterns
            r'(?:we\'re|we are)\s+(?:hiring|looking for)\s+(?:a|an)?\s*([A-Za-z\s\-/]+?)(?:\s*\||$|\n)',
            r'(?:join us as|join our team as)\s+(?:a|an)?\s*([A-Za-z\s\-/]+?)(?:\s*\||$|\n)',
            r'([A-Za-z\s\-/]+?)\s+(?:position|role|opportunity)(?:\s*\||$|\n)',
        ]
        
        text_lower = text.lower()
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                role = match.group(1).strip()
                # Filter out common false positives
                if len(role) > 3 and not any(word in role.lower() for word in ['the', 'our', 'we', 'you', 'and', 'or']):
                    return role
        
        # Look for common role keywords with broader context
        role_keywords = [
            'engineer', 'developer', 'designer', 'manager', 'analyst',
            'scientist', 'researcher', 'architect', 'consultant', 'specialist',
            'intern', 'internship', 'graduate', 'junior', 'senior', 'lead'
        ]
        
        for keyword in role_keywords:
            if keyword in text_lower:
                # Find the context around the keyword
                pattern = rf'([A-Za-z\s\-/]*{keyword}[A-Za-z\s\-/]*)'
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    role = match.group(1).strip()
                    if 3 < len(role) < 50:  # Reasonable length for a role
                        return role
        
        return None
    
    def _extract_location(self, text: str) -> Optional[str]:
        """Extract location from job posting."""
        patterns = [
            r'(?:Location|Based in|Office)\s*:\s*([A-Za-z\s,\-]+?)(?:\s*\||$|\n)',
            r'(?:in|at)\s+(?:our)?\s*([A-Z][A-Za-z\s,]+?)\s+(?:office|location|HQ)',
            r'(?:San Francisco|New York|NYC|London|Berlin|Tokyo|Seattle|Boston|Austin|Denver|Toronto|Vancouver)',
        ]
        
        # Common city names to look for
        cities = [
            'San Francisco', 'New York', 'NYC', 'London', 'Berlin', 'Tokyo',
            'Seattle', 'Boston', 'Austin', 'Denver', 'Toronto', 'Vancouver',
            'Los Angeles', 'Chicago', 'Paris', 'Amsterdam', 'Singapore',
            'Hong Kong', 'Sydney', 'Melbourne', 'Dublin', 'Tel Aviv'
        ]
        
        # Check for explicit location patterns
        for pattern in patterns[:2]:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        # Check for city names
        for city in cities:
            if city in text:
                return city
        
        return None
    
    def _extract_salary(self, text: str) -> Optional[str]:
        """Extract salary information from job posting."""
        patterns = [
            r'(?:\$|USD|€|EUR|£|GBP)\s*(\d{1,3}(?:,\d{3})*(?:k|K)?)\s*(?:-|to|–)\s*(?:\$|USD|€|EUR|£|GBP)?\s*(\d{1,3}(?:,\d{3})*(?:k|K)?)',
            r'(\d{1,3}(?:,\d{3})*(?:k|K)?)\s*(?:-|to|–)\s*(\d{1,3}(?:,\d{3})*(?:k|K)?)\s*(?:\$|USD|€|EUR|£|GBP)',
            r'(?:salary|compensation|pay)\s*(?:range)?[:\s]+([^\n]+?)(?:\n|$)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                if len(match.groups()) >= 2:
                    return f"{match.group(1)} - {match.group(2)}"
                else:
                    salary_text = match.group(1).strip()
                    if len(salary_text) < 100:  # Reasonable length
                        return salary_text
        
        return None
    
    def _extract_keywords(self, text: str) -> List[str]:
        """Extract relevant keywords from job posting."""
        # Technology keywords to look for
        tech_keywords = [
            # Languages
            'python', 'javascript', 'typescript', 'java', 'go', 'golang', 'rust',
            'c++', 'c#', 'ruby', 'php', 'swift', 'kotlin', 'scala', 'elixir',
            
            # Frameworks
            'react', 'vue', 'angular', 'django', 'flask', 'rails', 'spring',
            'express', 'fastapi', 'nextjs', 'svelte', 'flutter', 'react native',
            
            # Databases
            'postgresql', 'mysql', 'mongodb', 'redis', 'elasticsearch', 'dynamodb',
            'cassandra', 'neo4j', 'sqlite', 'oracle',
            
            # Cloud/DevOps
            'aws', 'gcp', 'azure', 'docker', 'kubernetes', 'k8s', 'terraform',
            'jenkins', 'gitlab', 'github', 'ci/cd', 'devops', 'microservices',
            
            # Data/ML
            'machine learning', 'ml', 'ai', 'artificial intelligence', 'data science',
            'tensorflow', 'pytorch', 'scikit-learn', 'pandas', 'numpy',
            
            # Other
            'api', 'rest', 'graphql', 'websocket', 'blockchain', 'web3',
            'security', 'linux', 'agile', 'scrum'
        ]
        
        text_lower = text.lower()
        found_keywords = []
        
        for keyword in tech_keywords:
            if keyword in text_lower:
                found_keywords.append(keyword)
        
        return list(set(found_keywords))[:20]  # Limit to 20 keywords
    
    @timeit
    def save_jobs_to_db(self, jobs: List[Dict]) -> Tuple[int, int]:
        """Save parsed jobs to the database."""
        if not jobs:
            logger.info("No jobs to save")
            return 0, 0
        
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            inserted = 0
            updated = 0
            
            try:
                for job in jobs:
                    # Convert keywords list to PostgreSQL array format
                    keywords_array = '{' + ','.join(f'"{kw}"' for kw in job.get('keywords', [])) + '}'
                    
                    cursor.execute("""
                        INSERT INTO jobs (
                            hn_id, parent_id, posted_by, posted_at, text, html_text,
                            url, company, role, location, salary_info, is_remote,
                            is_internship, is_new_grad, keywords
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (hn_id) DO UPDATE SET
                            text = EXCLUDED.text,
                            html_text = EXCLUDED.html_text,
                            company = EXCLUDED.company,
                            role = EXCLUDED.role,
                            location = EXCLUDED.location,
                            salary_info = EXCLUDED.salary_info,
                            is_remote = EXCLUDED.is_remote,
                            is_internship = EXCLUDED.is_internship,
                            is_new_grad = EXCLUDED.is_new_grad,
                            keywords = EXCLUDED.keywords,
                            updated_at = CURRENT_TIMESTAMP
                        RETURNING (xmax = 0) AS inserted
                    """, (
                        job['hn_id'], job.get('parent_id'), job['posted_by'],
                        job['posted_at'], job['text'], job['html_text'],
                        job['url'], job['company'], job['role'], job['location'],
                        job.get('salary_info'), job['is_remote'], job['is_internship'],
                        job['is_new_grad'], keywords_array
                    ))
                    
                    result = cursor.fetchone()
                    if result[0]:  # inserted
                        inserted += 1
                    else:  # updated
                        updated += 1
                
                logger.info(f"Saved {inserted} new jobs, updated {updated} existing jobs")
                return inserted, updated
                
            except Exception as e:
                logger.error(f"Error saving jobs to database: {e}")
                raise
            finally:
                cursor.close()
    
    def fetch_and_save_latest_jobs(self, posts_limit: int = 1) -> Dict[str, int]:
        """Main method to fetch latest jobs and save to database."""
        logger.info(f"=== Starting HackerNews job fetch (requesting {posts_limit} posts) ===")
        
        stats = {
            'posts_found': 0,
            'comments_fetched': 0,
            'jobs_parsed': 0,
            'jobs_inserted': 0,
            'jobs_updated': 0
        }
        
        # Test API connectivity
        try:
            test_response = self._fetch_json(f"{self.BASE_URL}/maxitem.json")
            if test_response:
                logger.info(f"✅ HN API connectivity confirmed (max item: {test_response})")
            else:
                logger.error("❌ HN API connectivity failed")
                return stats
        except Exception as e:
            logger.error(f"❌ API connectivity test failed: {e}")
            return stats
        
        # Find recent hiring posts
        logger.info("🔍 Searching for hiring posts...")
        hiring_posts = self.find_whos_hiring_posts(limit=posts_limit)
        stats['posts_found'] = len(hiring_posts)
        
        if not hiring_posts:
            logger.warning("⚠️ No hiring posts found - this explains the zero results!")
            return stats
        
        logger.info(f"📊 Found {len(hiring_posts)} hiring posts to process")
        
        all_jobs = []
        
        for i, post in enumerate(hiring_posts, 1):
            post_title = post.get('title', 'No title')[:60]
            source_type = post.get('source_type', 'unknown')
            logger.info(f"📝 Processing post {i}/{len(hiring_posts)}: {post_title}... ({source_type})")
            
            # Fetch comments/content
            comments = self.fetch_job_comments(post['id'])
            stats['comments_fetched'] += len(comments)
            logger.info(f"📨 Retrieved {len(comments)} job entries from this post")
            
            # Parse jobs from comments
            jobs_from_this_post = 0
            for comment in comments:
                job = self.parse_job_posting(comment)
                if job:
                    all_jobs.append(job)
                    jobs_from_this_post += 1
                    if jobs_from_this_post <= 3:  # Log details for first few jobs
                        company = job.get('company', 'Unknown')
                        role = job.get('role', 'Unknown')
                        logger.info(f"   💼 Parsed job: {company} - {role}")
            
            logger.info(f"✅ Extracted {jobs_from_this_post} valid jobs from this post")
            
            # Rate limiting between posts
            if len(hiring_posts) > 1:
                time.sleep(2)
        
        stats['jobs_parsed'] = len(all_jobs)
        logger.info(f"🎯 Total jobs parsed: {stats['jobs_parsed']}")
        
        # Save to database
        if all_jobs:
            logger.info("💾 Saving jobs to database...")
            inserted, updated = self.save_jobs_to_db(all_jobs)
            stats['jobs_inserted'] = inserted
            stats['jobs_updated'] = updated
            logger.info(f"✅ Database operation complete: {inserted} new, {updated} updated")
        else:
            logger.warning("⚠️ No valid jobs to save to database")
        
        logger.info(f"🎉 Job fetch complete: {stats}")
        logger.info("=== End of job fetch operation ===")
        return stats


# CLI helper functions
def search_jobs(filters: Dict) -> List[Dict]:
    """Search jobs in the database with filters."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Build query
        query = "SELECT * FROM jobs WHERE 1=1"
        params = []
        
        if filters.get('internship'):
            query += " AND is_internship = TRUE"
        
        if filters.get('new_grad'):
            query += " AND is_new_grad = TRUE"
        
        if filters.get('remote'):
            query += " AND is_remote = TRUE"
        
        if filters.get('company'):
            query += " AND company ILIKE %s"
            params.append(f"%{filters['company']}%")
        
        if filters.get('location'):
            query += " AND location ILIKE %s"
            params.append(f"%{filters['location']}%")
        
        if filters.get('keywords'):
            # Search in keywords array
            query += " AND keywords && %s"
            params.append(filters['keywords'])
        
        if filters.get('days'):
            query += " AND posted_at >= CURRENT_TIMESTAMP - INTERVAL '%s days'"
            params.append(filters['days'])
        
        query += " ORDER BY posted_at DESC"
        
        if filters.get('limit'):
            query += " LIMIT %s"
            params.append(filters['limit'])
        
        cursor.execute(query, params)
        
        columns = [desc[0] for desc in cursor.description]
        jobs = []
        for row in cursor.fetchall():
            job = dict(zip(columns, row))
            jobs.append(job)
        
        cursor.close()
        return jobs


def get_job_stats() -> Dict:
    """Get statistics about jobs in the database."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Check if jobs table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'jobs'
            )
        """)
        if not cursor.fetchone()[0]:
            cursor.close()
            return {
                'total_jobs': 0,
                'internships': 0,
                'new_grad': 0,
                'remote': 0,
                'top_companies': [],
                'top_keywords': [],
                'jobs_by_day': []
            }
        
        stats = {}
        
        # Total jobs
        cursor.execute("SELECT COUNT(*) FROM jobs")
        stats['total_jobs'] = cursor.fetchone()[0]
        
        # Jobs by type
        cursor.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE is_internship) as internships,
                COUNT(*) FILTER (WHERE is_new_grad) as new_grad,
                COUNT(*) FILTER (WHERE is_remote) as remote
            FROM jobs
        """)
        row = cursor.fetchone()
        stats['internships'] = row[0]
        stats['new_grad'] = row[1]
        stats['remote'] = row[2]
        
        # Top companies
        cursor.execute("""
            SELECT company, COUNT(*) as count
            FROM jobs
            WHERE company IS NOT NULL
            GROUP BY company
            ORDER BY count DESC
            LIMIT 10
        """)
        stats['top_companies'] = cursor.fetchall()
        
        # Top keywords
        cursor.execute("""
            SELECT keyword, COUNT(*) as count
            FROM jobs, unnest(keywords) as keyword
            GROUP BY keyword
            ORDER BY count DESC
            LIMIT 20
        """)
        stats['top_keywords'] = cursor.fetchall()
        
        # Jobs by day
        cursor.execute("""
            SELECT DATE(posted_at) as date, COUNT(*) as count
            FROM jobs
            WHERE posted_at >= CURRENT_TIMESTAMP - INTERVAL '30 days'
            GROUP BY date
            ORDER BY date DESC
        """)
        stats['jobs_by_day'] = cursor.fetchall()
        
        cursor.close()
        return stats
    