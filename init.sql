  CREATE TABLE IF NOT EXISTS articles (
      id SERIAL PRIMARY KEY,
      title VARCHAR(500) NOT NULL,
      url VARCHAR(1000) UNIQUE NOT NULL,
      content TEXT,
      source VARCHAR(100),
      published_date TIMESTAMP,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );