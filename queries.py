# SQL Queries for Maeil-Mail Scraper

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS maeil_mail_questions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    question_id INT UNIQUE,
    title VARCHAR(512),
    content TEXT,
    additional_links TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

INSERT_QUESTION = """
INSERT INTO maeil_mail_questions (question_id, title, content, additional_links)
VALUES (%(question_id)s, %(title)s, %(content)s, %(additional_links)s)
ON DUPLICATE KEY UPDATE
    title = VALUES(title),
    content = VALUES(content),
    additional_links = VALUES(additional_links)
"""

SELECT_ALL = "SELECT * FROM maeil_mail_questions ORDER BY question_id ASC"
