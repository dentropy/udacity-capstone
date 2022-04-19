reddit_comments_table_create = ("""
CREATE TABLE IF NOT EXISTS reddit_comments (
    author VARCHAR(1000), 
    body VARCHAR(40000), 
    score INT,
    can_gild VARCHAR(20),
    controversiality INT,
    created_utc VARCHAR(20),
    edited VARCHAR(20),
    guilded INT,
    comment_id VARCHAR(10),
    is_submitter VARCHAR(10),
    link_id VARCHAR(20),
    parent_id  VARCHAR(20),
    permalink  VARCHAR(1000),
    subreddit  VARCHAR(100)
);""")


git_metadata_table_create = ("""
CREATE TABLE IF NOT EXISTS git_metadata (
    author_email VARCHAR(2000),
    author_name VARCHAR(2000),
    commits INT,
    remote_url VARCHAR(10000)
);""")

list_table_names = ["reddit_comments", "git_metadata"]

reddit_comments_table_insert = ("""
INSERT INTO songs (author, body, score, can_gild, controversiality, created_utc, edited, guilded, comment_id, is_submitter, link_id, parent_id, permalink, subreddit)
    VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {});
""")