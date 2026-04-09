import requests
import sqlite3

BASE_URL = "https://api.github.com/repos/apache/airflow/commits"
DB_NAME = "commits.db"


def extract_commits(): # Extract commits from GitHub API with pagination
    all_commits = []
    page = 1

    while True:
        params = { # Fetch commits from the main branch for January 2026, 100 per page
            "sha": "main",
            "since": "2026-01-01T00:00:00Z",
            "until": "2026-01-31T23:59:59Z",
            "per_page": 100,
            "page": page,
        }

        headers = { # Set headers for GitHub API request
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

        response = requests.get(BASE_URL, params=params, headers=headers, timeout=30) # Make the API request with a timeout
        response.raise_for_status()

        commits = response.json() # Parse the JSON response
        if not commits:
            break

        all_commits.extend(commits) # Add the commits from this page to the list
        page += 1

    return all_commits


def transform_commits(raw_commits): # Transform raw commit data into a structured format for database insertion
    transformed = []

    for commit in raw_commits: # Extract relevant fields from each commit and handle missing data gracefully
        row = {
            "sha": commit.get("sha"),
            "message": commit.get("commit", {}).get("message", "").strip(),
            "author_name": commit.get("commit", {}).get("author", {}).get("name"),
            "author_email": commit.get("commit", {}).get("author", {}).get("email"),
            "author_date": commit.get("commit", {}).get("author", {}).get("date"),
            "committer_name": commit.get("commit", {}).get("committer", {}).get("name"),
            "committer_email": commit.get("commit", {}).get("committer", {}).get("email"),
            "committer_date": commit.get("commit", {}).get("committer", {}).get("date"),
            "author_login": (commit.get("author") or {}).get("login"),
            "committer_login": (commit.get("committer") or {}).get("login"),
            "html_url": commit.get("html_url"),
            "is_verified": int(commit.get("commit", {}).get("verification", {}).get("verified", False)),
            "parent_count": len(commit.get("parents", [])),
        }
        transformed.append(row)

    return transformed


def create_table(connection): # Create the commits table if it doesn't exist
    cursor = connection.cursor()

    # Execute the SQL command to create the table
    cursor.execute(""" 
    CREATE TABLE IF NOT EXISTS commits (
        sha TEXT PRIMARY KEY,
        message TEXT,
        author_name TEXT,
        author_email TEXT,
        author_date TEXT,
        committer_name TEXT,
        committer_email TEXT,
        committer_date TEXT,
        author_login TEXT,
        committer_login TEXT,
        html_url TEXT,
        is_verified INTEGER,
        parent_count INTEGER
    )
    """) 

    connection.commit()


def load_commits(connection, transformed_commits): # Load transformed commits into the database
    cursor = connection.cursor() # Insert each transformed commit into the commits table, using INSERT OR REPLACE to handle duplicates

    for row in transformed_commits:
        cursor.execute("""
        INSERT OR REPLACE INTO commits (
            sha, message, author_name, author_email, author_date,
            committer_name, committer_email, committer_date,
            author_login, committer_login, html_url, is_verified, parent_count
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["sha"],
            row["message"],
            row["author_name"],
            row["author_email"],
            row["author_date"],
            row["committer_name"],
            row["committer_email"],
            row["committer_date"],
            row["author_login"],
            row["committer_login"],
            row["html_url"],
            row["is_verified"],
            row["parent_count"],
        ))

    connection.commit()


def main(): # Main ETL process
    connection = sqlite3.connect(DB_NAME) # Connect to the SQLite database (or create it if it doesn't exist)

    create_table(connection) # Ensure the commits table exists before loading data

    raw_commits = extract_commits() # Extract commits from GitHub API
    transformed_commits = transform_commits(raw_commits) # Transform the raw commit data into a structured format
    load_commits(connection, transformed_commits) # Load the transformed commits into the database

    connection.close()
    print("ETL complete.")


if __name__ == "__main__":
    main()