# Data Engineering Take-Home Assignment

## Overview

This project implements a simple end-to-end ETL (Extract, Transform, Load) pipeline using Python. The goal is to retrieve commit data from the Apache Airflow GitHub repository for January 2026, transform it into a structured format, and load it into a SQL database.

The solution is intentionally kept simple, readable, and modular to reflect a practical data engineering approach within the expected time constraints.

---

## Setup Instructions

### 1. Clone the repository

```bash
git clone https://github.com/klailatimad/take_home_de_assignment
cd take_home_de_assignment
```

### 2. Create and activate a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate      # macOS/Linux
.venv\Scripts\activate         # Windows
```

### 3. Install dependencies

```bash
pip install requests pysqlite3
```

### 4. Run the script

```bash
python main.py
```

This will:

* Fetch commit data from GitHub
* Transform the data
* Create a local SQLite database (`commits.db`)
* Load the processed data into the database

---

## General Approach

The solution follows a standard ETL structure:

### 1. Extract

* Data is retrieved from the GitHub REST API:

  * Repository: `apache/airflow`
  * Branch: `main`
  * Date range: January 2026
* Pagination is handled to ensure all commits are collected
* The extraction loop continues until no more results are returned

### 2. Transform

* Nested JSON data from the API is flattened into a structured format
* Only relevant fields are selected (e.g., commit SHA, message, author info)
* Minor cleaning is applied:

  * Commit messages are stripped of whitespace
  * Boolean values are normalized for database storage
* Defensive programming is used to handle missing fields safely

### 3. Load

* Data is stored in a local SQLite database (`commits.db`)
* A single table (`commits`) is created with a flat schema
* `INSERT OR REPLACE` is used to ensure idempotency and prevent duplicate records
* The `sha` field is used as the primary key

---

## Design Decisions

* **SQLite** was chosen for simplicity and zero setup, making the solution easy to run locally
* **Single Python file** aligns with assignment requirements and keeps the solution concise
* **pysqlite3 and requests libraries** were used for database interaction and API calls due to their simplicity and readability
* Data is stored in a **flattened structure** to simplify querying and loading
* The pipeline is broken into clear functions (`extract`, `transform`, `load`) for readability and maintainability

---

## Known Limitations

* **No authentication**: The GitHub API is used without authentication, which may hit rate limits in some cases
* **No advanced error handling**: Basic error handling is implemented, but retries and backoff strategies are not included
* **No logging framework**: The script uses simple print statements instead of structured logging
* **No incremental loading**: The pipeline performs a full extraction each time rather than tracking state
* **Limited validation**: Only basic data cleaning and safe access patterns are used
* **Not optimized for scale**: The solution is designed for clarity rather than performance or large-scale processing

---

## Potential Improvements

Given more time, the following enhancements could be added:

* Add **GitHub API authentication** to handle rate limits
* Implement **retry logic with exponential backoff**
* Introduce **incremental loading** based on commit timestamps
* Add **structured logging**
* Use **SQLAlchemy or ORM** for cleaner database interaction
* Add **unit tests** for transformation logic
* Containerize the pipeline using Docker

---

## Summary

This project demonstrates a clean and practical approach to building a small-scale ETL pipeline. The focus was on readability, modularity, and correctness, while making reasonable trade-offs given the time constraints.
