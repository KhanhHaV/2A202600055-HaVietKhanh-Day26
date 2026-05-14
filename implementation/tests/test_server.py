import pytest
import sqlite3
import json
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db import SQLiteAdapter, ValidationError

@pytest.fixture
def test_db(tmp_path):
    db_path = tmp_path / "test.sqlite"
    adapter = SQLiteAdapter(str(db_path))
    
    # Initialize schema and data
    with adapter._get_connection() as conn:
        cursor = conn.cursor()
        cursor.executescript("""
            CREATE TABLE students (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);
            INSERT INTO students (name, score) VALUES ('Alice', 90), ('Bob', 80);
        """)
        conn.commit()
    return adapter

def test_list_tables(test_db):
    tables = test_db.list_tables()
    assert "students" in tables

def test_search_valid(test_db):
    results = test_db.search("students", filters={"score": {">": 85}})
    assert len(results) == 1
    assert results[0]["name"] == "Alice"

def test_search_invalid_table(test_db):
    with pytest.raises(ValidationError, match="does not exist"):
        test_db.search("unknown_table")

def test_search_invalid_column(test_db):
    with pytest.raises(ValidationError, match="Unknown column"):
        test_db.search("students", filters={"invalid_col": 10})

def test_search_sql_injection_attempt(test_db):
    # Try to inject SQL in order_by
    with pytest.raises(ValidationError, match="Invalid identifier"):
        test_db.search("students", order_by="score; DROP TABLE students")

def test_insert_valid(test_db):
    inserted = test_db.insert("students", {"name": "Charlie", "score": 95})
    assert inserted["name"] == "Charlie"
    assert inserted["score"] == 95
    # Verify in DB
    results = test_db.search("students")
    assert len(results) == 3

def test_insert_invalid_column(test_db):
    with pytest.raises(ValidationError, match="Unknown column"):
        test_db.insert("students", {"name": "Dave", "unknown": 100})

def test_aggregate_valid(test_db):
    results = test_db.aggregate("students", metric="avg", column="score")
    assert len(results) == 1
    assert results[0]["result"] == 85.0

def test_aggregate_invalid_metric(test_db):
    with pytest.raises(ValidationError, match="Invalid aggregate metric"):
        test_db.aggregate("students", metric="DROP", column="score")
