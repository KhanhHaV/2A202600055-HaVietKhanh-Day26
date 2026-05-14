import sqlite3
import os

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS students (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    cohort TEXT NOT NULL,
    score INTEGER
);

CREATE TABLE IF NOT EXISTS courses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    credits INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS enrollments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    student_id INTEGER NOT NULL,
    course_id INTEGER NOT NULL,
    grade TEXT,
    FOREIGN KEY(student_id) REFERENCES students(id),
    FOREIGN KEY(course_id) REFERENCES courses(id)
);
"""

SEED_SQL = """
INSERT INTO students (name, cohort, score) VALUES 
('Alice Smith', 'A1', 95),
('Bob Jones', 'A1', 82),
('Charlie Brown', 'B2', 76),
('Diana Prince', 'A1', 98),
('Eve Davis', 'B2', 88);

INSERT INTO courses (title, credits) VALUES 
('Computer Science 101', 3),
('Calculus I', 4),
('Physics for Engineers', 4);

INSERT INTO enrollments (student_id, course_id, grade) VALUES 
(1, 1, 'A'),
(1, 2, 'A'),
(2, 1, 'B'),
(3, 3, 'C'),
(4, 1, 'A+'),
(5, 2, 'B+');
"""

def create_database(db_path="database.sqlite"):
    """
    1. Open SQLite database file.
    2. Execute schema SQL.
    3. Execute seed SQL if empty.
    4. Commit.
    5. Return database path.
    """
    is_new = not os.path.exists(db_path)
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.executescript(SCHEMA_SQL)
        
        # Only seed if it's empty
        cursor.execute("SELECT COUNT(*) FROM students")
        if cursor.fetchone()[0] == 0:
            cursor.executescript(SEED_SQL)
            
        conn.commit()
    
    return db_path

if __name__ == "__main__":
    db_path = create_database()
    print(f"Database initialized at {db_path}")
