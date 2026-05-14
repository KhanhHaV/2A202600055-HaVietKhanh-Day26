import sqlite3
import re
from typing import List, Dict, Any, Optional

class ValidationError(Exception):
    """Raised when a request cannot be safely executed."""
    pass

class DatabaseAdapter:
    """Abstract database adapter interface."""
    def list_tables(self) -> List[str]:
        raise NotImplementedError
    def get_table_schema(self, table: str) -> List[Dict[str, Any]]:
        raise NotImplementedError
    def search(self, table: str, columns: Optional[List[str]]=None, filters: Optional[Dict[str, Any]]=None, limit: int=20, offset: int=0, order_by: Optional[str]=None, descending: bool=False) -> List[Dict[str, Any]]:
        raise NotImplementedError
    def insert(self, table: str, values: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError
    def aggregate(self, table: str, metric: str, column: Optional[str]=None, filters: Optional[Dict[str, Any]]=None, group_by: Optional[List[str]]=None) -> List[Dict[str, Any]]:
        raise NotImplementedError

class SQLiteAdapter(DatabaseAdapter):
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _validate_identifier(self, identifier: str) -> str:
        """Ensure identifier is alphanumeric or underscore to prevent injection."""
        if not re.match(r'^[a-zA-Z0-9_]+$', identifier):
            raise ValidationError(f"Invalid identifier: {identifier}")
        return identifier

    def _validate_operator(self, operator: str) -> str:
        allowed = {"=", "!=", ">", "<", ">=", "<=", "LIKE", "IN"}
        op_upper = operator.upper()
        if op_upper not in allowed:
            raise ValidationError(f"Unsupported operator: {operator}")
        return op_upper

    def list_tables(self) -> List[str]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            return [row["name"] for row in cursor.fetchall()]

    def get_table_schema(self, table: str) -> List[Dict[str, Any]]:
        table = self._validate_identifier(table)
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            if not columns:
                raise ValidationError(f"Table '{table}' does not exist.")
            return [dict(row) for row in columns]

    def _validate_table_and_columns(self, table: str, columns: Optional[List[str]] = None):
        schema = self.get_table_schema(table)
        valid_cols = {col["name"] for col in schema}
        if columns:
            for col in columns:
                if col not in valid_cols:
                    raise ValidationError(f"Unknown column: '{col}' in table '{table}'")

    def _build_where_clause(self, table: str, filters: Optional[Dict[str, Any]]) -> tuple[str, list]:
        if not filters:
            return "", []
        
        self._validate_table_and_columns(table, list(filters.keys()))
        clauses = []
        params = []
        for col, condition in filters.items():
            self._validate_identifier(col)
            if isinstance(condition, dict):
                for op, val in condition.items():
                    op = self._validate_operator(op)
                    if op == "IN":
                        if not isinstance(val, (list, tuple)):
                            raise ValidationError("IN operator requires a list of values")
                        placeholders = ",".join(["?"] * len(val))
                        clauses.append(f"{col} IN ({placeholders})")
                        params.extend(val)
                    else:
                        clauses.append(f"{col} {op} ?")
                        params.append(val)
            else:
                clauses.append(f"{col} = ?")
                params.append(condition)
        
        return " WHERE " + " AND ".join(clauses), params

    def search(self, table: str, columns: Optional[List[str]]=None, filters: Optional[Dict[str, Any]]=None, limit: int=20, offset: int=0, order_by: Optional[str]=None, descending: bool=False) -> List[Dict[str, Any]]:
        self._validate_table_and_columns(table, columns)
        table = self._validate_identifier(table)
        
        select_cols = "*"
        if columns:
            select_cols = ", ".join([self._validate_identifier(c) for c in columns])
            
        where_clause, params = self._build_where_clause(table, filters)
        
        query = f"SELECT {select_cols} FROM {table}{where_clause}"
        
        if order_by:
            self._validate_identifier(order_by)
            query += f" ORDER BY {order_by} {'DESC' if descending else 'ASC'}"
            
        if not isinstance(limit, int) or limit < 0 or limit > 1000:
            raise ValidationError("Limit must be an integer between 0 and 1000")
        if not isinstance(offset, int) or offset < 0:
            raise ValidationError("Offset must be a non-negative integer")
            
        query += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def insert(self, table: str, values: Dict[str, Any]) -> Dict[str, Any]:
        if not values:
            raise ValidationError("Cannot execute empty insert")
            
        self._validate_table_and_columns(table, list(values.keys()))
        table = self._validate_identifier(table)
        
        cols = [self._validate_identifier(k) for k in values.keys()]
        placeholders = ",".join(["?"] * len(cols))
        query = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
        params = list(values.values())
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            
            # Fetch the inserted row back (assuming an auto-incrementing rowid or similar)
            # In SQLite, we can use lastrowid
            rowid = cursor.lastrowid
            
            # Retrieve the inserted row
            # If the table doesn't have an integer primary key, rowid still works in sqlite
            cursor.execute(f"SELECT * FROM {table} WHERE rowid = ?", (rowid,))
            inserted_row = cursor.fetchone()
            return dict(inserted_row) if inserted_row else values

    def aggregate(self, table: str, metric: str, column: Optional[str]=None, filters: Optional[Dict[str, Any]]=None, group_by: Optional[List[str]]=None) -> List[Dict[str, Any]]:
        self._validate_table_and_columns(table, [column] if column and column != "*" else None)
        table = self._validate_identifier(table)
        
        allowed_metrics = {"count", "sum", "avg", "min", "max"}
        metric_lower = metric.lower()
        if metric_lower not in allowed_metrics:
            raise ValidationError(f"Invalid aggregate metric: {metric}. Allowed: {allowed_metrics}")
            
        if column and column != "*":
            column = self._validate_identifier(column)
        else:
            if metric_lower != "count":
                raise ValidationError("Column must be specified for sum, avg, min, max")
            column = "*"
            
        where_clause, params = self._build_where_clause(table, filters)
        
        select_clause = f"{metric_lower}({column}) as result"
        group_by_clause = ""
        if group_by:
            self._validate_table_and_columns(table, group_by)
            gb_cols = [self._validate_identifier(c) for c in group_by]
            group_by_clause = f" GROUP BY {', '.join(gb_cols)}"
            select_clause = f"{', '.join(gb_cols)}, " + select_clause
            
        query = f"SELECT {select_clause} FROM {table}{where_clause}{group_by_clause}"
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

class PostgresAdapter(DatabaseAdapter):
    """
    Bonus: PostgresAdapter showing extensibility.
    Note: Requires psycopg2 or psycopg3 to be installed.
    """
    def __init__(self, dsn: str):
        self.dsn = dsn
        try:
            import psycopg2
            import psycopg2.extras
            self.psycopg2 = psycopg2
        except ImportError:
            pass

    def _get_connection(self):
        conn = self.psycopg2.connect(self.dsn)
        return conn, conn.cursor(cursor_factory=self.psycopg2.extras.DictCursor)

    def list_tables(self) -> List[str]:
        pass
    def get_table_schema(self, table: str) -> List[Dict[str, Any]]:
        pass
    def search(self, table: str, columns: Optional[List[str]]=None, filters: Optional[Dict[str, Any]]=None, limit: int=20, offset: int=0, order_by: Optional[str]=None, descending: bool=False) -> List[Dict[str, Any]]:
        pass
    def insert(self, table: str, values: Dict[str, Any]) -> Dict[str, Any]:
        pass
    def aggregate(self, table: str, metric: str, column: Optional[str]=None, filters: Optional[Dict[str, Any]]=None, group_by: Optional[List[str]]=None) -> List[Dict[str, Any]]:
        pass
