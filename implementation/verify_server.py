import json
from mcp_server import mcp, adapter, search, insert, aggregate, database_schema, table_schema

def verify():
    print("--- Verifying MCP Server Tools Locally ---")
    
    # 1. Test search
    print("\n[Search] Fetching students in cohort A1:")
    res = search(table="students", filters={"cohort": "A1"})
    print(res)

    # 2. Test insert
    print("\n[Insert] Adding a new student:")
    res = insert(table="students", values={"name": "Test Student", "cohort": "C3", "score": 100})
    print(res)

    # 3. Test aggregate
    print("\n[Aggregate] Average score of students:")
    res = aggregate(table="students", metric="avg", column="score")
    print(res)
    
    print("\n[Aggregate] Count of students by cohort:")
    res = aggregate(table="students", metric="count", group_by=["cohort"])
    print(res)

    # 4. Test resource: schema
    print("\n[Resource] Database Schema Summary:")
    res = database_schema()
    parsed = json.loads(res)
    print(f"Tables found: {list(parsed.get('schema', {}).keys())}")

    print("\n[Resource] Students Table Schema:")
    res = table_schema(table_name="students")
    print(res)

    # 5. Error handling tests
    print("\n[Error Handling] Searching an unknown table:")
    res = search(table="non_existent")
    print(res)

    print("\n[Error Handling] Invalid operator:")
    res = search(table="students", filters={"score": {"DROP TABLE": 100}})
    print(res)
    
if __name__ == "__main__":
    verify()
