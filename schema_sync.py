import oracledb
from datetime import datetime

#CONFIG for database

ORIGIN_USER = {
    "user": "origin_user",
    "password": "origin_password",
    "dsn": "origin_db_dsn"
}

TARGET_USER = {
    "user": "target_user",
    "password": "target_password",
    "dsn": "target_db_dsn"
}

#FUNCTION to get schema objects from the database

OUTPUT_FILE = "schema_sync.sql"

def get_columns(conn,owner):
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT table_name, column_name, data_type, data_length, nullable
        FROM all_tab_columns
        WHERE owner = :owner
        ORDER BY table_name, column_id
        """,
       {"owner":owner.upper()})
    
    schema = {}
    for row in cursor:
        table = row[0]
        if table not in schema:
            schema [table] = {}
        schema[table][row[1]] = row[2:]
        return schema
    
    def build_definition(coldata):
        data_type, length, precision, scale, nullable = coldata

        if data_type == "VARCHAR2":
            dtype = f"{data_type}({length})"
        elif data_type == "NUMBER" :
            if precision :
                if scale :
                    dtype = f"{data_type}({precision},{scale})"
                else:
                    dtype = f"{data_type}({precision})"
            else:
                dtype = data_type
        else:
            dtype = data_type
    if nullable  == "N":
        dtype += " NOT NULL"
    return dtype

def main():
    print("Connecting to Origin User...")
    conn_dev = oracledb.connect(**ORIGIN_USER)

    print("Connecting to Target User...")
    conn_dev = oracledb.connect(**TARGET_USER)

    ORIGIN_schema = get_columns(conn_dev, ORIGIN_USER["user"])
    TARGET_schema = get_columns(conn_dev, TARGET_USER["user"])

    ddls = []

    for table in ORIGIN_schema:
        if table not in TARGET_schema:
            continue

        origin_cols = ORIGIN_schema[table]
        target_cols = TARGET_schema[table]

        missing = set(origin_cols.keys()) - set(target_cols.keys())

        for col in missing:
            definition = build_definition(origin_cols[col])
            ddl = f"ALTER TABLE {table} ADD ({col} {definition});"
            ddls.append(ddl)
            print(f"[+] {table}.{col} missing")

        if ddls:
            with open(OUTPUT_FILE, "w") as f:
                f.write(f"-- Generated at {datetime.now()}\n\n")
                for ddl in ddls:
                    f.write(ddl + "\n")
            print(f"\nDDL saved to {OUTPUT_FILE}")
        else:

        print("No differences found. Target schema is in sync with Origin.")

        conn_target.close()
        conn_origin.close()

        if __name__ == "__main__":
            main()
            
