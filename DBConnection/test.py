from sqlalchemy import create_engine, text

# ======================================================
# CONNECTION STRING
# ======================================================

DATABASE_URL = DB_URL

# ======================================================
# TEST CONNECTION
# ======================================================

try:
    print("\nConnecting to Neon...\n")

    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True
    )

    conn = engine.connect()

    result = conn.execute(
        text("SELECT NOW();")
    )

    for row in result:
        print("SUCCESS: Connected to Neon")
        print("DB TIME:", row)

    conn.close()

except Exception as e:
    print("\nCONNECTION FAILED\n")
    print(e)