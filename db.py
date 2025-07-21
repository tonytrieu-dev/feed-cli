import psycopg2

def get_db_connection():
    # This code is from your project plan 
    return psycopg2.connect(
        dbname="news",
        user="postgres",
        password="secret",
        host="127.0.0.1",
        port=5433
    )

if __name__ == '__main__':
    try:
        conn = get_db_connection()
        print("Connection successful with psycopg2!")
        conn.close()
    except Exception as e:
        print(f"Connection failed: {e}")