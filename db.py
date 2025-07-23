import psycopg2

def get_db_connection():
    return psycopg2.connect(
        dbname="news",
        user="postgres",
        password="secret",
        host="127.0.0.1",
        port=5433
    )

if __name__ == '__main__':
    try:
        connection = get_db_connection()
        print("Connection successful with psycopg2!")
        connection.close()
    except Exception as e:
        print(f"Connection failed: {e}")