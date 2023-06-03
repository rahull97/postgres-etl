import psycopg2
from sql_queries import drop_table_queries, create_table_queries


def create_database():

    # connect to postgres database
    conn = psycopg2.connect(database="postgres", user="postgres", password="postgres", host="localhost", port=5432)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify db with UTF-8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf-8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    # connect to sparkify db
    conn = psycopg2.connect(database="sparkifydb", user="postgres", password="postgres", host="localhost", port=5432)
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()
    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()
