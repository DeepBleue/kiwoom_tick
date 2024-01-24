# db_operation.py
import socket
import json
import mysql.connector
from multiprocessing import Process, Queue
from datetime import datetime
from mysql.connector import Error

def create_database_if_not_exists() :
    database_name = "B" + datetime.now().strftime("%Y%m%d")
    try:
        # Connect to the MySQL server
        conn = mysql.connector.connect(
                    host='127.0.0.1',
                    user='root',
                    password='93150lbm!!')
        
        cursor = conn.cursor()

        # Check if database exists
        cursor.execute(f"SHOW DATABASES LIKE '{database_name}'")
        result = cursor.fetchone()

        # If database does not exist, create it
        if not result:
            cursor.execute(f"CREATE DATABASE {database_name}")
            print(f"Database '{database_name}' created.")
        else:
            print(f"Database '{database_name}' already exists.")

    except Error as e:
        print(f"Error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def create_and_insert_query(code_dict):
    code = list(code_dict.keys())[0]
    data = code_dict[code]

    # Columns for CREATE TABLE statement, excluding '호가시간'
    columns = [f"`{k}` VARCHAR(255)" for k in data.keys()]

    # SQL for CREATE TABLE
    table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{code}` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            {', '.join(columns)}
        );
    """

    # Columns for INSERT INTO statement
    insert_columns = [f"`{k}`" for k in data.keys()]

    # Placeholders for values
    placeholders = ', '.join(['%s'] * len(data))

    # Values for INSERT INTO statement
    values = tuple(data.values())

    # SQL for INSERT INTO
    insert_sql = f"INSERT INTO `{code}` ({', '.join(insert_columns)}) VALUES ({placeholders})"

    return table_sql, insert_sql, values


def pro1(batch_data):
    database_name = "B" + datetime.now().strftime("%Y%m%d")
    conn = mysql.connector.connect(
        host='127.0.0.1',
        user='root',
        password='93150lbm!!',
        database=database_name
    )
    cursor = conn.cursor()
    print(f"Receive {len(batch_data)} data...")
    for code_dict in batch_data:
        table_sql, insert_sql, values = create_and_insert_query(code_dict)
        cursor.execute(table_sql)
        cursor.execute(insert_sql, values)
    conn.commit()
    conn.close()

def worker_process(queue):
    while True:
        batch_data = queue.get()
        if batch_data is None:
            break
        pro1(batch_data)
        
def server_process():
    queue = Queue()
    num_workers = 8
    workers = [Process(target=worker_process, args=(queue,)) for _ in range(num_workers)]
    for w in workers:
        w.start()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('localhost', 23456))
        s.listen()
        while True:
            conn, addr = s.accept()
            with conn:
                data_parts = []
                while True:
                    part = conn.recv(4096)  # Increased buffer size
                    if not part:
                        break
                    data_parts.append(part)
                data = b''.join(data_parts)
                if data:
                    batch_data = json.loads(data.decode('utf-8'))
                    queue.put(batch_data)

    # for _ in range(num_workers):
    #     queue.put(None)
    # for w in workers:
    #     w.join()

        
        
if __name__ == '__main__':
    
    create_database_if_not_exists()
    server_process()