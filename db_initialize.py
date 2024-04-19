# crypto_price_fetcher/db_initialize.py

import mysql.connector
from mysql.connector import Error

DB_CONFIG = {
    'database': 'crypto_watchdog',
    'user': 'cryptowddbusername',
    'password': 'Ethereum@2024#$',
    'host': 'cwddatabase.mysql.database.azure.com',
    'port': '3306'
}

CRYPTO_CONFIG = {
    "btc": "bitcoin",
    "xrp": "ripple",
    "eth": "ethereum",
    "doge": "dogecoin"
}

# Function to check if the database exists
def check_database_exists(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("SHOW DATABASES LIKE %s", (DB_CONFIG['database'],))
        result = cursor.fetchone()
        if result:
            return True
        else:
            return False
    except Error as e:
        print("Error checking database existence:", e)
        return False
    finally:
        cursor.close()

# Function to create tables if they don't exist
def create_tables():
    conn = mysql.connector.connect(host=DB_CONFIG['host'], 
                                   user=DB_CONFIG['user'], 
                                   password=DB_CONFIG['password'], 
                                   port=DB_CONFIG['port'])

    if not check_database_exists(conn):
            print(f"Database '{DB_CONFIG['database']}' does not exist. Creating...")
            create_database(conn)

    # Select the database
    conn.database = DB_CONFIG['database']
    cursor = conn.cursor()  

    # Create cryptocurrency table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cryptocurrency (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            short_name VARCHAR(20) NOT NULL,
            image_url VARCHAR(255),
            description TEXT
        )
    """)

    # Create price table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS price (
        id INT AUTO_INCREMENT PRIMARY KEY,
        crypto_id INT NOT NULL,
        price_usd DECIMAL(20, 2),
        price_cad DECIMAL(20, 2),
        last_updated_at BIGINT, 
        last_updated_at_datetime TIMESTAMP, 
        FOREIGN KEY (crypto_id) REFERENCES cryptocurrency(id)
    )
""")

    # Create price_change table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS price_change (
            id INT AUTO_INCREMENT PRIMARY KEY,
            crypto_id INT NOT NULL,
            change_24h DECIMAL(10, 2),
            last_updated_at TIMESTAMP,
            FOREIGN KEY (crypto_id) REFERENCES cryptocurrency(id)
        )
    """)

    conn.commit()
    cursor.close()
    conn.close()

    print("Tables created successfully.")

def insert_initial_data():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        # Check if BTC already exists
        cursor.execute("SELECT id FROM cryptocurrency WHERE short_name = 'doge'")
        btc_exists = cursor.fetchone()

        if not btc_exists:
            print("Adding BTC in the database ...")
            # Insert Bitcoin data
            cursor.execute("""
                INSERT INTO cryptocurrency (name, short_name, image_url, description)
                VALUES (%s, %s, %s, %s)
            """, ("Dogecoin", "doge", "https://assets.coingecko.com/coins/images/1/large/dogecoin.png", "Dogecoin is a digital currency which operates free of any central control or the oversight of banks or governments."))

        conn.commit()
        print("Initial data inserted successfully.")
    except mysql.connector.Error as e:
        conn.rollback()
        print("Error inserting initial data:", e)
    finally:
        cursor.close()
        conn.close()


# Function to create the database if it doesn't exist
def create_database(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS %s" % DB_CONFIG['database'])
        print(f"Database '{DB_CONFIG['database']}' created successfully.")
    except Error as e:
        print("Error creating database:", e)
    finally:
        cursor.close()
        
# Main function
def main():
    create_tables()
    insert_initial_data()

if __name__ == "__main__":
    main()
