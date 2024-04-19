# fetch_crypto_prices.py

import requests
import mysql.connector
from datetime import datetime
from crypto_config import CRYPTO_CONFIG
from db_conn import DB_CONFIG
import traceback


# Function to fetch cryptocurrency prices from the API
def fetch_prices():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": ",".join(CRYPTO_CONFIG.values()),  # Fetch prices for all configured cryptocurrencies
        "vs_currencies": "usd,cad",
        "include_24hr_change": "true",
        "include_last_updated_at": "true"
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an error for any HTTP errors
        prices_data = response.json()
        return prices_data
    except requests.RequestException as e:
        print("Error fetching crypto prices:", e)
        return None

# Function to save cryptocurrency prices to the database
def save_prices(prices_data):
    if prices_data:
        conn = mysql.connector.connect(host=DB_CONFIG['host'], 
                                    user=DB_CONFIG['user'], 
                                    password=DB_CONFIG['password'], 
                                    port=DB_CONFIG['port'],
                                    database=DB_CONFIG['database'])
        cursor = conn.cursor()

        try:
            for short_name in CRYPTO_CONFIG:
                currency = CRYPTO_CONFIG[short_name]
                currency_price_data = prices_data.get(currency)
                if not currency_price_data:
                    continue
                
                cursor.execute("SELECT id FROM cryptocurrency WHERE short_name = %s", (short_name,))
                crypto_row = cursor.fetchone()
                if not crypto_row:
                    continue
                crypto_id = crypto_row[0]

                # Insert prices in price table
                last_updated_unix_timestamp = currency_price_data['last_updated_at']
                utc_datetime = datetime.utcfromtimestamp(last_updated_unix_timestamp)
                cursor.execute("""
                    INSERT INTO price (crypto_id, price_usd, price_cad, last_updated_at, last_updated_at_datetime)
                    VALUES (%s, %s, %s, %s, %s)
                """, (crypto_id, currency_price_data['usd'], currency_price_data['cad'], currency_price_data['last_updated_at'],
                      utc_datetime))
                # Insert 24h change into the price_change table
                cursor.execute("""
                    INSERT INTO price_change (crypto_id, change_24h, last_updated_at)
                    VALUES (%s, %s, %s)
                """, (crypto_id, currency_price_data['usd_24h_change'], datetime.utcfromtimestamp(currency_price_data['last_updated_at'])))
                
                conn.commit()

            print("Prices saved successfully.")

        except mysql.connector.Error as e:
            conn.rollback()
            print(traceback.format_exc())
            print("Error saving prices:", e)
        finally:
            print()
            cursor.close()
            conn.close()

# Main function
def main():
    prices_data = fetch_prices()
    if prices_data:
        save_prices(prices_data)

if __name__ == "__main__":
    main()
