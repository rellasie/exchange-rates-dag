"""DAG for fetching exchange rates and calculating deviations."""

import csv
import json
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
import telebot
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

DEFAULT_ARGS = {
    "owner": "Airflow",
    "start_date": datetime(2024, 9, 1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}


def get_exchange_rates(**context):
    """Fetch exchange rates from API and save to JSON file."""
    api_key = os.environ.get('EXCHANGE_RATES_API_KEY')
    execution_date = context['ds']
    url = (
        f"https://api.apilayer.com/exchangerates_data/{execution_date}"
        f"?base=USD"
    )
    headers = {"apikey": api_key}
    response = requests.get(url, headers=headers)
    data = response.json()

    file_path = f'/opt/airflow/data/exchange_rates_{execution_date}.json'
    with open(file_path, 'w') as outfile:
        json.dump(data, outfile)


def create_index_and_upsert_data(**context):
    """Create unique index and upsert data into PostgreSQL."""
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT 1 FROM pg_indexes
            WHERE indexname = 'exchange_rates_unique_idx';
        """)
        index_exists = cursor.fetchone()

        if not index_exists:
            # Remove duplicates
            cursor.execute("""
                DELETE FROM exchange_rates a USING (
                    SELECT MIN(ctid) as ctid, base, date, symbol
                    FROM exchange_rates
                    GROUP BY base, date, symbol HAVING COUNT(*) > 1
                ) b
                WHERE a.base = b.base AND a.date = b.date
                AND a.symbol = b.symbol
                AND a.ctid <> b.ctid;
            """)
            print(f"Removed {cursor.rowcount} duplicate rows.")

            cursor.execute("""
                CREATE UNIQUE INDEX exchange_rates_unique_idx
                ON exchange_rates (base, date, symbol);
            """)
            print("Unique index created.")
        else:
            print("Unique index already exists.")

        execution_date = context['ds']
        file_path = f'/opt/airflow/data/exchange_rates_{execution_date}.json'
        with open(file_path, 'r') as infile:
            data = json.load(infile)

        if 'rates' not in data:
            print(f"Error: 'rates' key not found in JSON data. Data: {data}")
            return

        df = pd.DataFrame(data['rates'].items(), columns=['symbol', 'rate'])

        if 'date' not in data:
            print("Warning: 'date' key not found. Using execution date.")
            df['date'] = execution_date
        else:
            df['date'] = data['date']

        if 'base' not in data:
            print("Warning: 'base' key not found. Using 'USD' as default.")
            df['base'] = 'USD'
        else:
            df['base'] = data['base']

        upsert_query = """
            INSERT INTO exchange_rates (base, date, symbol, rate)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (base, date, symbol) DO UPDATE SET rate = EXCLUDED.rate;
        """

        cursor.executemany(
            upsert_query,
            df[['base', 'date', 'symbol', 'rate']].values.tolist()
        )
        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"Error during index creation or upsert: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()


def calculate_deviations(**context):
    """Calculate max deviation and save results to CSV."""
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    execution_date = context['ds']
    start_date = context['dag'].default_args['start_date'].strftime('%Y-%m-%d')

    # Fetch data from start_date to execution_date
    query = """
    SELECT * FROM exchange_rates
    WHERE date BETWEEN %s::DATE AND %s::DATE
    ORDER BY date DESC, symbol
    """
    df = pd.read_sql(
        query,
        pg_hook.get_sqlalchemy_engine(),
        params=(start_date, execution_date)
    )

    if df.empty:
        print(f"No data found between {start_date} and {execution_date}")
        result = {
            'run_date': execution_date,
            'max_deviation_date': None,
            'symbol': None,
            'deviation': None
        }
    else:
        latest_date = df['date'].max()
        print(f"Latest date in data: {latest_date}")

        latest_rates = df[df['date'] == latest_date].set_index('symbol')['rate']

        deviations = []
        for symbol in latest_rates.index:
            symbol_data = df[df['symbol'] == symbol]
            if len(symbol_data) > 1:  # Ensure we have data to compare
                symbol_latest_rate = latest_rates[symbol]
                symbol_data['deviation'] = abs(
                    symbol_data['rate'] - symbol_latest_rate
                ) / symbol_latest_rate
                max_deviation_row = symbol_data.loc[
                    symbol_data['deviation'].idxmax()
                ]
                deviations.append({
                    'symbol': symbol,
                    'max_deviation_date': max_deviation_row['date'],
                    'deviation': max_deviation_row['deviation']
                })

        if deviations:
            max_deviation = max(deviations, key=lambda x: x['deviation'])
            result = {
                'run_date': execution_date,
                'max_deviation_date': max_deviation['max_deviation_date'],
                'symbol': max_deviation['symbol'],
                'deviation': max_deviation['deviation']
            }
        else:
            print("No deviations calculated. Check if there's enough data.")
            result = {
                'run_date': execution_date,
                'max_deviation_date': None,
                'symbol': None,
                'deviation': None
            }

    file_path = f'/opt/airflow/data/{execution_date}_max_deviation.csv'
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=['run_date', 'max_deviation_date', 'symbol', 'deviation']
        )
        writer.writeheader()
        writer.writerow(result)

    print(f"Max deviation calculated: {result}")


def send_telegram_message(**context):
    """Send Telegram message with max deviation report."""
    bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHAT_ID')

    bot = telebot.TeleBot(bot_token)

    execution_date = context['ds']
    csv_file_path = f'/opt/airflow/data/{execution_date}_max_deviation.csv'

    try:
        df = pd.read_csv(csv_file_path)
        print("CSV content:", df)

        message = "Max deviation report:\n\n"
        for _, row in df.iterrows():
            message += (
                f"Run Date: {row['run_date']}\n"
                f"Max Deviation Date: {row['max_deviation_date']}\n"
                f"Symbol: {row['symbol']}\n"
                f"Deviation: {row['deviation']:.4f}\n\n"
            )

        bot.send_message(chat_id=chat_id, text=message)
        print("Message sent successfully.")

        with open(csv_file_path, 'rb') as csv_file:
            bot.send_document(chat_id, csv_file, caption="Max deviation report CSV")
        print("CSV file sent successfully.")

    except Exception as e:
        print(f"Error in send_telegram_message: {str(e)}")
        raise


with DAG(
    dag_id="exchange-rates-dag",
    schedule_interval="0 1 * * *",
    default_args=DEFAULT_ARGS,
    catchup=True
) as dag:

    task1 = PythonOperator(
        task_id="get_exchange_rates",
        python_callable=get_exchange_rates,
        provide_context=True
    )

    task2 = PostgresOperator(
        task_id="create_postgres_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS exchange_rates (
                date DATE,
                base VARCHAR(3),
                symbol VARCHAR(3),
                rate FLOAT,
                CONSTRAINT exchange_rates_pkey PRIMARY KEY (date, base, symbol)
            );
        """
    )

    task3 = PythonOperator(
        task_id="create_index_and_upsert_data",
        python_callable=create_index_and_upsert_data,
        provide_context=True
    )

    task4 = PythonOperator(
        task_id="calculate_deviations",
        python_callable=calculate_deviations,
        provide_context=True
    )

    task5 = PythonOperator(
        task_id="send_telegram_message",
        python_callable=send_telegram_message,
        provide_context=True
    )

    task1 >> task2 >> task3 >> task4 >> task5
