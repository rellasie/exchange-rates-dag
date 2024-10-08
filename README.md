# Exchange Rates DAG

# DAG structure

<a href="https://ibb.co/WftqB2g"><img src="https://i.ibb.co/jDRB3rg/mynh24-dag.png" alt="mynh24-dag" border="0"></a>

The DAG consists of five main tasks:

1. `get_exchange_rates`: Fetches exchange rate data from an API.
2. `create_postgres_table`: Creates or ensures the existence of the required database table.
3. `create_index_and_upsert_data`: Creates a unique index and upserts the fetched data into the database.
4. `calculate_deviations`: Analyzes the data to find maximum deviations in exchange rates.
5. `send_telegram_message`: Sends a report of the findings via Telegram.

# Overview
This DAG fetches daily exchange rates, processes them, and sends a report via Telegram. It demonstrates ETL processes, data analysis, and automated reporting using Apache Airflow.


## Requirements
- Apache Airflow
- PostgreSQL
- Python libraries: requests, pandas, telebot

## Configuration
### Environment Variables
Set the following environment variables in your Docker Compose file:
- `EXCHANGE_RATES_API_KEY`: Your API key for the exchange rates service.
- `TELEGRAM_BOT_TOKEN`: Your Telegram bot token.
- `TELEGRAM_CHAT_ID`: The chat ID where the bot should send messages.

### Airflow Connections
Set up a PostgreSQL connection in Airflow with the ID `postgres_default`.

## DAG Details
- Schedule: Daily at 1:00 AM
- Start Date: September 1, 2024
- Catchup: Enabled

## Task Descriptions

### 1. get_exchange_rates
Fetches exchange rate data from an API and saves it as a JSON file.

### 2. create_postgres_table
Creates the `exchange_rates` table if it doesn't exist.

### 3. create_index_and_upsert_data
Creates a unique index on the `exchange_rates` table and upserts the fetched data.

### 4. calculate_deviations
Calculates the maximum deviation in exchange rates and saves the result to a CSV file.

### 5. send_telegram_message
Reads the CSV file with maximum deviations and sends a formatted report via Telegram.

## Usage
1. Ensure all environment variables are set in the Docker Compose file.
2. Place the DAG file in the Airflow DAGs folder.
3. Start the Airflow environment.
4. The DAG will run automatically according to the schedule, or you can trigger it manually from the Airflow UI.

## Output
- The DAG will populate the `exchange_rates` table in your PostgreSQL database.
- A daily report will be sent to the specified Telegram chat, including:
  - A formatted message with the maximum deviation details.
  - A CSV file attachment with the full report.

## Troubleshooting
- Ensure all required libraries are installed in your Airflow environment.
- Check Airflow logs for any error messages if the DAG fails.
- Verify that the Telegram bot has permission to send messages to the specified chat.



