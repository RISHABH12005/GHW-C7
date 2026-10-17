# Real-Time Stock Data Tracker

This project collects live stock market updates and saves them so you can explore how prices change over time.

It was created for the **Global Hack Week Open Source challenge** using Twelve Data and Tiger Cloud.

## What does it do?

- Receives stock market updates as they arrive.
- Saves price, volume, and other market information.
- Handles repeated updates without needlessly saving the same entry twice.
- Includes example queries for reviewing recent updates and summarizing price changes over time.

## How it works

1. Twelve Data provides market updates.
2. The project collects those updates.
3. The data is saved in a Tiger Cloud database.
4. You can run the included SQL examples to explore recent prices and summaries.

## What you need

Before running the project, you will need:

- Python 3.10 or later.
- A [Twelve Data](https://twelvedata.com/) account and API key with access to the market data you want.
- A [Tiger Cloud](https://console.cloud.timescale.com/) database.
- The database connection details.

## Set up and run

1. Download or clone this repository.
2. Create a Python virtual environment and install the packages:

   ```bash
   python -m venv .venv
   ```

   Activate it:

   - **Windows PowerShell:** `.venv\Scripts\Activate.ps1`
   - **macOS/Linux:** `source .venv/bin/activate`

   Then install the packages:

   ```bash
   pip install -r requirements.txt
   ```

3. Copy `.env.example` to a new file named `.env`.
4. Add your Twelve Data API key and Tiger Cloud database connection details to `.env`. You can also change the stock symbols the project follows.
5. Prepare the database:

   ```bash
   python setup_db.py
   ```

6. Start collecting updates:

   ```bash
   python ingest.py
   ```

Stop the program with **Ctrl+C**.

**Important:** You must provide your own API key and database. Market updates depend on your data plan and the availability of market data. Never upload your `.env` file or share your keys and database password.

## Explore the data

Open [sql/analytics.sql](sql/analytics.sql) in your Tiger Cloud SQL editor to try the included examples. They show recent updates, summaries for each stock, and a minute-by-minute view of price activity.

## Project files

- `ingest.py` — collects and saves market updates.
- `setup_db.py` — prepares the database.
- `sql/analytics.sql` — example queries for exploring the data.
- `.env.example` — shows which settings you need to provide.

## Learn more

- [Twelve Data](https://twelvedata.com/)
- [Tiger Cloud](https://console.cloud.timescale.com/)
- [Tiger Data real-time financial data tutorial](https://www.tigerdata.com/docs/tutorials/financial-ingest-real-time/)
- [Global Hack Week](https://www.mlh.com/events/global-hack-week-open-source-90/challenges)

This project is for learning and demonstration. It is not financial advice.
