# Real-Time S&P 500 Stock Monitoring and Automated Trading Bot

## About The Project

This project is a platform designed to monitor real-time asset prices for all companies in the **S&P 500 index**. It will execute trading orders based on predefined policies to automatically manage a portfolio. A key feature is its ability to dynamically update its target list of stocks as the S&P 500 composition changes.

The development is structured in an agile, sprint-based approach, focusing first on building a solid data engineering pipeline (ETL) that will serve as the foundation for more complex trading logic.

Along developments strategical decisions will be recorded [here](./docs/STRATEGIC_DECISIONS.md) in case your interested in, check it out!

### Key Features
* **Dynamic Ticker Sourcing:** Automatically fetches the current list of S&P 500 companies.
* **Data Ingestion:** Fetches stock market data through a dedicated, abstracted data source layer.
* **Data Storage:** Persists time-series data in a reliable database for analysis and backtesting.
* **Trading Logic:** (Future) Implements customizable strategies to trigger buy/sell orders.
* **Execution:** (Future) Integrates with brokerage APIs (like Interactive Brokers) to manage a live portfolio.

## Tech Stack

This project leverages the following technologies:

* **Language:** Python 3
* **Web Scraping:** BeautifulSoup
**Data Manipulation:** Pandas, Polars and Marimo (reactive Notebook)
* **Database ORM:** SQLAlchemy
* **Database:** SQLite in early estage now using PostgreSQL
**Containerization:** Docker to run database and later for 'packagin' the application itself
* **Initial Data Source**: Switched to YFinance python's lib given [Alpha Vantage API](https://www.alphavantage.co/)'s free plan limitations 


## Getting Started

To get a local copy up and running, follow these simple steps.

### Prerequisites

* Python 3.1.13+
**Docker Desktop 4.47.0+**

### Installation

1.  Clone the repo
    ```sh
    git clone [https://github.com/your_username/your_repository.git](https://github.com/your_username/your_repository.git)
    ```
2.  Create and activate a virtual environment
    ```sh
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```
3.  Install Python packages
    ```sh
    pip install -r requirements.txt
    ```
4. Start docker artfacts:
    ```sh
    docker compose up
    ```

5.  Create a `.env` file in the root directory and add postgres's credentials
    ```env
    PG_USER='your_username'
    PG_PASSWORD='your_password'
    ```

## Usage

Run the main ETL script to fetch and store the latest stock data:

```sh
python main_etl.py