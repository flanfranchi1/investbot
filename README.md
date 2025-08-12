# Real-Time S&P 500 Stock Monitoring and Automated Trading Bot

## About The Project

This project is a platform designed to monitor real-time asset prices for all companies in the **S&P 500 index**. It will execute trading orders based on predefined policies to automatically manage a portfolio. A key feature is its ability to dynamically update its target list of stocks as the S&P 500 composition changes.

The development is structured in an agile, sprint-based approach, focusing first on building a solid data engineering pipeline (ETL) that will serve as the foundation for more complex trading logic.

### Key Features
* **Dynamic Ticker Sourcing:** Automatically fetches the current list of S&P 500 companies.
* **Data Ingestion:** Fetches stock market data through a dedicated, abstracted data source layer.
* **Data Storage:** Persists time-series data in a reliable database for analysis and backtesting.
* **Trading Logic:** (Future) Implements customizable strategies to trigger buy/sell orders.
* **Execution:** (Future) Integrates with brokerage APIs (like Interactive Brokers) to manage a live portfolio.

## Tech Stack

This project leverages the following technologies:

* **Language:** Python 3
* **Web Scraping:** BeautifulSoup, Pandas
* **Database ORM:** SQLAlchemy
* **Initial Database:** SQLite
* **Initial Data Source:** [Alpha Vantage API](https://www.alphavantage.co/)

## Getting Started

To get a local copy up and running, follow these simple steps.

### Prerequisites

* Python 3.10+
* An API Key from [Alpha Vantage](https://www.alphavantage.co/support/#api-key)

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
4.  Create a `.env` file in the root directory and add your API key
    ```env
    ALPHA_VANTAGE_API_KEY='YOUR_API_KEY'
    ```

## Usage

Run the main ETL script to fetch and store the latest stock data:

```sh
python main_etl.py