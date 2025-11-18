# Investbot: Data Pipeline for S&P 500 Stock Monitoring

## 🎯 Project Overview
The Investbot is a Data Engineering platform designed to build a clean and complete time-series database of historical prices for all companies in the **S&P 500 index**.

The core focus of this phase is a robust **ETL (Extract, Transform, Load)** pipeline, capable of:
1.  Staying updated with daily changes to the index composition.
2.  Correcting the **survivorship bias** by reconstructing the accurate historical index timeline.

This pipeline serves as the foundation for future data analysis and an Automated Trading Bot.

## ✨ Engineering Highlights

| Feature | Technical Implementation | Engineering Justification |
| :--- | :--- | :--- |
| **Orchestration** | Apache Airflow (Daily DAG) | Manages dependencies and automates the end-to-end pipeline execution. |
| **Transformation** | **Polars** (replacing Pandas) | Utilizes Rust-based multi-threading for intensive *join* and *group-by* operations, optimizing performance and memory usage, especially for the historical timeline creation. |
| **Data Integrity** | **Historical Timeline Logic** | Captures the historical composition of the S&P 500 to prevent backfilling prices for periods when a stock was not part of the index (Survivorship Bias Correction). |
| **Data Sources** | Wikipedia (Constituents) and YFinance (Historical Prices) | Implements HTTP header handling and migrated from rate-limited APIs to the YFinance library for reliable sourcing. |
| **Persistence** | PostgreSQL (Containerized via Docker) | Migrated from SQLite to an industry-standard production environment for superior concurrency and data integrity. |

## 🛠️ Tech Stack

* **Language:** Python 3.11+
* **Orchestration:** Apache Airflow
* **Data Processing:** Polars, Pandas
* **Web Scraping:** BeautifulSoup
* **Database:** PostgreSQL (Containerized)
* **Containerization:** Docker & Docker Compose

## 🚀 Getting Started

The project uses Docker Compose to orchestrate PostgreSQL and Airflow in `Standalone` mode for lightweight local development.

### Prerequisites
* Python 3.11+
* Docker Desktop

### Installation

1.  **Clone the repository:**
    ```sh
    git clone [https://github.com/flanfranchi1/investbot.git](https://github.com/flanfranchi1/investbot.git)
    cd investbot
    ```

2.  **Create the environment file** (`.env`) and add your PostgreSQL credentials:
    ```env
    # .env file
    PG_USER='your_username'
    PG_PASSWORD='your_password'
    PG_DB='your_db_name'
    ```

3.  **Start the containers:**
    ```sh
    docker compose up -d
    ```

4.  **Access Airflow:**
    * **UI:** `http://localhost:8080`
    * **User/Pass:** Check your configuration for credentials.

## 📈 Data Flow Diagram (DFD)

The diagram below illustrates the flow of data and dependencies within the Airflow DAG (`investbot-main-dag`).

**Note on Accessibility:** This diagram is rendered using Mermaid, which uses a text-based syntax, ensuring that the entire workflow structure is readable by screen readers.

```mermaid
graph LR
    subgraph Orchestration: Airflow
        A[DAG: investbot-main-dag]
        A --> B{_database_setup}
        B --> C[Fetch S&P 500: _sp500_data]
        C --> D(Data Integrity: _missing_date_ranges)
        D --> E[Fetch Prices: _fettch_data]
        E --> F[Load Data: _load_fetched_data]
    end

    subgraph Sources and Destinations
        W(Web: Wikipedia)
        Y(API: YFinance)
        G[/DB: PostgreSQL Metastore/]
        P[/DB: PostgreSQL stock_prices/]
    end

    W --> C
    Y --> E

    C --> G: Constituents/Changes
    D -. Reads .- G: Timeline
    E --> F
    F --> P
    
    style A fill:#f9f,stroke:#333
    style D fill:#ddf,stroke:#333