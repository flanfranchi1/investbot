## 1. Phase 1: Initial Architecture - Proof of Concept

The initial phase focused on establishing a baseline ETL pipeline to validate the core concept: sourcing S&P 500 data and persisting it locally.



* **Data Sourcing (Commit 6828f19...)**:
    * **Constituents List**: pandas.read_html combined with BeautifulSoup was chosen for its simplicity in parsing the S&P 500 constituents table directly from Wikipedia.
    * **Price Data**: The **Alpha Vantage API** was the initial source for historical price data due to its free tier and structured API.
* **Persistence (Commit 6828f19...)**:
    * **Database**: **SQLite** was selected as the initial database.
    * **Justification**: Its file-based, serverless nature made it ideal for rapid prototyping and local development, requiring minimal setup. SQLAlchemy was used from the start to provide an abstraction layer over the database-specific SQL syntax.
* **Workflow Enhancement (Commit 1da570b...)**:
    * **Marimo Notebook**: A reactive notebook was integrated into the project.
    * **Justification**: To facilitate interactive data exploration, validation of transformations, and direct querying of the database during development, thereby accelerating the feedback loop.


## 2. Phase 2: Pivoting the Data Source

The limitations of the initial data source became a significant bottleneck, necessitating a fundamental change in the extraction layer.



* **Data Source Migration (Commit 398f365...)**:
    * **Decision**: Replaced the Alpha Vantage API with the **yfinance** library.
    * **Justification**: The free tier of the Alpha Vantage API imposed restrictive rate limits. yfinance provided more reliable and less constrained access to Yahoo Finance's historical data. This required creating a new data_sourcing.py module and refactoring the data fetching and processing logic in main.py to handle the multi-index DataFrame structure returned by yfinance.
* **Code Structuring (Commit f08c7a3...)**:
    * **Decision**: A central config.py file was introduced.
    * **Justification**: To decouple constants (URLs, file paths) from the application logic, improving maintainability and making the codebase easier to configure.


## 3. Phase 3: Advanced Data Transformation and Integrity

With a stable data source, the project's focus shifted to addressing the core data engineering challenge: ensuring the historical integrity and completeness of the time-series data.



* **Historical Timeline Logic (Commit c1a6642... & 216924a...)**:
    * **Decision**: The system was enhanced to track not only the current S&P 500 constituents but also historical changes (additions and removals). A new module, transformations.py, was created to house this complex logic.
    * **Justification**: To build a historically accurate dataset, it was necessary to know which tickers were part of the index at any given point in time. This prevents the pipeline from attempting to backfill data for periods where a stock was not an index component.
* **Technology Stack Upgrade: Adopting Polars (Commit 216924a...)**:
    * **Decision**: **Polars** was introduced and became the primary library for all complex data transformations, replacing Pandas for these tasks.
    * **Justification**: The process of creating the historical timeline (creating_sp500_index_timeline) involved large joins and group-by operations. Polars' multi-threaded, Rust-based backend offered significantly better performance and more efficient memory management for these computationally intensive tasks compared to Pandas.
* **Refactoring of the "Missing Dates" Logic (Commit 950176b... -> 2cd3257...)**:
    * **Initial Approach**: The first implementation relied on a complex SQL query with window functions (LEAD) to identify gaps in the stored price data.
    * **Decision**: This SQL-based logic was deprecated and fully reimplemented in Polars within the catch_missing_prices function.
    * **Justification**: Moving the logic from the database to the application layer using Polars resulted in a more cohesive, maintainable, and testable solution. It centralized the transformation logic in a single place (transformations.py) and leveraged the superior performance of Polars for this specific task.


## 4. Phase 4: Production-Ready Infrastructure and Pipeline Resilience

The final phase focused on evolving the project from a script-based process to a more robust, production-oriented system.



* **Infrastructure Upgrade (Commit 48765f9...)**:
    * **Decision**: The **SQLite** database was replaced with **PostgreSQL**, managed via **Docker and Docker Compose**.
    * **Justification**: This aligns the project with industry-standard production environments. PostgreSQL provides superior scalability, concurrency, and data integrity features. Docker ensures a consistent, reproducible environment for both development and future deployment.
* **Pipeline Automation and Error Handling (Commit c39da7e...)**:
    * **Decision**: The main execution block in main.py was wrapped in a while True loop, and the error handling was enhanced.
    * **Justification**: To enable the pipeline to run continuously and autonomously. The introduction of more specific try-except blocks (e.g., for IntegrityError) makes the pipeline more resilient to common issues like API instabilities or attempts to insert duplicate data, preventing entire runs from failing due to transient errors. The logic was also optimized to prioritize fetching larger missing data ranges first, making the backfilling process more efficient.