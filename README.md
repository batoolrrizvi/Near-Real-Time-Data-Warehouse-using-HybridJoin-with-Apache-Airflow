# Near-Real-Time-Data-Warehouse-using-HybridJoin-with-Apache-Airflow

This is a project that focuses on data warehousing concepts such as dimensional modelling, OLAP, high velocity transactional data streams and proper pipeline orchestration. This data warehouse was specifically modeled to use transactional data for retail analytics to enable near-real-time analysis of shopping behaviour. 

## Data Modelling:
A star schema was used to model the data warehouse. The star schema represents aggregated data for specific business activities. Using the schema, one can create multiple aggregated data sources that will represent different aspects of business operations.

* **Fact Table:** `Sales` (Tracks individual transactions, revenue, and quantities)
* **Dimension Tables:**
    * `Customer`: Demographics (Age, Gender, Marital Status, Occupation, City Category)
    * `Product`: Product details, categories, and pricing
    * `Store`: Store identifiers and names
    * `Supplier`: Supplier details
    * `Date`: Granular time tracking (Day, Month, Year, Quarter, Weekend flags) for time-series analysis
 

## Join Algorithm:
HYBRIDJOIN is a stream-based join algorithm that was designed for scenarios like near-real-time data warehousing. 
HYBRIDJOIN has two aims:
  * (a) efficient access of disk-based relation R by loading only the useful part of R into memory
  * (b) dealing with bursty streams effectively.

**Algorithm Components**
* **Stream Buffer:** Temporarily holds incoming stream tuples to prevent data loss during bursty transactional periods.
* **Hash Table & Queue:** Maps stream tuples into memory slots while maintaining a doubly-linked list queue to track arrival order for fair processing.
* **Disk Buffer:** Leverages an indexed disk-based relation to load partitions dynamically based on the oldest keys in the queue, minimizing I/O overhead.
* **Multithreading:** Implements a Producer/Consumer pattern where one thread continuously pushes transactional data into the stream buffer, while a worker thread independently executes the join and database commits.

## Data Pipeline using Apache Airflow

The ETL process is fully automated via the `walmart_dw_pipeline` DAG, executing in four sequential phases:

1.  **`create_schema`**: Task that connects to the PostgreSQL instance, initializes the `WalmartDW` schema, sets up custom ENUM types, and builds the Star Schema with appropriate constraints.
2.  **`load_master_data`**: Extracts static dimensional data (`product` and `customer` CSVs) using Pandas and efficiently loads them into Postgres using batch `executemany` inserts with conflict resolution.
3.  **`run_hybrid_join`**: Runs the multithreaded Hybrid Join algorithm, caching dimension tables in memory, parsing dynamic dates on the fly, and streaming enriched transactional records into the `Sales` fact table using parameterized batch commits.
4.  **`run_analytical_queries`**: Executes a comprehensive suite of 20 advanced SQL queries designed for business intelligence.


