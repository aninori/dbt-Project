### 🛒 Walmart Retail Data Warehouse Project (SCD1 & SCD2 Implementation using dbt + Snowflake + AWS)

**Objective:**
Built a full-stack modern data pipeline to transform Walmart sales and inventory data using **dbt**, **Snowflake**, and **AWS S3**, enabling SCD1 for dimension tables and SCD2 for historical fact tracking.

---

**Key Features & Technologies:**

* **Source Data**: Ingested `department.csv`, `stores.csv`, and `fact.csv` from AWS S3 using Snowpipe + external stages
* **Pre-Hook Automation**: Wrote a custom dbt macro to dynamically reload files from S3 into Snowflake (`copy_into_snowflake`)
* **Bronze Layer**: Loaded raw data into `BRONZE.DEPARTMENT`, `BRONZE.STORES`, `BRONZE.FACT`
* **Silver Layer**: Created cleaned dimension and fact staging models

  * `walmart_date_dim`: SCD1 incremental model with surrogate key
  * `walmart_store_dim`: Composite PK (Store\_ID + Dept\_ID) dimension with overwrite logic
  * `walmart_fact_table_source`: Joined cleaned fact table used for SCD2 snapshotting
* **Snapshots (SCD2)**: Built `walmart_fact_snapshot.sql` to track historical changes (weekly sales, markdowns, CPI, etc.)
* **Gold Layer**: Finalized `walmart_fact_view` as versioned SCD2 fact view, including `vrsn_start_date`, `vrsn_end_date`, and metrics
* **dbt Cloud**: Managed environments, job runs, and auto-refresh of gold layer views

---

**Tools & Skills Demonstrated:**

* **dbt (Cloud)**: Modular modeling, snapshots, incremental loads, YAML configs
* **Snowflake**: Warehousing, staging tables, SCD1/2 logic, file formats
* **AWS S3**: Data lake source, IAM role for Snowflake integration
* **SQL**: Complex joins, surrogate key generation, incremental merge strategies
* **Data Modeling**: Layered (Bronze → Silver → Gold), snapshot versioning, slowly changing dimensions
* **Automation**: Used macros + `pre_hook` to automate S3 → Snowflake ingestion on each run

---

**Example GitHub Folder Structure:**

```
├── models/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── snapshots/
│   └── walmart_fact_snapshot.sql
├── macros/
│   └── copy_into_snowflake.sql
├── dbt_project.yml
└── README.md
```


