# Data Engineer Challenges

> An enterprise-grade, end-to-end data engineering platform built on **Databricks**, **Apache Spark (PySpark)**, **Delta Lake**, and **Unity Catalog** for ingesting, validating, modeling, orchestrating, and analyzing high-frequency IoT telemetry and facility operational events.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [System Architecture](#system-architecture)
- [Technology Stack](#technology-stack)
- [Repository Structure](#repository-structure)
- [Data Modeling & Schema Design](#data-modeling--schema-design)
  - [Entity-Relationship Model](#entity-relationship-model)
  - [Dimension Tables](#dimension-tables)
  - [Fact Tables](#fact-tables)
  - [Asset Relationship Graph](#asset-relationship-graph)
- [Data Pipeline & Medallion Architecture](#data-pipeline--medallion-architecture)
  - [Bronze Layer (Raw Ingestion)](#bronze-layer-raw-ingestion)
  - [Silver Layer (Validation & Cleaning)](#silver-layer-validation--cleaning)
  - [Gold Layer (Aggregations & Business KPIs)](#gold-layer-aggregations--business-kpis)
- [Data Quality & Quarantine Framework](#data-quality--quarantine-framework)
  - [Validation Rule Engine](#validation-rule-engine)
  - [Dead-Letter Queue / Quarantine Routing](#dead-letter-queue--quarantine-routing)
  - [Automated DQ Audit Report](#automated-dq-audit-report)
- [Multi-Asset Hierarchy & Graph Traversal](#multi-asset-hierarchy--graph-traversal)
  - [Recursive Site Hierarchy Rollup](#recursive-site-hierarchy-rollup)
  - [Failure Blast Radius (Downstream Impact)](#failure-blast-radius-downstream-impact)
  - [Orphan & Disconnected Asset Detection](#orphan--disconnected-asset-detection)
- [Advanced Analytical SQL Queries](#advanced-analytical-sql-queries)
- [Orchestration, Resilience & Monitoring](#orchestration-resilience--monitoring)
  - [Pipeline DAG Flow](#pipeline-dag-flow)
  - [Failure Handling & Circuit Breaking](#failure-handling--circuit-breaking)
  - [Retry Policy & Idempotency](#retry-policy--idempotency)
  - [Alerting & Notification Matrix](#alerting--notification-matrix)
- [Performance Optimization & Storage Strategy](#performance-optimization--storage-strategy)
- [Prerequisites & Environment Setup](#prerequisites--environment-setup)
- [Execution & Deployment Guide](#execution--deployment-guide)
- [Artifacts & Media Walkthrough](#artifacts--media-walkthrough)
- [Troubleshooting & Common Issues](#troubleshooting--common-issues)
- [Security & Data Governance](#security--data-governance)
- [Future Enhancements](#future-enhancements)
- [License & Credits](#license--credits)

---

## 🌟 Overview

Modern facilities and industrial campuses generate massive streams of time-series sensor telemetry (power consumption, temperature, humidity, pressure, vibration) and discrete operational logs (faults, warnings, operating mode transitions). Without a robust data platform, organizations struggle with dirty data, schema inconsistencies, unmonitored asset hierarchies, and latency in critical maintenance decisions.

This project delivers a production-ready **Industrial IoT Data Platform** designed to:
1. **Ingest and Harmonize Streams**: Ingest high-volume time-series telemetry and event logs into a structured Medallion Lakehouse.
2. **Enforce Automated Data Quality Gates**: Intercept dirty, malformed, or outlier data before it infects downstream models, routing corrupt records to a dedicated quarantine table.
3. **Model Complex Multi-Asset Hierarchies**: Model and traverse parent-child physical and functional relationships across campuses, buildings, equipment (chillers, pumps, AHUs), and sensors using recursive graph SQL queries.
4. **Deliver Curated Analytics & Anomaly Detection**: Aggregate multi-level business metrics (Asset → Building → Site) and perform statistical anomaly detection (rolling 7-day μ ± 2σ power surges).
5. **Ensure Enterprise Reliability**: Package workloads into resilient Databricks / Airflow DAGs with exponential backoff retries, SLA alerting, and Delta Lake Z-Ordering for lightning-fast querying.

---

## 🚀 Key Features

### 1. Robust Medallion Data Pipeline
- **Raw / Bronze**: Schema-inferred ingestion from Unity Catalog Volumes / Cloud Object Storage (`Iot telemetry.csv`, `Event.csv`, `Asset Metadata.csv`).
- **Silver Cleaning**: Deduplication on composite keys (`timestamp`, `asset_id`, `sensor_id`), strict timestamp parsing, null handling, and column sanitization.
- **Gold Aggregations**: 1-hour windowed rollups, asset utilization rates, and multi-tier operational summaries (`asset_metrics`, `building_metrics`, `site_metrics`).

### 2. Comprehensive Data Quality & Quarantine Engine
- 6-point automated data validation suite checking for nulls, duplicates, schema mismatches, physical boundary outliers, late-arriving records, and reporting gaps.
- Non-blocking dead-letter routing to `quarantine_telemetry` with exact reason codes (`NULL_MANDATORY_FIELD`, `SCHEMA_TIMESTAMP_INVALID`, `METRIC_OUTLIER_OUT_OF_BOUNDS`).
- Automatic generation of the `data_quality_summary_report` table tracking ingestion volume and pass-rate percentages.

### 3. Graph-Based Asset Hierarchy & Impact Analysis
- Recursive Common Table Expressions (CTEs) traversing directed acyclic asset graphs.
- **Blast Radius Analysis**: Traces cascading downstream impacts when an upstream equipment unit (e.g., Chiller-01) experiences a failure.
- **Topology Auditing**: Detects orphan sensors (no parent node) and disconnected equipment (isolated islands).

### 4. Advanced Analytical SQL Solutions
- Identification of Top 10 energy-consuming assets and site-level average daily consumption.
- Identification of repeat-fault equipment (>10 faults in 30 days) and silent assets (>24h silence).
- Hourly operational utilization percentage per building (`Running` vs total reading ratios).
- Rolling 7-day statistical anomaly detection for power consumption spikes.

### 5. Production Orchestration & Storage Optimization
- Delta Lake ACID transactions with idempotent partition overwrites (`mode("overwrite")`).
- Multi-dimensional Z-Ordering on `(asset_id, sensor_id)` and Liquid Clustering for query acceleration.
- Multi-tier alert dispatch (P1 Critical PagerDuty, P2 Warning Slack `#iot-pipeline-alerts`, P3 Info Daily Email Digest).

---

## 🏗️ System Architecture

The end-to-end data platform follows a modern Lakehouse architecture spanning edge ingestion, distributed batch/stream processing, curated medallion storage, graph topology, orchestration/monitoring, and consumption layers:

1. **Data Ingestion Layer**: Continuous time-series telemetry streams from IoT sensors (power, temperature, pressure, vibration), discrete equipment event logs (faults, warnings, alarms), and asset metadata ingested via MQTT/HTTPS, Apache Kafka, and cloud storage volumes (ADLS Gen2 / AWS S3).
2. **Bronze Layer (Raw Storage)**: Raw, immutable Delta tables (`raw_telemetry`, `raw_events`, `raw_metadata`) capturing data in its native structure.
3. **Silver Layer & Data Quality**: Dedicated validation pipelines for telemetry, events, and metadata:
   - Evaluates records against null checks, timestamp schemas, physical bounds, and deduplication rules.
   - Routes validated records to `silver_telemetry_clean`, `silver_events_clean`, and `silver_metadata_clean`.
   - Diverts malformed records to `quarantine_telemetry` (Dead-Letter Queue) with tagged rejection reasons.
   - Logs validation metrics to `data_quality_summary_report`.
   - Populates core dimensional tables (`dim_site`, `dim_building`, `dim_asset`, `dim_date`, `dim_time`).
4. **Gold Layer (Curated Facts & KPIs)**: Curated analytical fact tables (`fact_telemetry`, `fact_energy_hourly`, `fact_event`) joined with dimensions and aggregated into multi-tier operational summaries (`asset_metrics`, `building_metrics`, `site_metrics`).
5. **Hierarchy & Topology Engine**: Graph adjacency tables (`dim_asset_hierarchy`, `asset_relationship`) traversed via recursive SQL CTEs to compute failure blast radius (downstream impact cascade) and perform topology auditing (orphan and disconnected asset detection).
6. **Orchestration & Monitoring**: Supervised by Databricks Workflows / Apache Airflow DAGs with automatic retries, idempotency, circuit breakers (>10% invalid threshold), and multi-tier alert dispatch (P1 PagerDuty, P2 Slack `#iot-pipeline-alerts`, P3 Email digest).
7. **Serving & Consumption Layer**: Downstream delivery to Power BI / Databricks SQL / Snowflake for business intelligence, CMMS work orders for facility operations and maintenance, and MLflow / SageMaker for predictive maintenance and Remaining Useful Life (RUL) modeling.

---

## 🛠️ Technology Stack

| Category | Technology / Tool | Version / Spec | Purpose |
|---|---|---|---|
| **Data Platform** | Databricks Unified Analytics Platform | Azure / AWS Runtime 13.x+ | Cloud execution environment and managed compute |
| **Data Processing** | Apache Spark (PySpark) | 3.4+ | Distributed data ingestion, transformations, and windowed aggregations |
| **Storage & Format** | Delta Lake | 2.4+ / 3.0+ | ACID transactions, time travel, schema enforcement, Z-Ordering |
| **Metastore / Governance** | Unity Catalog | `dataworkspace.default.*` | Centralized data catalog, volumes, and role-based access control |
| **Data Modeling** | Dimensional Modeling (Star / Snowflake) | SQL DDL | Facts, dimensions, and graph relationship tables |
| **Query Engine** | Databricks SQL / Spark SQL | ANSI SQL standard | Recursive CTEs, window analytics, and business reporting |
| **Orchestration** | Databricks Workflows / Apache Airflow | DAG-based | Scheduled execution, task dependency management, circuit breaking |
| **Optimization** | Delta Z-Ordering & Liquid Clustering | `OPTIMIZE ... ZORDER BY` | Compaction and multi-column file clustering for query acceleration |
| **Data Storage** | Azure Data Lake Storage Gen2 / AWS S3 | Unity Catalog Volumes | Scalable object storage for raw and curated datasets |
| **Languages** | Python, SQL, Markdown | Python 3.10+, SQL | Pipeline logic, orchestration, analysis, and documentation |

---

## 📁 Repository Structure

```text
DE-task/
├── Build Data Quality Framework                 # Specifications for DQ validation rules & error handling
├── Build Data Quality.ipynb                     # Databricks notebook implementing 6-point DQ rule engine & reporting
├── Build a Data Pipeline                        # High-level architecture walkthrough of the 5-stage ETL pipeline
├── Data Architecture Design                     # Architecture document covering ingestion, medallion storage & serving
├── Data Architecture Design.png                 # Architecture diagram image
├── Data Engineer Presentation.pptx              # Executive presentation slide deck for the DE assignment
├── Data Modeling                                # DDL scripts for Star/Snowflake schemas, partitioning & indexing
├── ER.drawio.png                                # Entity-Relationship diagram illustrating fact-dimension topology
├── Multi-Asset Hierarchy &  Connectivity       # SQL queries for graph hierarchy, blast radius & orphan detection
├── Multi-Asset Hierarchy &  Connectivity.ipynb # Executed notebook containing recursive CTE queries and result sets
├── Orchestration & Scheduling                   # Production DAG specifications, failure policies, and retry strategies
├── Orchestration & Scheduling.ipynb             # Databricks notebook demonstrating end-to-end pipeline run & Z-Ordering
├── README.md                                    # Comprehensive project documentation
├── SQL Challenge                                # Production SQL queries for 6 advanced analytical use cases
├── Task-Explanation.mp4                         # Video presentation and technical walkthrough of the solution
└── transformation and validation.ipynb          # Spark ETL pipeline notebook for transformation and gold aggregation
```

---

## 📊 Data Modeling & Schema Design

### Entity-Relationship Model
The repository models facility operations using a combination of **Star / Snowflake Dimensional Modeling** for analytical rollups and an **Adjacency List Graph Model** for physical/functional equipment hierarchies.

*Reference diagram: [`ER.drawio.png`](./ER.drawio.png)*

### Dimension Tables

#### 1. Site Dimension (`dim_site`)
```sql
CREATE TABLE IF NOT EXISTS dataworkspace.default.dim_site (
    site_id STRING NOT NULL,
    site_name STRING,
    created_at TIMESTAMP
) USING DELTA;
```

#### 2. Building Dimension (`dim_building`)
```sql
CREATE TABLE IF NOT EXISTS dataworkspace.default.dim_building (
    building_id STRING NOT NULL,
    site_id STRING NOT NULL,
    building_name STRING
) USING DELTA;
```

#### 3. Asset Dimension (`dim_asset`)
```sql
CREATE TABLE IF NOT EXISTS dataworkspace.default.dim_asset (
    asset_id STRING NOT NULL,
    asset_name STRING,
    asset_type STRING,
    manufacturer STRING,
    installation_date DATE,
    site_id STRING,
    building_id STRING
) USING DELTA;
```

#### 4. Date & Time Dimensions (`dim_date`, `dim_time`)
```sql
CREATE TABLE IF NOT EXISTS dataworkspace.default.dim_date (
    date_key INT NOT NULL, -- Format: YYYYMMDD
    full_date DATE NOT NULL,
    day_of_week INT,
    day_name STRING,
    month INT,
    month_name STRING,
    quarter INT,
    year INT
) USING DELTA;

CREATE TABLE IF NOT EXISTS dataworkspace.default.dim_time (
    time_key INT NOT NULL, -- Format: HHMM
    hour INT NOT NULL,
    minute INT NOT NULL,
    time_of_day STRING
) USING DELTA;
```

### Fact Tables

#### 1. Telemetry Fact (`fact_telemetry`)
High-volume, granular sensor measurements partitioned by date for partition pruning.
```sql
CREATE TABLE IF NOT EXISTS dataworkspace.default.fact_telemetry (
    telemetry_id BIGINT GENERATED ALWAYS AS IDENTITY,
    timestamp TIMESTAMP NOT NULL,
    date_key INT NOT NULL,
    site_id STRING NOT NULL,
    building_id STRING NOT NULL,
    asset_id STRING NOT NULL,
    sensor_id STRING NOT NULL,
    temperature DOUBLE,
    humidity DOUBLE,
    pressure DOUBLE,
    vibration DOUBLE,
    power_consumption DOUBLE,
    operating_mode STRING
)
USING DELTA
PARTITIONED BY (date_key);
```

#### 2. Hourly Energy Fact (`fact_energy_hourly`)
Curated rollups pre-aggregated into 1-hour time buckets.
```sql
CREATE TABLE IF NOT EXISTS dataworkspace.default.fact_energy_hourly (
    energy_fact_id BIGINT GENERATED ALWAYS AS IDENTITY,
    date_key INT NOT NULL,
    hour_window TIMESTAMP NOT NULL,
    site_id STRING NOT NULL,
    building_id STRING NOT NULL,
    asset_id STRING NOT NULL,
    total_energy_kwh DOUBLE,
    avg_power_kw DOUBLE
)
USING DELTA
PARTITIONED BY (date_key);
```

#### 3. Event Fact (`fact_event`)
Discrete operational incidents, alarms, and maintenance events.
```sql
CREATE TABLE IF NOT EXISTS dataworkspace.default.fact_event (
    event_id STRING NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    date_key INT NOT NULL,
    asset_id STRING NOT NULL,
    event_type STRING,
    severity STRING,
    message STRING
)
USING DELTA
PARTITIONED BY (date_key);
```

### Asset Relationship Graph

To support recursive hierarchy resolution, relationships are stored in a normalized adjacency structure:

```sql
-- Master Node Catalog
CREATE TABLE IF NOT EXISTS dataworkspace.default.dim_asset_hierarchy (
    node_id STRING NOT NULL,
    node_name STRING NOT NULL,
    node_type STRING NOT NULL, -- SITE, BUILDING, EQUIPMENT, SENSOR
    site_id STRING NOT NULL,
    manufacturer STRING,
    installation_date DATE,
    created_at TIMESTAMP
) USING DELTA;

-- Directed Graph Edges
CREATE TABLE IF NOT EXISTS dataworkspace.default.asset_relationship (
    relationship_id BIGINT GENERATED ALWAYS AS IDENTITY,
    parent_id STRING NOT NULL,
    child_id STRING NOT NULL,
    relationship_type STRING NOT NULL, -- LOCATED_IN, HOUSES, FEEDS_TO, MONITORS
    is_active BOOLEAN
) USING DELTA;
```

---

## 🔄 Data Pipeline & Medallion Architecture

The ETL workflow is structured according to the **Medallion Architecture**, implemented across [`transformation and validation.ipynb`](./transformation%20and%20validation.ipynb) and [`Orchestration & Scheduling.ipynb`](./Orchestration%20&%20Scheduling.ipynb).

### 1. Bronze Layer (Raw Ingestion)
Raw files are read from Unity Catalog Volumes (`/Volumes/dataworkspace/default/data/`):
- `Iot telemetry.csv`: Continuous telemetry metrics.
- `Event.csv`: Equipment event and fault logs.
- `Asset Metadata.csv`: Physical attributes and parent-child linkages.

Column headers are normalized with snake_case sanitization (`clean_cols`) to strip special characters (`°C`, `%`, spaces, parentheses) that cause Delta metadata errors:
```python
def clean_cols(df):
    for c in df.columns:
        clean_name = re.sub(r'[ ,;{}()\n\t=%/°]', '_', c)
        clean_name = re.sub(r'_+', '_', clean_name).strip('_')
        df = df.withColumnRenamed(c, clean_name)
    return df
```

### 2. Silver Layer (Validation & Cleaning)
- **Deduplication**: Drops duplicate telemetry entries on composite key `[timestamp, asset_id, sensor_id]` and duplicate events on `[event_id]`.
- **Timestamp Normalization**: Casts ISO/string timestamp fields to structured Spark SQL `TimestampType` using `F.to_timestamp()`.
- **Null Filtering**: Enforces non-null invariants on critical foreign keys (`asset_id`, `site_id`, `building_id`) and telemetry metrics.

### 3. Gold Layer (Aggregations & Business KPIs)
Transforms validated silver records into three business aggregation tables:

```python
# 1. Hourly Rollups with 1-Hour Time Windows
hourly_curated = (
    clean_telemetry
    .withColumn("hour_window", F.date_trunc("hour", "parsed_timestamp"))
    .groupBy("site_id", "building_id", "asset_id", "hour_window")
    .agg(
        F.sum("power_consumption_kW").alias("hourly_energy_consumption_kWh"),
        F.avg("temperature_C").alias("avg_temperature_C"),
        F.avg("humidity").alias("avg_humidity_pct"),
        F.avg("pressure_hPa").alias("avg_pressure_hPa"),
        F.avg("vibration_mm_s").alias("avg_vibration")
    )
)

# 2. Asset Metrics (Energy + Lifetime Fault Join)
asset_metrics = (
    hourly_curated
    .groupBy("asset_id")
    .agg(
        F.sum("hourly_energy_consumption_kWh").alias("total_energy_kWh"),
        F.avg("avg_temperature_C").alias("overall_avg_temp_C"),
        F.avg("avg_humidity_pct").alias("overall_avg_humidity_pct")
    )
    .join(fault_statistics, on="asset_id", how="left")
    .fillna(0, subset=["total_fault_count"])
)

# 3. Building & Site Rollups
building_metrics = (
    hourly_curated
    .groupBy("site_id", "building_id")
    .agg(
        F.sum("hourly_energy_consumption_kWh").alias("total_building_energy_kWh"),
        F.avg("avg_temperature_C").alias("building_avg_temp_C"),
        F.avg("avg_humidity_pct").alias("building_avg_humidity_pct"),
        F.countDistinct("asset_id").alias("total_assets")
    )
)

site_metrics = (
    hourly_curated
    .groupBy("site_id")
    .agg(
        F.sum("hourly_energy_consumption_kWh").alias("total_site_energy_kWh"),
        F.avg("avg_temperature_C").alias("site_avg_temp_C"),
        F.avg("avg_humidity_pct").alias("site_avg_humidity_pct"),
        F.countDistinct("building_id").alias("total_buildings"),
        F.countDistinct("asset_id").alias("total_assets")
    )
)
```

Tables are persisted to Unity Catalog (`dataworkspace.default.*`) in overwrite mode with schema validation.

---

## 🛡️ Data Quality & Quarantine Framework

The platform implements a dedicated Data Quality framework (documented in [`Build Data Quality Framework`](./Build%20Data%20Quality%20Framework) and implemented in [`Build Data Quality.ipynb`](./Build%20Data%20Quality.ipynb)).

### Validation Rule Engine

| # | Validation Rule | Condition / Boundary | Action on Failure | Flag Column |
|---|---|---|---|---|
| **1** | **Mandatory Null Check** | `asset_id`, `site_id`, `building_id`, or parsed `timestamp` is NULL or empty string | Flag & Quarantine | `flag_null_mandatory` |
| **2** | **Schema / Format Violation** | Timestamp string failed parsing while raw timestamp was present | Flag & Quarantine | `flag_schema_violation` |
| **3** | **Sensor Physical Outliers** | Sensor metrics outside physical operating thresholds:<br>• `temperature_C`: [-20°C, 120°C]<br>• `humidity`: [0%, 100%]<br>• `pressure_hPa`: [700 hPa, 1300 hPa]<br>• `vibration_mm_s`: [0 mm/s, 50 mm/s]<br>• `power_consumption_kW`: ≥ 0 | Flag & Quarantine | `flag_outlier` |
| **4** | **Late-Arriving Telemetry** | Reading timestamp is > 7 days older than current dataset high watermark | Flag for Audit | `flag_late_arriving` |
| **5** | **Deduplication Gate** | Duplicate sensor reading on composite key `[timestamp, asset_id, sensor_id]` | Drop duplicate record | Deduplicated via `.dropDuplicates()` |
| **6** | **Reporting Gap Check** | Telemetry silence from an active registered asset in the last 24 hours | Surface in DQ report | Handled via analytical query |

### Dead-Letter Queue / Quarantine Routing

Clean records that satisfy all validation rules (`is_valid = True`) are loaded directly into `dataworkspace.default.silver_telemetry_clean` for downstream transformation and aggregation.

Records failing any validation rule (`is_valid = False`) are diverted to `dataworkspace.default.quarantine_telemetry` alongside concatenated rejection reason codes (`NULL_MANDATORY_FIELD`, `SCHEMA_TIMESTAMP_INVALID`, `METRIC_OUTLIER_OUT_OF_BOUNDS`). This dead-letter pattern isolates invalid telemetry without halting batch execution.

Clean records are routed directly to `dataworkspace.default.silver_telemetry_clean`, while malformed records are diverted into `dataworkspace.default.quarantine_telemetry` along with detailed rejection reason strings without failing the entire orchestration run.

### Automated DQ Audit Report

Every pipeline execution records audit metrics into `dataworkspace.default.data_quality_summary_report`:

| Quality Check Metric | Record Count | Pass Rate Percentage |
|---|---|---|
| **Total Records Ingested** | 8 | 100.0% |
| **Duplicate Records Dropped** | 0 | 0.0% |
| **Null / Missing Value Violations** | 0 | 0.0% |
| **Schema / Format Violations** | 0 | 0.0% |
| **Sensor Outlier Violations** | 0 | 0.0% |
| **Passed Quality Gate (Clean Data)** | 8 | 100.0% |
| **Quarantined Records** | 0 | 0.0% |

---

## 🌳 Multi-Asset Hierarchy & Graph Traversal

Facilities feature deeply nested, directed multi-asset topologies (e.g., Campus → Building → Chiller → AHUs → Sensors). For example:
- **Campus Root**: `Site-A`
  - **Building Node**: `Building-1` (`LOCATED_IN` Site-A)
    - **Primary Equipment**: `Chiller-01` (`HOUSES` in Building-1) and `Pump-01` (`HOUSES` in Building-1)
      - **Secondary Equipment**: `AHU-01` and `AHU-02` (`FEEDS_TO` from Chiller-01)
      - **Sensor Unit**: `Flow Sensor-01` (`MONITORS` Pump-01)
- **Unconnected / Irregular Nodes**:
  - `Orphan-Sensor-09`: Detached sensor with no parent link
  - `Isolated-Asset-99`: Disconnected spare motor with neither parent nor child connections

The queries in [`Multi-Asset Hierarchy &  Connectivity`](./Multi-Asset%20Hierarchy%20&%20%20Connectivity) solve key operational graph problems:

### 1. Recursive Site Hierarchy Rollup
Retrieves the complete downstream asset tree starting from a given root (`Site-A`) with tree depth:

```sql
WITH RECURSIVE site_hierarchy AS (
    SELECT child_id AS asset_id, 1 AS depth
    FROM dataworkspace.default.asset_relationship
    WHERE parent_id = 'Site-A'

    UNION ALL

    SELECT r.child_id, sh.depth + 1
    FROM dataworkspace.default.asset_relationship r
    JOIN site_hierarchy sh ON r.parent_id = sh.asset_id
)
SELECT DISTINCT h.node_id, h.node_name, h.node_type, sh.depth
FROM site_hierarchy sh
JOIN dataworkspace.default.dim_asset_hierarchy h ON sh.asset_id = h.node_id
ORDER BY sh.depth, h.node_type;
```

### 2. Failure Blast Radius (Downstream Impact)
When a critical asset (e.g., `Chiller-01`) fails or requires maintenance, this recursive query calculates all downstream dependent systems impacted:

```sql
WITH RECURSIVE downstream_impact AS (
    SELECT child_id AS impacted_asset, relationship_type, 1 AS depth
    FROM dataworkspace.default.asset_relationship
    WHERE parent_id = 'Chiller-01'

    UNION ALL

    SELECT r.child_id, r.relationship_type, di.depth + 1
    FROM dataworkspace.default.asset_relationship r
    JOIN downstream_impact di ON r.parent_id = di.impacted_asset
)
SELECT * FROM downstream_impact;
```

### 3. Orphan & Disconnected Asset Detection
- **Orphan Assets**: Nodes that have no incoming parent relationship (excluding root sites).
- **Disconnected Assets**: Isolated nodes with neither incoming nor outgoing relationships.

```sql
-- Identify Disconnected Assets
SELECT h.node_id, h.node_name, h.node_type
FROM dataworkspace.default.dim_asset_hierarchy h
LEFT JOIN dataworkspace.default.asset_relationship r_child ON h.node_id = r_child.parent_id
LEFT JOIN dataworkspace.default.asset_relationship r_parent ON h.node_id = r_parent.child_id
WHERE r_child.parent_id IS NULL
  AND r_parent.child_id IS NULL
  AND h.node_type != 'SITE';
```

---

## 📈 Advanced Analytical SQL Queries

The [`SQL Challenge`](./SQL%20Challenge) script contains 6 production-grade analytical queries designed for Databricks SQL / Snowflake data warehouses:

### 1. Top 10 Energy-Consuming Assets
```sql
SELECT
    t.asset_id,
    a.asset_name,
    a.asset_type,
    a.site_id,
    ROUND(SUM(t.power_consumption), 2) AS total_energy_consumption
FROM iot_telemetry t
JOIN asset_metadata a ON t.asset_id = a.asset_id
GROUP BY t.asset_id, a.asset_name, a.asset_type, a.site_id
ORDER BY total_energy_consumption DESC
LIMIT 10;
```

### 2. Average Daily Energy Consumption per Site
Performs a two-step aggregation: rolls high-frequency readings to daily site sums first, then calculates the multi-day mean.
```sql
WITH daily_site_consumption AS (
    SELECT
        site_id,
        DATE(timestamp)        AS reading_date,
        SUM(power_consumption) AS daily_total
    FROM iot_telemetry
    GROUP BY site_id, DATE(timestamp)
)
SELECT
    site_id,
    ROUND(AVG(daily_total), 2) AS avg_daily_energy_consumption,
    COUNT(*)                   AS days_observed
FROM daily_site_consumption
GROUP BY site_id
ORDER BY avg_daily_energy_consumption DESC;
```

### 3. Repeat Fault Assets (>10 Faults in 30 Days)
```sql
SELECT
    e.asset_id,
    a.asset_name,
    a.site_id,
    COUNT(*) AS fault_count
FROM event e
JOIN asset_metadata a ON e.asset_id = a.asset_id
WHERE e.event_type = 'Fault'
  AND e.timestamp >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
GROUP BY e.asset_id, a.asset_name, a.site_id
HAVING COUNT(*) > 10
ORDER BY fault_count DESC;
```

### 4. Silent Assets (No Telemetry in Last 24 Hours)
Uses an anti-join against active 24-hour reporters combined with a `last_seen` timestamp tracker.
```sql
WITH recent_reporters AS (
    SELECT DISTINCT asset_id
    FROM iot_telemetry
    WHERE timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
),
last_seen AS (
    SELECT asset_id, MAX(timestamp) AS last_reading_ts
    FROM iot_telemetry
    GROUP BY asset_id
)
SELECT
    a.asset_id,
    a.asset_name,
    a.site_id,
    ls.last_reading_ts
FROM asset_metadata a
LEFT JOIN recent_reporters r ON a.asset_id = r.asset_id
LEFT JOIN last_seen ls ON a.asset_id = ls.asset_id
WHERE r.asset_id IS NULL
ORDER BY ls.last_reading_ts ASC NULLS FIRST;
```

### 5. Hourly Building Utilization Percentage
Calculates the proportion of active operating cycles (`operating_mode = 'Running'`) per hour bucket.
```sql
SELECT
    building_id,
    DATE_TRUNC('HOUR', timestamp) AS hour_bucket,
    COUNT(*) AS total_readings,
    SUM(CASE WHEN operating_mode = 'Running' THEN 1 ELSE 0 END) AS active_readings,
    ROUND(
        100.0 * SUM(CASE WHEN operating_mode = 'Running' THEN 1 ELSE 0 END) / COUNT(*), 2
    ) AS utilization_pct
FROM iot_telemetry
GROUP BY building_id, DATE_TRUNC('HOUR', timestamp)
ORDER BY building_id, hour_bucket;
```

### 6. Power Consumption Anomaly Detection (μ ± 2σ)
Identifies power consumption surges by comparing daily totals against a trailing 7-day rolling window baseline.
```sql
WITH daily_site_consumption AS (
    SELECT
        site_id,
        DATE(timestamp)        AS reading_date,
        SUM(power_consumption) AS daily_total
    FROM iot_telemetry
    GROUP BY site_id, DATE(timestamp)
),
rolling_stats AS (
    SELECT
        site_id,
        reading_date,
        daily_total,
        AVG(daily_total) OVER (
            PARTITION BY site_id
            ORDER BY reading_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS rolling_avg_7d,
        STDDEV(daily_total) OVER (
            PARTITION BY site_id
            ORDER BY reading_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS rolling_stddev_7d
    FROM daily_site_consumption
)
SELECT
    site_id,
    reading_date,
    daily_total,
    ROUND(rolling_avg_7d, 2) AS baseline_avg_7d,
    ROUND(rolling_avg_7d + 2 * rolling_stddev_7d, 2) AS anomaly_threshold,
    ROUND(daily_total - rolling_avg_7d, 2) AS delta_from_baseline
FROM rolling_stats
WHERE rolling_avg_7d IS NOT NULL
  AND daily_total > rolling_avg_7d + 2 * rolling_stddev_7d
ORDER BY site_id, reading_date;
```

---

## ⏱️ Orchestration, Resilience & Monitoring

The production orchestration strategy is defined in [`Orchestration & Scheduling`](./Orchestration%20&%20Scheduling) and verified in [`Orchestration & Scheduling.ipynb`](./Orchestration%20&%20Scheduling.ipynb).

### Pipeline DAG Flow

The pipeline executes as a Directed Acyclic Graph (DAG) with strict dependency enforcements:
1. **Trigger / Sensor**: FileSensor or storage trigger verifies the arrival of raw batch files in cloud storage before allocating compute resources.
2. **Bronze Ingestion**: Loads raw CSV files into distributed DataFrames for `raw_telemetry`, `raw_events`, and `raw_metadata`.
3. **Silver Cleaning & Validation**: Executes data quality validation rules in parallel:
   - **Branch A (Clean Data)**: Records meeting all validation criteria are persisted to `silver_telemetry_clean`.
   - **Branch B (Quarantine Data)**: Violations are routed to `quarantine_telemetry` (dispatches P2 Slack alert if failure rate > 5%).
   - **Branch C (Audit Reporting)**: Execution summary metrics are logged to `data_quality_summary_report` (dispatches P3 Daily Digest).
4. **Gold Aggregations**: Aggregates hourly rollups and multi-level summaries (`asset_metrics`, `building_metrics`, `site_metrics`) only after Silver clean ETL succeeds (`all_success` trigger rule).
5. **Delta Optimization**: Runs `OPTIMIZE` with Z-Ordering on `(asset_id, sensor_id)` to ensure low-latency analytical reads.

### Failure Handling & Circuit Breaking
- **Dead-Letter Isolation**: Individual malformed records are diverted to `quarantine_telemetry` to protect downstream batch SLAs.
- **Task-Level Circuit Breaking**: The orchestration pipeline aborts execution if:
  1. Mandatory raw ingestion files are missing or unreadable.
  2. Cluster compute allocation or runtime initialization fails.
  3. Data quality failure rate exceeds the critical circuit breaker threshold (> 10% quarantined records).
- **Transactional State Isolation**: Delta Lake ACID logs ensure failed jobs perform automatic rollback, preventing dirty reads.

### Retry Policy & Idempotency
- **Exponential Backoff**: Transient cloud API rate limits and network blips trigger retries with increasing wait intervals:
  - `retries`: 3
  - `retry_delay`: `timedelta(minutes=2)`
  - `retry_exponential_backoff`: `True`
  - `max_retry_delay`: `timedelta(minutes=15)`
- **Strict Idempotency**: All table writes use `mode("overwrite")` on partitions or `MERGE INTO` (Upsert), ensuring safe re-execution without duplicate generation.

### Alerting & Notification Matrix

| Severity Level | Trigger Condition | Notification Channel | Action / Response |
|---|---|---|---|
| **P1 - Critical** | Pipeline failure, compute crash, or circuit-breaker trip | PagerDuty Incident + SMS / Call | Immediate on-call Data Engineer escalation |
| **P2 - Warning** | Batch SLA exceeded (>15 min) or Quarantine Rate >5% | Slack / MS Teams (`#iot-pipeline-alerts`) | On-call engineer inspects quarantine log |
| **P3 - Info** | Daily pipeline completion | Stakeholder Email Digest | Daily executive summary of ingested volumes and pass rates |

---

## ⚡ Performance Optimization & Storage Strategy

1. **Multi-Dimensional Z-Ordering**:
   Accelerates time-series slicing and asset queries by co-locating data along high-cardinality search columns:
   ```sql
   OPTIMIZE dataworkspace.default.silver_telemetry_clean ZORDER BY (asset_id, sensor_id);
   OPTIMIZE dataworkspace.default.asset_metrics ZORDER BY (asset_id);
   ```
2. **Partition Pruning**:
   Fact tables (`fact_telemetry`, `fact_energy_hourly`, `fact_event`) are partitioned by `date_key` (e.g., `YYYYMMDD`). Date-filtered dashboard queries skip unneeded partition files, drastically lowering I/O costs.
3. **Delta Column Sanitization**:
   Removes special characters (`(kW)`, `(°C)`, `(%)`, spaces) from raw CSV headers during initial ingestion, preventing expensive schema evolution conflicts in Delta parquet files.
4. **Liquid Clustering (Databricks / Delta 3.0+)**:
   Eliminates partition skew and file over-segmentation for high-volume streaming ingest.

---

## 📦 Prerequisites & Environment Setup

### Environment Requirements
- **Databricks Runtime**: Version 12.2 LTS or 13.x+ (Spark 3.4+, Scala 2.12/2.13, Python 3.10+)
- **Storage & Metastore**: Azure Data Lake Storage Gen2 / AWS S3 with **Databricks Unity Catalog** enabled
- **Local Development (Optional)**:
  - Python 3.10+
  - PySpark `3.4.0+`
  - Delta-Spark `2.4.0+`
  - JupyterLab or VS Code with Jupyter extension

### Unity Catalog Volumes Setup
Ensure the following volume path is accessible in your Databricks workspace:
```text
/Volumes/dataworkspace/default/data/
├── Iot telemetry.csv
├── Event.csv
└── Asset Metadata.csv
```

---

## 🚀 Execution & Deployment Guide

### Option 1: Running in Databricks Workspace (Recommended)
1. **Import Notebooks**:
   Import the `.ipynb` files into your Databricks workspace:
   - `Build Data Quality.ipynb`
   - `transformation and validation.ipynb`
   - `Multi-Asset Hierarchy &  Connectivity.ipynb`
   - `Orchestration & Scheduling.ipynb`
2. **Configure Unity Catalog**:
   Verify catalog `dataworkspace` and schema `default` exist:
   ```sql
   CREATE CATALOG IF NOT EXISTS dataworkspace;
   CREATE SCHEMA IF NOT EXISTS dataworkspace.default;
   ```
3. **Execute the End-to-End Orchestration Notebook**:
   Open and run [`Orchestration & Scheduling.ipynb`](./Orchestration%20&%20Scheduling.ipynb) to execute Bronze ingestion, Silver cleaning, Gold aggregations, and Delta Z-Ordering in sequence.

### Option 2: Deploying via Databricks Workflows / Jobs
1. Navigate to **Workflows** → **Create Job** in the Databricks console.
2. Configure tasks in sequence:
   - **Task 1 (`Bronze_Ingest_Silver_Clean`)**: Run notebook `Build Data Quality.ipynb`.
   - **Task 2 (`Gold_Aggregations`)**: Run notebook `transformation and validation.ipynb` (Depends on Task 1).
   - **Task 3 (`Optimize_ZOrder`)**: Run SQL / notebook optimizing tables (Depends on Task 2).
3. Set schedule trigger (e.g., hourly cron `0 * * * *`).
4. Configure Slack Webhook under Job Notifications for failure alerting.

---

## 🎥 Artifacts & Media Walkthrough

| Artifact | File | Description |
|---|---|---|
| **Executive Presentation** | [`Data Engineer Presentation.pptx`](./Data%20Engineer%20Presentation.pptx) | Comprehensive slide deck covering architecture, data quality results, and performance benchmarks |
| **Video Walkthrough** | [`Task-Explanation.mp4`](./Task-Explanation.mp4) | High-definition technical walkthrough explaining design decisions, notebooks, and query outputs |
| **System Architecture Diagram** | [`data architecture design.png`](./data%20architecture%20design.png) | Visual architectural blueprint of the end-to-end IoT platform |
| **ER Diagram** | [`ER.drawio.png`](./ER.drawio.png) | Complete Entity-Relationship diagram for dimensional and graph tables |

---

## 🔧 Troubleshooting & Common Issues

| Issue / Error | Root Cause | Solution |
|---|---|---|
| `AnalysisException: Attribute name contains invalid character(s)` | Raw CSV headers contain special characters (e.g., `power_consumption (kW)`, `temperature (°C)`) | Use the `clean_cols()` regex function before writing to Delta Lake, or enable `spark.databricks.delta.properties.defaults.columnMapping.mode = "name"`. |
| `FileNotFoundException: /Volumes/dataworkspace/default/data/...` | Unity Catalog Volume path is not mounted or file names differ | Verify Unity Catalog Volume permissions and confirm CSV file names in `/Volumes/dataworkspace/default/data/`. |
| `Recursion depth limit exceeded in CTE` | Cycle / circular reference in `asset_relationship` graph | Add a cycle detection guard: `WHERE sh.depth < 10` or verify that parent-child relationships are strictly directed acyclic. |
| `SparkOutOfMemory / GC Thrashing during Z-Order` | Small worker nodes attempting Z-Ordering on millions of small files | Enable Delta auto-compaction (`delta.autoOptimize.optimizeWrite = true`) and run `OPTIMIZE` on multi-core compute clusters. |

---

## 🔒 Security & Data Governance

- **Unity Catalog Governance**: Centralized access control on catalogs, schemas, and tables (`GRANT SELECT ON TABLE dataworkspace.default.asset_metrics TO role_analysts`).
- **Credential Isolation**: Direct access to storage accounts is mediated via Databricks Managed Identities / IAM instance profiles, eliminating hardcoded access keys.
- **Zero Sensitive Data in Code**: No cloud secrets, tokens, or private endpoints are hardcoded in the codebase.

---

## 🔮 Future Enhancements

- [ ] **Streaming Ingestion**: Transition from micro-batch ingestion to real-time Spark Structured Streaming with Delta Live Tables (DLT).
- [ ] **Predictive Maintenance ML Pipeline**: Integrate Databricks Feature Store and MLflow to train Remaining Useful Life (RUL) regression models on vibration and temperature time-series features.
- [ ] **Automated Remediation**: Implement automated webhooks triggered by the anomaly detection query to automatically file work orders in CMMS (Computerized Maintenance Management Systems).

---

## 📄 License & Credits

- **Repository**: [`Priya458798/DE-task`](https://github.com/Priya458798/DE-task)
- **Author**: Priya
- **License**: No license has been specified for this project.