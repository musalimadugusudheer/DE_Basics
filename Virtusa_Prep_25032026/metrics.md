Absolutely. Here’s how I’d answer each of those parameters for the Kahramaa project, based on the architecture we discussed – in a conversational, bullet‑point style.


---

## 1. Pipeline Inventory (ADF + Databricks jobs)

- **Total ADF pipelines** – about **27**, grouped by purpose.
  - **Ingestion – batch** (12): hourly/daily copies from Oracle ERP, MySQL CRM, flat files.
  - **Streaming orchestration** (2): trigger Databricks Structured Streaming jobs from Event Hubs.
  - **Orchestration** (6): Bronze→Silver, Silver→Gold – call Databricks notebooks and wait for completion.
  - **Post‑processing** (4): `pl_vacuum_optimize` (Delta maintenance), `pl_refresh_pbi_dataset` (Power BI).
  - **Error handling / monitoring** (3): reprocess quarantine, send alerts on failure.
- **Databricks jobs** – not ADF pipelines, but about **8‑10** production notebooks scheduled via Databricks jobs API or ADF.
- **Schedule examples**:
  - Streaming pipelines – continuous / always on.
  - Hourly batch – e.g., `pl_ingest_oracle_erp` runs at `:05` past each hour.
  - Daily batch – `pl_ingest_mysql_crm` runs at 02:00 UTC.
  - Silver transforms – triggered immediately after Bronze ingestion completes (event‑based).
  - Gold aggregations – once per day after Silver is ready.
  - Maintenance pipelines – weekly (Sunday 01:00) for `VACUUM` and `OPTIMIZE`.

---

## 2. Data Volumes

- **Daily raw ingestion** – ~185 GB (streaming + batch).
  - Streaming (IoT): ~150 GB/day from 192M smart meter events.
  - Batch: ~35 GB/day from ERP, CRM, flat files.
- **Monthly raw** – ~5.5 TB → after Delta compaction: Bronze ~4.5 TB, Silver ~2.1 TB, Gold ~0.9 TB.
- **Yearly raw** – ~65 TB → Bronze ~54 TB, Silver ~25 TB, Gold ~11 TB.
- **Record counts (streaming only)** – 192M events/day → 5.8B/month → 70B/year.
- **Gold table sizes** – `FactElectricityUsage` (daily grain) ~730M rows/year.
- **Key observation** – Gold is ~17% of raw volume because of aggregation (15‑minute readings rolled up to daily/hourly).

---

## 3. Batch vs. Streaming Architecture

- **Streaming path**:
  - Sources: IoT smart meters (electricity/water), grid sensors → Azure Event Hubs / IoT Hub.
  - Processing: Databricks Structured Streaming with watermarking (for late arrivals).
  - Sink: Bronze Delta tables (append‑only).
  - Latency: ~1‑2 minutes from meter to Bronze.
  - Use case: real‑time dashboards, anomaly detection, alerts.
- **Batch path**:
  - Sources: Oracle ERP, MySQL CRM, flat files, APIs.
  - Ingestion: ADF Copy Activity or PolyBase → Landing zone (ADLS).
  - Processing: Databricks Auto Loader → Bronze → Silver → Gold (batch mode).
  - Frequency: hourly (small incremental) or daily (full refresh for slowly changing dimensions).
  - Use case: billing, customer 360, historical reporting, regulatory compliance.
- **Why both?** – Streaming for operational visibility (e.g., grid overload), batch for heavy ETL and cost efficiency.

---

## 4. Incremental Strategy

- **Bronze layer** – append‑only. Every new file or streaming micro‑batch is appended. No updates/deletes.
- **Silver layer** – upsert pattern using Delta Lake `MERGE`.
  - For fact‑like tables (e.g., meter readings): merge on natural key + timestamp.
  - For dimension tables (e.g., customer): SCD Type 2 – insert new version, update current flag.
- **Gold layer** – incremental insert of new facts; dimensions refreshed fully when changes are detected.
- **Change data capture (CDC)** – not fully implemented; instead use:
  - `ROW_NUMBER()` over partition by business key ordered by ingestion timestamp to deduplicate.
  - For Oracle/MySQL, ADF uses query partitioning with `watermark column` (e.g., `last_modified_date`).
- **Checkpointing** – Databricks Structured Streaming checkpoints stored in `abfss://logs@.../checkpoints/` – allows exactly‑once semantics.
- **Batch incremental example** – `pl_ingest_oracle_erp` reads only rows where `last_modified > last_run_high_watermark`.

---

## 5. Error Handling

- **Three‑tier approach**:
  1. **Schema & type validation** – at Bronze ingestion. Malformed records (e.g., negative consumption, missing meter ID) go to a **dead‑letter / quarantine Delta table** – `bronze.quarantine_iot`, etc.
  2. **Business rule validation** – at Silver layer. Rules like “reading date > installation date”, “tariff code exists”. Failed rows moved to `silver.quarantine` instead of blocking the entire pipeline.
  3. **Pipeline failures** – ADF retry policy (3 retries, 5‑minute delay). If still failing → send alert to Azure Monitor → email to on‑call.
- **Dead‑letter reprocessing** – a manual or scheduled ADF pipeline (`pl_quarantine_reprocess`) reads quarantine, fixes issues (if possible), and re‑inserts.
- **Monitoring** – Azure Monitor + Log Analytics. Custom queries for:
  - Late‑arriving events (watermark exceeded)
  - Row count drift between Bronze and Silver
  - Pipeline duration outliers
- **Idempotency** – all pipelines are idempotent. Re‑running a batch won’t double‑count because Silver upserts and Bronze is append‑only.

---

## 6. Cluster Configuration (Databricks)

We have **four main clusters** – tailored to workload.

| Cluster Name | Mode | Node Type | Auto‑scaling | Runtime | Key Spark Config |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `cluster_bronze_streaming` | Standard | `Standard_D4ds_v5` (4 vCPU, 16 GB) | 2–6 nodes | 13.3 LTS | `spark.sql.streaming.schemaInference=true`, `spark.databricks.delta.retentionDurationCheck.enabled=false` |
| `cluster_silver_transform` | Job | `Standard_E8ds_v4` (8 vCPU, 64 GB) – memory‑optimised | 4–20 nodes | 13.3 LTS ML | `spark.sql.adaptive.enabled=true`, `spark.databricks.delta.optimizeWrite.enabled=true` |
| `cluster_gold_aggregate` | Job | `Standard_D8ds_v5` (8 vCPU, 32 GB) | 2–12 nodes | 13.3 LTS | `spark.sql.shuffle.partitions=400` (tuned for large fact tables) |
| `cluster_dev_interactive` | Standard (single user) | `Standard_D4ds_v5` | 1–4 nodes | 13.3 LTS | Same as prod but with lower max cores |

- **Idle timeout** – 20 minutes for interactive, job clusters terminate immediately after job finishes.
- **Libraries** – Event Hubs connector (`com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22`), Azure Blob File System (ABFS) driver.
- **Environment variables** – `DELTA_RETENTION=168h`, `LOG_LEVEL=INFO`.
- **Autoscaling** – enabled on all; production clusters have min workers to handle peak load.

---

**“So for the Kahramaa project, we’ve got a clear inventory of pipelines, realistic data volumes, a hybrid batch‑streaming architecture, robust incremental handling, a quarantine‑based error model, and clusters sized per workload. All metrics are estimated but backed by scripts to pull real numbers.”**