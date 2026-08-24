# 🛡️ Real-Time Fraud Detection Data Platform

> **An end-to-end data engineering and machine learning platform for
> ingesting, transforming, scoring, and analyzing financial transactions
> using Apache Airflow, Databricks, Delta Lake, dbt, MLflow, SQL, and
> Power BI.**

![Python](https://img.shields.io/badge/Python-3.x-3776AB?logo=python&logoColor=white)
![Apache
Airflow](<https://img.shields.io/badge/Apache%20Airflow-Orchestration-017CEE?logo=apacheairflow&logoColor=white>)
![Databricks](<https://img.shields.io/badge/Databricks-Data%20Platform-EF3B2D?logo=databricks&logoColor=white>)
![Delta
Lake](<https://img.shields.io/badge/Delta%20Lake-Lakehouse-00ADD8>)
![dbt](https://img.shields.io/badge/dbt-Transformations-FF694B?logo=dbt&logoColor=white)
![MLflow](<https://img.shields.io/badge/MLflow-Model%20Tracking-0194E2?logo=mlflow&logoColor=white>)
![Power
BI](<https://img.shields.io/badge/Power%20BI-Analytics-F2C811?logo=powerbi&logoColor=black>)
![SQL](<https://img.shields.io/badge/SQL-Data%20Engineering-336791>)

---

## 📌 Project Overview

Fraud detection is not only a machine learning problem --- it is also a
**data engineering, orchestration, data quality, feature engineering,
and analytics problem**.

This project demonstrates how to build a production-style fraud
detection platform that takes transaction data from ingestion through a
**Medallion Architecture**, creates analytical and ML-ready datasets,
scores transactions for fraud risk, tracks model performance, and
exposes the results through a Power BI dashboard.

The platform is designed around four major responsibilities:

1. **Ingestion & orchestration** --- Apache Airflow
2. **Lakehouse processing** --- Databricks + Delta Lake
3. **Transformation & data quality** --- dbt
4. **Fraud scoring & model lifecycle** --- Python + MLflow
5. **Business analytics** --- Power BI

> **Important:** The current orchestration shown in the project DAG is
> scheduled/batch-oriented. The architecture is designed so the
> ingestion and scoring layer can be evolved into true event-driven
> streaming using Kafka/Auto Loader + Structured Streaming. This README
> therefore avoids claiming sub-second real-time processing unless that
> streaming layer is enabled.

---

# 🏗️ High-Level Architecture

![1787559014952](image/README(1)/1787559014952.png)

```text
                    ┌──────────────────────────┐
                    │ Synthetic Transactions   │
                    │       Python Generator   │
                    └────────────┬─────────────┘
                                 │
                                 ▼
                    ┌──────────────────────────┐
                    │     Apache Airflow        │
                    │   Pipeline Orchestration  │
                    └────────────┬─────────────┘
                                 │
                                 ▼
                    ┌──────────────────────────┐
                    │       Databricks          │
                    │     Delta Lake / UC       │
                    └────────────┬─────────────┘
                                 │
                         ┌───────▼───────┐
                         │    BRONZE     │
                         │ Raw ingestion  │
                         └───────┬───────┘
                                 │
                         ┌───────▼───────┐
                         │    SILVER     │
                         │ Clean + model │
                         └───────┬───────┘
                                 │
                         ┌───────▼───────┐
                         │     GOLD      │
                         │ Business + ML │
                         └───────┬───────┘
                                 │
                  ┌──────────────┴──────────────┐
                  ▼                             ▼
        ┌──────────────────┐          ┌──────────────────┐
        │ Fraud ML Scoring │          │    Power BI      │
        │ Python + MLflow  │          │ Risk Analytics   │
        └────────┬─────────┘          └──────────────────┘
                 │
                 ▼
        ┌──────────────────┐
        │ Fraud Risk Tier  │
        │ Low / Medium /   │
        │ High             │
        └──────────────────┘
```

---

# 🔄 End-to-End Data Flow

The pipeline follows this logical flow:

```text
Generate Transactions
        ↓
Load / Ingest Data
        ↓
Trigger dbt Pipeline
        ↓
Bronze Transformation
        ↓
Bronze Tests
        ↓
Silver Transformation
        ↓
Silver Tests
        ↓
Snapshots
        ↓
Gold Transformation
        ↓
Final Data Quality Tests
        ↓
Fraud Scoring / Analytics
        ↓
Power BI
```

This separation keeps **orchestration, transformation, ML, and
visualization responsibilities distinct** instead of putting the entire
pipeline inside one large script.

---

# ⚙️ Apache Airflow Orchestration

Airflow is responsible for coordinating the complete workflow and
controlling execution order.

### DAG workflow

![Airflow DAG Workflow](assets/airflow-dag-workflow.png)

![1787559036332](image/README(1)/1787559036332.png)

The current DAG contains tasks following the pattern:

```text
generate_transactions
        ↓
load_to_neon
        ↓
trigger_dbt_pipeline
        ↓
dbt_deps
        ↓
dbt_run_bronze
        ↓
dbt_test_bronze
        ↓
dbt_run_silver
        ↓
dbt_test_silver
        ↓
dbt_snapshot_silver
        ↓
dbt_run_gold
        ↓
dbt_test_all
```

> The task names shown above reflect the current workflow diagram. The
> ingestion component can be migrated from the earlier PostgreSQL/Neon
> landing approach to a Databricks-native ingestion path as the
> streaming architecture evolves.

### Why Airflow?

Airflow provides:

- Dependency management
- Scheduling
- Retry handling
- Task-level logging
- Pipeline observability
- Failure isolation
- Separation between orchestration and transformation logic

---

# 🧱 Medallion Architecture

The transformation layer follows the **Bronze → Silver → Gold** pattern.

## 🥉 Bronze Layer

The Bronze layer represents the first structured representation of
incoming transaction data.

Responsibilities:

- Ingest raw transaction records
- Preserve source-level information
- Apply basic schema normalization
- Add ingestion metadata
- Provide a reliable downstream source

Example conceptual table:

```text
bronze.raw_transactions
```

The Bronze layer should remain close to the source and avoid heavy
business logic.

---

## 🥈 Silver Layer

The Silver layer converts raw data into clean, analytics-ready entities.

Typical models include:

```text
si_transactions_fact
si_users_dimension
si_devices_dimension
```

### Transaction fact

Contains the transaction-level grain:

```text
transaction_id
user_id
device_id
merchant_id
transaction_date
amount
payment_method
location
fraud indicators
risk-related attributes
```

### User dimension

Contains user-level descriptive information.

### Device dimension

Contains device-level attributes and risk-related information.

The result is a dimensional/star-schema-oriented analytical model that
can be consumed by downstream business and ML workloads.

---

## 🥇 Gold Layer

The Gold layer contains business-facing metrics and ML-oriented feature
datasets.

Important models include:

```text
go_user_behavior_metrics
go_merchant_risk_assessment
go_predictive_model_features
go_fraud_detection_dashboard_metrics
go_fraud_prediction
```

The Gold layer answers questions such as:

- Which users show abnormal transaction behavior?
- Which merchants have elevated fraud rates?
- What is the user's historical transaction baseline?
- What features should be supplied to the fraud model?
- How many transactions are flagged?
- How much money is at risk?
- How is fraud distributed across payment methods and devices?

---

# 🔗 dbt Model Lineage

dbt manages the SQL transformation layer and makes dependencies explicit
through model references.

### Current dbt lineage

![dbt Lineage Graph](assets/dbt-lineage-graph.png)



The lineage demonstrates the progression from raw transaction data
through Silver models, snapshots, predictive features, and Gold
analytical outputs.

A simplified representation is:

```text
raw_transactions
       │
       ▼
transactions
       │
       ├──────────────► users
       │
       ├──────────────► devices
       │
       ▼
si_transactions_fact
       │
       ├──────────────► go_user_behavior_metrics
       │
       ├──────────────► go_merchant_risk_assessment
       │
       └──────────────► go_predictive_model_features
                                  │
                                  ▼
                         fraud prediction layer
                                  │
                                  ▼
                    fraud detection dashboard
```

---

# 🧪 Data Quality with dbt

Data quality is treated as part of the pipeline rather than as an
afterthought.

The project uses dbt tests across the transformation layers.

Typical checks include:

```text
unique
not_null
relationships
accepted_values
```

For example:

```yaml
tests:
  - unique
  - not_null
```

This provides automated quality gates between Bronze, Silver, and Gold.

The Airflow DAG also separates transformation and testing tasks, making
failures easier to identify.

---

# 📸 Snapshots & Historical Tracking

The project uses dbt snapshots to preserve historical changes in
selected entities.

Conceptually:

```text
Current Record
      ↓
Change Detected
      ↓
Snapshot
      ↓
Historical Version
```

This is useful for fraud analytics because attributes such as:

- user information
- device characteristics
- risk classifications

may change over time.

Instead of overwriting history, snapshots allow the pipeline to answer:

> **"What did this entity look like when the transaction occurred?"**

---

# 🤖 Fraud Detection & Machine Learning

The ML component is separated from the transformation layer.

The pipeline prepares features in the Gold layer and then uses Python/ML
tooling for model training and scoring.

### Feature engineering examples

The project includes fraud-oriented features such as:

- Transaction velocity
- Geo-anomaly indicators
- Device risk
- Merchant risk rate
- Historical user behavior
- Transaction frequency
- Amount-based behavioral signals

### Why feature engineering happens before scoring

Instead of calculating every feature inside the model code:

```text
Raw Transaction
      ↓
Clean Data
      ↓
Behavioral Features
      ↓
ML-ready Feature Table
      ↓
Fraud Model
```

This creates reusable, testable feature datasets.

---

# 📊 Fraud Classification

The fraud model is designed for an imbalanced classification problem
where fraudulent transactions represent a small percentage of the total
volume.

The scoring layer can produce:

```text
Fraud Probability / Risk Score
            ↓
       Risk Tier
      /    |     \
    Low  Medium   High
```

The model evaluation focuses on metrics that are more meaningful for
fraud detection than accuracy alone:

- Precision
- Recall
- PR-AUC
- Confusion Matrix
- Fraud detection rate

### Why accuracy is not enough

If only \~2% of transactions are fraudulent, a model that predicts every
transaction as legitimate could still achieve very high accuracy while
being useless for fraud detection.

Therefore:

```text
Precision + Recall + PR-AUC
```

are more useful indicators of model quality.

---

# 🧪 MLflow Model Tracking

MLflow is used to track the machine learning lifecycle.

The experiment workflow is:

```text
Train Model
    ↓
Log Parameters
    ↓
Log Metrics
    ↓
Log Model
    ↓
Register / Track Model
    ↓
Score New Transactions
```

Example tracked metrics:

```text
precision
recall
PR-AUC
fraud detection rate
```

This makes model experiments reproducible and provides a foundation for
model versioning and deployment.

---

# 📈 Power BI Analytics

Power BI acts as the business-facing consumption layer.

The dashboard is designed around three major perspectives.

## Page 1 --- Executive Fraud Overview

Focuses on high-level KPIs:

- Total transactions
- Total flagged transactions
- Fraud rate
- Total amount at risk
- Model precision
- Daily transaction volume
- Flagged transaction trend
- Risk-tier distribution
- Fraud rate by payment method

---

## Page 2 --- Transaction & Risk Deep Dive

Provides detailed transaction-level analysis:

- Risk score distribution
- Fraud by payment method
- Fraud by device type
- Geographic fraud analysis
- Time-of-day patterns
- Transaction risk segmentation

---

## Page 3 --- User Risk Insights

Focuses on user behavior:

- Users ranked by flagged transaction count
- Flagged transaction amount
- User-level risk
- Historical transaction behavior
- Drill-through into transaction history
- Behavioral baseline comparison

---

# 🧠 What Makes This Project Different?

This project is intentionally more than a simple:

> **"CSV → SQL → Power BI"**

dashboard.

It combines multiple layers of a modern data platform:

```text
                DATA ENGINEERING
                     │
       ┌─────────────┼─────────────┐
       ▼             ▼             ▼
   Ingestion     Transformation  Quality
       │             │             │
       └─────────────┼─────────────┘
                     ▼
                FEATURE STORE
                     │
                     ▼
                MACHINE LEARNING
                     │
                     ▼
                 MLflow
                     │
                     ▼
                 POWER BI
```

The result demonstrates understanding of:

- Data engineering
- Lakehouse architecture
- Dimensional modeling
- SQL transformation
- dbt
- Spark/Databricks
- Workflow orchestration
- Data quality
- Feature engineering
- Imbalanced classification
- Model tracking
- BI analytics

---

# 🚀 Real-Time / Near-Real-Time Evolution

The current Airflow workflow is batch-oriented. To turn the platform
into a genuinely event-driven fraud detection system, the next
architectural evolution would be:

```text
Transaction Event
       ↓
Kafka / Event Bus
       ↓
Databricks Structured Streaming
       ↓
Bronze Delta Table
       ↓
Streaming Feature Engineering
       ↓
Fraud Model
       ↓
Risk Score
       ↓
High-Risk Alert
       ↓
Gold Delta Table
       ↓
Power BI / Alerting
```

### Where dbt fits

dbt should remain responsible for the **analytics and batch
transformation layer**, rather than being used as the event-by-event
fraud scoring engine.

That gives the architecture a clean separation:

```text
HOT PATH
Kafka → Spark Streaming → Fraud Score

ANALYTICS PATH
Delta → dbt → Gold Models → Power BI
```

This is a more realistic production architecture than trying to force
dbt itself to perform sub-second fraud detection.

---

# 🗂️ Suggested Repository Structure

```text
fraud-detection-platform/
│
├── airflow/
│   ├── dags/
│   │   └── fraud_detection_pipeline.py
│   └── config/
│
├── DataGeneration/
│   ├── generate_transactions.py
│   └── raw_data/
│
├── dbt/
│   └── fraud_detection/
│       ├── models/
│       │   ├── bronze/
│       │   ├── silver/
│       │   └── gold/
│       │
│       ├── snapshots/
│       ├── tests/
│       ├── macros/
│       ├── seeds/
│       ├── dbt_project.yml
│       └── packages.yml
│
├── ml/
│   ├── train.py
│   ├── predict.py
│   ├── features.py
│   └── evaluation.py
│
├── powerbi/
│   └── fraud_detection_dashboard.pbix
│
├── assets/
│   ├── airflow-dag-workflow.png
│   └── dbt-lineage-graph.png
│
├── requirements.txt
├── .env.example
└── README.md
```

---

# 🔧 Technology Stack

  Layer                 Technology                   Purpose

---

  Data Generation       Python                       Synthetic transaction generation
  Orchestration         Apache Airflow               Scheduling & dependency management
  Processing            Databricks                   Lakehouse compute
  Storage               Delta Lake                   Reliable transactional data storage
  Transformation        dbt-databricks               SQL transformation & modeling
  Modeling              SQL / Dimensional Modeling   Analytics-ready datasets
  ML                    Python / scikit-learn        Fraud classification
  Experiment Tracking   MLflow                       Model lifecycle & experiment tracking
  Visualization         Power BI                     Fraud analytics & reporting
  Version Control       Git / GitHub                 Source control
  CI/CD                 GitHub Actions               Automated testing/deployment

---

# 🛠️ Key Engineering Concepts Demonstrated

### Data Engineering

- ETL / ELT
- Batch processing
- Incremental processing
- Lakehouse architecture
- Delta Lake
- Medallion architecture
- Dimensional modeling
- Data lineage

### dbt

- Models
- Sources
- `ref()`
- Incremental models
- Snapshots
- Generic tests
- Singular tests
- Macros
- Model dependencies
- Documentation

### Airflow

- DAG design
- PythonOperator
- BashOperator
- Task dependencies
- Scheduling
- Retries
- Pipeline monitoring

### Databricks

- Unity Catalog
- Delta tables
- Spark processing
- Databricks SQL
- Lakehouse architecture

### Machine Learning

- Feature engineering
- Binary classification
- Class imbalance
- Precision
- Recall
- PR-AUC
- Risk scoring
- Model tracking

---

# 🔐 Configuration

Secrets should never be committed to GitHub.

Use environment variables or a secret manager for credentials such as:

```text
DATABRICKS_HOST
DATABRICKS_TOKEN
DATABRICKS_HTTP_PATH
MLFLOW_TRACKING_URI
```

Create a local `.env` from:

```text
.env.example
```

and keep `.env` in `.gitignore`.

---

# ▶️ Pipeline Execution

A typical execution sequence is:

```bash
# Generate transaction data
python DataGeneration/generate_transactions.py

# Data Upload to Neon Database
python DBConnection/ data_load.py

# Start Airflow
airflow standalone
airflow scheduler
airflow webserver

# Trigger the fraud detection DAG
# Airflow UI → fraud_detection_pipeline
```

The DAG then orchestrates:

```text
Generate
   ↓
Ingest
   ↓
dbt dependencies
   ↓
Bronze
   ↓
Bronze tests
   ↓
Silver
   ↓
Silver tests
   ↓
Snapshots
   ↓
Gold
   ↓
Final tests
```

---

# 📋 Data Quality Strategy

The pipeline follows a layered quality approach.

```text
SOURCE QUALITY
      ↓
Bronze Tests
      ↓
SILVER QUALITY
      ↓
Silver Tests
      ↓
BUSINESS QUALITY
      ↓
Gold Tests
      ↓
ML QUALITY
      ↓
Model Evaluation
```

This means data quality is validated before data reaches the business
dashboard or ML scoring layer.

---

# 📊 Business Questions This Platform Can Answer

### Transaction-level

- Is this transaction high risk?
- What is the predicted fraud probability?
- Which device was used?
- Is the transaction geographically unusual?

### User-level

- Which users generate the most suspicious transactions?
- Has a user's transaction behavior changed?
- Is transaction velocity unusually high?

### Merchant-level

- Which merchants have elevated fraud rates?
- What is the historical merchant risk?

### Business-level

- What percentage of transactions are flagged?
- How much money is at risk?
- Which payment method has the highest fraud rate?
- How is fraud changing over time?

---

# 🎯 Future Enhancements

The strongest next steps for turning this into a more production-grade
platform are:

### 1. True event-driven ingestion

```text
Kafka / Redpanda
        ↓
Databricks Structured Streaming
```

### 2. Online fraud scoring

Score transactions as events arrive rather than waiting for a scheduled
batch.

### 3. Automated alerts

Trigger notifications when:

```text
Risk Score > Threshold
```

through email, Slack, Teams, or an incident platform.

### 4. Feature Store

Create reusable and versioned fraud features for both training and
inference.

### 5. Model monitoring

Track:

- Prediction drift
- Feature drift
- Fraud rate drift
- Precision/recall degradation
- Data freshness

### 6. CI/CD

Add automated:

```text
Lint
  ↓
dbt compile
  ↓
dbt test
  ↓
Unit tests
  ↓
Deploy
```

### 7. Infrastructure as Code

Introduce Terraform for reproducible Databricks infrastructure.

---

# 🏆 Portfolio / Interview Value

This project is designed to demonstrate that you can build more than
individual scripts or dashboards.

You can explain the system as:

> **"I built an end-to-end fraud detection data platform using Airflow
> for orchestration, Databricks and Delta Lake for lakehouse processing,
> dbt for modular transformations and data quality, MLflow for model
> tracking, and Power BI for business analytics. The pipeline follows a
> Bronze-Silver-Gold architecture, creates behavioral and risk features,
> scores transactions using an imbalanced classification model, and
> exposes user, merchant, transaction, and executive-level fraud
> insights. I also designed the architecture so the batch ingestion path
> can evolve into an event-driven Databricks Structured Streaming
> pipeline."**

---

# 💡 Why This Is a Strong Data Engineering Project

The important part is not simply the number of technologies used.

The project demonstrates **separation of responsibilities**:

```text
Airflow
  → When should things run?

Databricks
  → Where should large-scale processing happen?

Delta Lake
  → How should reliable lakehouse data be stored?

dbt
  → How should analytical data be transformed and tested?

MLflow
  → How should ML experiments and models be tracked?

Power BI
  → How should business users consume the results?
```

That architectural thinking is the core value of the project.

---

# 📌 Project Status

  Component                             Status

---

  Synthetic transaction generation      ✅
  Airflow orchestration                 ✅
  Databricks integration                ✅
  Delta Lake / Medallion architecture   ✅
  dbt Bronze layer                      ✅
  dbt Silver layer                      ✅
  dbt Gold layer                        ✅
  dbt tests                             ✅
  dbt snapshots                         ✅
  Behavioral feature engineering        ✅
  Fraud ML model                        ✅
  MLflow tracking                       ✅
  Power BI analytics                    ✅
  Event-driven streaming                🚧 Future enhancement
  Real-time alerting                    🚧 Future enhancement
  Model monitoring                      🚧 Future enhancement

---

# ⭐ Final Architecture Goal

The long-term architecture is:

```text
                   ┌─────────────────┐
                   │ Transaction     │
                   │ Events          │
                   └────────┬────────┘
                            │
                            ▼
                   ┌─────────────────┐
                   │ Kafka / Event   │
                   │ Bus             │
                   └────────┬────────┘
                            │
                            ▼
              ┌─────────────────────────┐
              │ Databricks Structured   │
              │ Streaming               │
              └────────────┬────────────┘
                           │
                           ▼
                    ┌─────────────┐
                    │   BRONZE    │
                    └──────┬──────┘
                           │
                           ▼
                    ┌─────────────┐
                    │   SILVER    │
                    └──────┬──────┘
                           │
             ┌─────────────┴─────────────┐
             ▼                           ▼
      ┌──────────────┐            ┌──────────────┐
      │ Fraud Model  │            │     dbt      │
      │ + MLflow     │            │ Gold Models  │
      └──────┬───────┘            └──────┬───────┘
             │                           │
             ▼                           ▼
      ┌──────────────┐            ┌──────────────┐
      │ Risk Score   │            │ Power BI     │
      │ + Alerts     │            │ Dashboard    │
      └──────────────┘            └──────────────┘
```

---

## 👨‍💻 Author

**Ayush Sharma**

Data Engineering \| Analytics Engineering \| Databricks \| dbt \|
PySpark \| Airflow \| Machine Learning

---

## ⭐ If you found this project useful

Give the repository a ⭐ and feel free to explore the architecture,
transformation models, and orchestration workflow.
