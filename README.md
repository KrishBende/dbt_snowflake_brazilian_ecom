# End-to-End Analytics Engineering & Machine Learning on Snowflake

## Project Overview

This repository contains a **production-style end-to-end data analytics and machine learning platform** built on **Snowflake** using **dbt Core** and **Snowpark**.

The project demonstrates how raw e-commerce data can be transformed into analytics-ready models, business KPIs, and machine learning predictions (revenue forecasting & customer churn), all while following **modern analytics engineering best practices**.

---

## Architecture Overview

**Data Flow**:

```
Raw Tables (Snowflake)
   ↓
Sources (dbt)
   ↓
Staging Models (Cleaning & Type Casting)
   ↓
Fact & Dimension Models
   ↓
Analytics Marts (KPIs)
   ↓
ML Feature Tables
   ↓
Snowpark ML Models
   ↓
Predictions Stored in Snowflake
```

Key design principle: **SQL-first transformations + in-warehouse ML**.

---

## Tech Stack

* **Data Warehouse**: Snowflake
* **Analytics Engineering**: dbt Core
* **Machine Learning**: Snowpark, Pandas, XGBoost
* **Languages**: SQL, Python
* **BI & Dashboards**: Snowflake Dashboards / Worksheets

---

## Project Structure

```
├── models/
│   ├── sources/              # Source definitions & freshness checks
│   ├── staging/              # Cleaned & typed staging models
│   ├── marts/
│   │   ├── core/              # Fact & dimension tables
│   │   ├── analytics/         # KPI & BI models
│   │   └── ml/                # ML feature & label tables

```

---

## Data Quality & Testing

Implemented comprehensive **dbt tests** to ensure reliability:

* `not_null`
* `unique`
* `accepted_values`

Data issues (duplicates, missing values) were **identified, analyzed, and handled intentionally** rather than disabling tests.

---

## Core Analytics & KPIs

Built analytics-ready marts for BI consumption:

* Total Revenue
* Total Orders
* Average Order Value (AOV)
* Revenue Trends (Daily / Monthly)
* Seller Performance
* Product Performance
* Delivery Time Metrics
* Review & Satisfaction Metrics

All KPIs are derived from **dbt models**, ensuring consistency between analytics and dashboards.

---

## Machine Learning Use Cases

### Revenue and Order Quantity Forecasting

* Daily revenue and order quantity aggregation from fact tables
* Time-aware train / test split
* XGBoost regression model
* Predictions written back to Snowflake

**Outputs**:

* Forecasted revenue by date
* Model versioning & timestamps

---

### Customer Churn Modeling

* Behavioral churn definition based on inactivity
* SQL-based feature engineering in dbt
* Binary classification using XGBoost
* Probabilistic churn scores

**Features Used**:

* Recency, Frequency, Monetary (RFM)
* Customer lifetime
* Delivery performance
* Review behavior

---

## Predictive Analytics Dashboards

Dedicated dashboards built for:

* Revenue forecast trends
* Churn rate monitoring
* High-risk customer segmentation
* Distribution of churn probabilities

ML outputs are stored in Snowflake tables for **direct BI access**.

---

## Snowflake-Specific Highlights

* Snowpark-powered ML execution
* In-warehouse model training & inference
* Safe Pandas ↔ Snowflake type handling (DATE, TIMESTAMP)
* Production-safe `write_pandas` patterns
* Schema-controlled prediction tables

---

## How to Run This Project

### Prerequisites

* Snowflake account
* dbt Core installed
* Python 3.9+

### Steps

```bash
# Install dbt dependencies
dbt deps

# Run models
dbt run

# Run tests
dbt test
```

Open Snowflake Notebooks to execute ML workflows.
