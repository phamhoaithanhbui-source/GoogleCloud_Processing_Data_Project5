# Glamira dbt Project

## Overview

This dbt project transforms raw Glamira customer event data into a dimensional data warehouse model in BigQuery.

The project follows a layered architecture:

```text
Raw Layer
    ↓
Staging Layer
    ↓
Dimension Layer
    ↓
Fact Layer
```

The resulting mart supports:

* Revenue Analysis
* Product Performance Analysis
* Geographic Analysis
* Time-Based Trend Analysis

---

# Project Structure

```text
glamira_project/

├── models/
│   ├── staging/
│   ├── dimensions/
│   └── facts/
│
├── snapshots/
│
├── seeds/
│
├── tests/
│
├── macros/
│
├── dbt_project.yml
│
└── README.md
```

---

# Models

## Staging Models

Purpose:

* Clean raw data
* Standardize data types
* Prepare source tables for dimensional modeling

Examples:

* stg_checkout_success
* stg_cart_product
* stg_ip_location
* stg_product

---

## Dimension Models

### dim_customer

Slowly Changing Dimension Type 2.

Business Key:

```text
customer_id
```

Tracks:

* customer_id
* email_address
* valid_from
* valid_to
* is_current

---

### dim_product

Business Key:

```text
product_id
```

Tracks:

* product_name
* product_url
* final_url

---

### dim_date

Calendar dimension.

Tracks:

* year
* quarter
* month
* weekday

---

### dim_location

Tracks:

* country
* region
* city

---

### dim_device

Tracks:

* device_id
* user_agent
* screen_resolution

---

### dim_store

Tracks:

* store_id
* store_name
* store_domain

---

### dim_currency

Tracks:

* currency_code
* currency_name
* currency_symbol

---

# Fact Models

## fact_sales_order_detail

Grain:

```text
One row per product item in a completed order.
```

Measures:

* order_qty
* unit_price
* sales_amount
* unit_price_usd
* sales_amount_usd

---

## fact_exchange_rate

Grain:

```text
One row per currency per month.
```

Measures:

* exchange_rate_to_usd

Purpose:

Currency conversion support for global reporting.

---

# Snapshots

## dim_customer_snapshot

Implements:

```text
SCD Type 2
```

Tracks customer email changes over time.

Columns:

* dbt_valid_from
* dbt_valid_to

---

# Seeds

## exchange_rate_monthly

Stores monthly exchange rates.

Columns:

* rate_month
* currency_code
* exchange_rate_to_usd

Used to populate:

```text
fact_exchange_rate
```

---

# Data Governance

## PII Protection

Protected columns:

* email_address
* ip_address

Implemented using:

* BigQuery Policy Tags
* Dynamic Data Masking

Taxonomy:

```text
Glamira PII Classification
```

Policy Tags:

```text
email_pii
ip_pii
```

---

# Data Quality Tests

Implemented in dbt.

Dimension Tests:

* Primary Key Uniqueness
* Business Key Not Null

Fact Tests:

* Fact Key Uniqueness
* Foreign Key Validation

SCD Tests:

* One Current Record Per Customer

Grain Tests:

* One row per product item in completed order

---

# Build Commands

Run models:

```bash
dbt run
```

Run dimensions:

```bash
dbt run --select dimensions
```

Run facts:

```bash
dbt run --select facts
```

Run snapshot:

```bash
dbt snapshot
```

Run tests:

```bash
dbt test
```

Refresh sales fact:

```bash
dbt run --full-refresh --select fact_sales_order_detail
```

---

# Data Warehouse Design

Schema Type:

```text
Star Schema
```

Dimensions:

* dim_customer
* dim_product
* dim_date
* dim_location
* dim_device
* dim_store
* dim_currency

Facts:

* fact_sales_order_detail
* fact_exchange_rate

---

# Author

Bui Pham Hoai Thanh

Computer Science

Western Connecticut State University

