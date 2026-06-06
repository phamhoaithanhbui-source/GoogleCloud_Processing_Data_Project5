

## Project Overview

This project builds an end-to-end cloud-based Data Warehouse solution for analyzing customer behavior, product performance, and sales activities from the Glamira e-commerce platform.

The solution follows a modern ELT architecture implemented on Google Cloud Platform and transforms raw event data into a dimensional data warehouse optimized for business intelligence and reporting.

The project supports:

* Revenue Analysis
* Geographic Distribution Analysis
* Product Performance Analysis
* Time-Based Trend Analysis
* Data Governance and PII Protection

---

# Business Problem

Glamira generates millions of customer interaction events across multiple international online stores.

The raw event data is difficult to analyze because:

* Data is stored as semi-structured MongoDB documents.
* Customer activities are spread across multiple event types.
* Product information is nested inside JSON arrays.
* Revenue is recorded in multiple currencies.
* Sensitive customer information requires governance controls.

The objective of this project is to transform raw operational data into a centralized analytics platform that enables business reporting and decision-making.

---

# Architecture

## End-to-End Data Flow

```text
MongoDB
   ↓
Python Extraction Scripts
   ↓
Google Cloud Storage (Data Lake)
   ↓
BigQuery Raw Layer
   ↓
BigQuery Staging Layer
   ↓
dbt Transformations
   ↓
BigQuery Mart Layer
   ↓
Looker Studio Dashboard
```

---

# Technology Stack

| Layer                | Technology                   |
| -------------------- | ---------------------------- |
| Source System        | MongoDB                      |
| Programming Language | Python                       |
| Cloud Platform       | Google Cloud Platform        |
| Data Lake            | Google Cloud Storage         |
| Data Warehouse       | BigQuery                     |
| Transformation       | dbt                          |
| Data Quality         | dbt Tests + dbt_expectations |
| Data Governance      | BigQuery Policy Tags         |
| Reporting            | Looker Studio                |

---

# Source Dataset

Dataset:

```text
glamira_ubl_oct2019_nov2019
```

MongoDB Database:

```text
countly
```

Collection:

```text
summary
```

The dataset contains millions of customer activity events including:

* Product Views
* Product Detail Views
* Recommendation Clicks
* Product Option Selection
* Cart Activities
* Successful Checkouts

---

# Data Extraction

Raw MongoDB documents were exported into JSONL format using Python.

Key challenges:

* Large dataset volume (40+ million records)
* MongoDB document size limitations
* Nested JSON structures

Important nested fields:

```text
option_json
cart_products_json
```

These fields required additional processing and flattening before loading into BigQuery.

---

# Data Lake

Google Cloud Storage was used as the landing zone for exported datasets.

Storage format:

```text
JSONL
```

Benefits:

* Scalable storage
* Decouples extraction from loading
* Supports batch ingestion

---

# BigQuery Layers

## Raw Layer

Dataset:

```text
glamira_dataset_raw
```

Purpose:

* Preserve original source data
* No business logic applied

---

## Staging Layer

Dataset:

```text
glamira_dataset_raw_staging
```

Purpose:

* Data cleansing
* Data type standardization
* JSON flattening

Examples:

```text
stg_checkout_success
stg_cart_product
stg_ip_location
```

---

## Mart Layer

Dataset:

```text
glamira_dataset_raw_mart
```

Purpose:

* Dimensional modeling
* Business reporting
* Dashboard consumption

---

# Dimensional Model

The warehouse follows a Star Schema design.

## Dimensions

### dim_customer

Customer dimension implemented using SCD Type 2.

Attributes:

* customer_key
* customer_id
* email_address
* valid_from
* valid_to
* is_current

---

### dim_product

Attributes:

* product_key
* product_id
* product_name

---

### dim_date

Attributes:

* date_key
* full_date
* year_number
* quarter_number
* month_number

---

### dim_location

Attributes:

* location_key
* country_name
* region_name
* city_name

---

### dim_device

Attributes:

* device_key
* device_id
* user_agent
* screen_resolution

---

### dim_store

Attributes:

* store_key
* store_id
* store_name
* store_domain

---

### dim_currency

Attributes:

* currency_key
* currency_code

---

# Fact Tables

## fact_sales_order_detail

Grain:

```text
One row per product item in a completed customer order.
```

Measures:

* order_qty
* unit_price
* sales_amount
* sales_amount_usd

Foreign Keys:

* customer_key
* product_key
* date_key
* location_key
* device_key
* store_key
* currency_key

---

## fact_exchange_rate

Grain:

```text
One row per currency per month.
```

Measure:

```text
exchange_rate_to_usd
```

Purpose:

Currency conversion for global revenue reporting.

---

# Slowly Changing Dimension Type 2

The customer dimension tracks historical changes.

Columns:

```text
valid_from
valid_to
is_current
```

Benefits:

* Historical reporting
* Customer profile tracking
* Auditability

---

# Data Quality

Data quality validation was implemented using:

* dbt built-in tests
* dbt_expectations package

Validation Categories:

### Primary Key Tests

* Unique
* Not Null

### Referential Integrity

Relationships between facts and dimensions.

### Fact Grain Validation

Validated:

```text
sales_order_detail_key
```

to ensure no duplicate transaction rows.

### Business Rules

Examples:

* Positive revenue amounts
* Valid exchange rates
* Valid month and quarter values

---

# Data Governance

Sensitive customer information is protected using BigQuery Data Governance features.

Protected Columns:

```text
email_address
ip_address
```

Implemented Using:

* Data Catalog Taxonomy
* Policy Tags
* Dynamic Data Masking

Access Model:

| User Type     | Access              |
| ------------- | ------------------- |
| Admin         | View original value |
| Standard User | View masked value   |

Example:

```text
Admin:
johnsmith@gmail.com

Viewer:
jo****@gmail.com
```

---

# Dashboard Development

Looker Studio was used to build business dashboards.


<img width="1002" height="747" alt="image" src="https://github.com/user-attachments/assets/3f5f15c8-a4cf-4751-9427-9ea2504bd47e" />




## Executive Summary

KPIs:

* Total Revenue
* Total Orders
* Total Customers
* Total Products Sold

---

## Revenue Analysis

Visualizations:

* Revenue Trend
* Revenue by Store
* Revenue by Country

---

## Geographic Distribution

Visualizations:

* Revenue by Country
* Revenue by Region
* Customer Distribution

---

## Product Performance

Visualizations:

* Top Products by Revenue
* Top Products by Quantity Sold

---

# Key Challenges Solved

## Handling Nested JSON

Flattened:

```text
option_json
cart_products_json
```

into relational structures.

---

## Large-Scale Processing

Processed millions of MongoDB documents while avoiding memory limitations.

---

## Duplicate Fact Rows

Identified and fixed many-to-many joins caused by:

```text
dim_device
dim_store
```

which originally multiplied fact rows.

---

## Data Governance

Implemented column-level protection for customer PII.

---



# Author

Bui Pham Hoai Thanh

