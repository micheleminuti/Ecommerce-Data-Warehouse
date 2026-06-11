# 🛒 E-Commerce Data Warehouse — dbt + DuckDB

A fully functional **analytical Data Warehouse** built on top of a real-world e-commerce dataset. The project covers the complete data lifecycle: source profiling, ODS cleaning, dimensional modeling (Kimball), incremental loading, and business dashboards in Tableau.

---

## 📌 Project Overview

| Item | Detail |
|---|---|
| **Domain** | E-commerce retail (Indonesian market) |
| **Source data** | [Kaggle — Transactional E-Commerce Dataset](https://www.kaggle.com/datasets/bytadit/transactional-ecommerce) |
| **Records processed** | ~853,000 transactions, ~100,000 customers, ~44,000 products |
| **Stack** | DuckDB · dbt · Python · Tableau |
| **Modeling approach** | Kimball Dimensional Modeling — Star Schema |
| **SCD strategy** | Type 2 (customers & products) via dbt snapshots |
| **Fact table update** | Incremental — delete + insert |

---

## 🏗️ Architecture

```
Raw CSV Files (Kaggle)
        │
        ▼
┌─────────────────────┐
│   Staging (seeds)   │  ← Raw CSVs loaded via dbt seed
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│   ODS Layer (dbt)   │  ← Cleaning, casting, JSON flattening, standardization
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│  Snapshots (SCD2)   │  ← dbt snapshots for customer & product history
└─────────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────┐
│              Marts Layer (Star Schema)           │
│                                                  │
│   fact_sales   ──►  dim_customer (SCD2)          │
│                ──►  dim_product (SCD2)           │
│                ──►  dim_date                     │
│                ──►  dim_device                   │
│                ──►  dim_promo                    │
│                ──►  dim_payment                  │
└──────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────┐
│  Tableau Dashboards │  ← CSV export via export_for_tableau.py
└─────────────────────┘
```

---

## 📂 Repository Structure

```
dwh_ecommerce/
├── models/
│   ├── ods/
│   │   ├── product_ods.sql          # Products: trim, cast, filter nulls
│   │   ├── customer_ods.sql         # Customers: standardize, type-cast dates
│   │   └── transaction_ods.sql      # Transactions: JSON unnesting, EUR conversion
│   └── marts/
│       ├── dim_customer.sql         # SCD2 customer dimension
│       ├── dim_product.sql          # SCD2 product dimension
│       ├── dim_date.sql             # Date spine (hourly, 1950–2025)
│       ├── dim_device.sql           # Device type & version
│       ├── dim_promo.sql            # Promo codes
│       ├── dim_payment.sql          # Payment method & status
│       └── fact_sales.sql           # Incremental fact table
├── snapshots/
│   ├── customer_snapshot.sql        # dbt SCD2 snapshot for customers
│   └── product_snapshot.sql        # dbt SCD2 snapshot for products
├── seeds/
│   ├── customers.csv
│   ├── products.csv
│   └── transactions.csv
├── macros/
├── export_tableau_csv/              # Pre-built CSVs for Tableau
├── export_for_tableau.py            # Python export script
├── dbt_project.yml
└── profiles.yml
```

---

## 🔧 Tech Stack

| Tool | Role |
|---|---|
| **DuckDB** | In-process analytical engine (vectorized execution, no server needed) |
| **dbt-core + dbt-duckdb** | SQL transformation orchestration, DAG management, testing |
| **Python** | Export automation (`export_for_tableau.py`) |
| **Tableau** | Business Intelligence & interactive dashboards |

---

## 🗃️ Data Sources

Three CSV datasets sourced from Kaggle:

| File | Description | Key | ~Records |
|---|---|---|---|
| `products.csv` | Product catalog with category, colour, season, etc. | `id` | 44,000 |
| `customers.csv` | Customer registry with demographics and device info | `customer_id` | 100,000 |
| `transactions.csv` | Order history; includes a nested JSON field `product_metadata` | `booking_id` | 853,000 |

The main challenge in `transactions.csv` is the `product_metadata` column — a semi-structured JSON array that is parsed, flattened, and unnested during the ODS transformation to produce one row per product per order.

---

## 🧱 Dimensional Model

### Fact Table — `fact_sales`

Grain: **one row per product line within a transaction** (`booking_id` + `product_id`).

| Measure | Type | Description |
|---|---|---|
| `quantity` | Additive | Units purchased |
| `total_amount_eur` | Additive | Revenue in EUR |
| `total_amount_idr` | Additive | Revenue in IDR |
| `shipment_fee_eur` | Additive | Shipping cost in EUR |
| `shipment_fee_idr` | Additive | Shipping cost in IDR |
| `promo_amount_eur` | Additive | Discount applied in EUR |
| `promo_amount_idr` | Additive | Discount applied in IDR |

Foreign keys: `fk_customer`, `fk_product`, `fk_date`, `fk_device`, `fk_promo`, `fk_payment`, `fk_shipment_date_limit`.

### Dimensions

| Dimension | SCD | Description |
|---|---|---|
| `dim_customer` | Type 2 | Customer registry with historical tracking |
| `dim_product` | Type 2 | Product catalog with historical tracking |
| `dim_date` | Static | Hourly date spine with year, month, hour, day name, is_weekend |
| `dim_device` | Type 1 | Device type + OS version |
| `dim_promo` | Type 1 | Promo codes (nulls replaced with `NO CODE`) |
| `dim_payment` | Type 1 | Payment method + success flag |

---

## ⚙️ ODS Transformations

### Products
- Filter records with null `id` or blank `productDisplayName`
- `TRIM` all text fields, `UPPER()` on `gender`
- Cast `id → BIGINT`, `year → INTEGER`

### Customers
- Filter records with null `customer_id` or empty `email`
- Keep only valid `gender` values (`M` / `F`)
- `LOWER()` on `email`, `UPPER()` on `gender`, `TRIM` on name fields
- Cast `birthdate` and `first_join_date → DATE`
- Drop irrelevant columns: `device_id`, `home_location_lat`, `home_location_long`

### Transactions
- Replace single quotes in `product_metadata` to make it valid JSON, then `unnest` into one row per product
- Map `payment_status = 'SUCCESS' → TRUE` (boolean `is_payment_success`)
- Replace null/empty `promo_code` with `'NO CODE'`
- Cast `promo_amount`, `shipment_fee → DOUBLE`
- Compute `total_amount_eur = quantity * item_price / 17000`
- Retain `created_at` and `shipment_date_limit` as `TIMESTAMP`

---

## 📈 Incremental Strategy

`fact_sales` uses `incremental_strategy = 'delete+insert'` with a composite `unique_key` across all foreign keys. On each run, the pipeline reads the maximum `fk_date` already present in the table and processes only newer records, keeping pipeline runs fast as data grows.

---

## 📊 Business Dashboards (Tableau)

Four dashboards were built on top of the exported mart CSVs:

| Dashboard | Key Question |
|---|---|
| **Revenue & Trends** | How has revenue evolved over time? When do customers buy most? |
| **Regional Sales** | Which Indonesian regions drive the most transactions? Where are promotions most used? |
| **Product Performance** | Which categories and products lead sales? How does demand shift by season? |
| **Customer Profile** | What are the demographics and payment preferences of the customer base? |

**Selected findings:**
- Revenue grew steadily from 2016 to a peak in 2021 (~€7.85M), then contracted in 2022
- **Jakarta Raya, Jawa Barat, and Jawa Tengah** account for the majority of transactions
- **T-Shirts** are the best-selling article type by volume, with demand peaking in summer
- ~77% of customers use Android; younger users favour digital wallets (OVO, LinkAja) while older users prefer credit cards
- Weekend evenings are the highest-engagement time window for purchases

---

## 🚀 Quickstart

**Prerequisites:** Python 3.8+, pip

```bash
# 1. Clone the repository
git clone https://github.com/<your-username>/dwh_ecommerce.git
cd dwh_ecommerce

# 2. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install dbt-core dbt-duckdb

# 4. Configure your profile (update dwh_ecommerce/profiles.yml if needed)
# Default uses a local DuckDB file: dev.duckdb

# 5. Install dbt packages and run the full pipeline
cd dwh_ecommerce
dbt deps
dbt seed          # Load raw CSVs
dbt snapshot      # Run SCD2 snapshots
dbt run           # Build ODS + mart models
dbt test          # Run data quality tests

# 6. (Optional) Export CSVs for Tableau
python export_for_tableau.py

# 7. (Optional) Browse dbt docs
dbt docs generate
dbt docs serve
```

---

## 🧪 Testing

dbt tests are defined in `schema.yml` files and cover:
- Primary key uniqueness on all dimension surrogate keys
- Not-null constraints on foreign keys in `fact_sales`
- Referential integrity between fact and dimensions
- Accepted value checks (e.g. `gender` ∈ {`M`, `F`})

Run with:
```bash
dbt test
```

---

## 🗺️ dbt DAG

The full lineage graph — from raw seeds through ODS models and snapshots to the final mart — is auto-generated by dbt and can be explored with `dbt docs serve`.

```
products  ──► product_ods  ──► product_snapshot  ──► dim_product ──┐
customers ──► customer_ods ──► customer_snapshot ──► dim_customer ─┤
                           └──► dim_device                         ├──► fact_sales
transactions ──► transaction_ods ──► dim_promo                     │
                               └──► dim_payment ───────────────────┘
dim_date ──────────────────────────────────────────────────────────┘
```

---

## 📝 Notes

- Currency conversion uses a fixed rate: `1 EUR = 17,000 IDR`
- The `dim_date` spine covers 1950–2025 at hourly granularity; `fact_sales` joins on `date_trunc('hour', created_at)`
- Geographic visualizations in Tableau use an external GeoJSON file ([superpikar/indonesia-geojson](https://github.com/superpikar/indonesia-geojson)) that is **not** stored in the warehouse — keeping geometry data out of the DW is intentional

---

## 📄 License

See `LICENSE` in the repository root.

---

## 🙋 Contact

Questions or suggestions? Open an issue or reach out via GitHub.