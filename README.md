## Context

You have built a complete Jaffle Shop transformation pipeline:

- **Sources** — raw customers, orders, and payments in DuckDB
- **Staging** — cleaned and typed models (`stg_customers`, `stg_orders`, `stg_payments`)
- **Intermediate** — business logic (`int_orders_with_payments`, `int_customer_order_summary`)
- **Marts** — analytics-ready output (`dim_customers`, `fct_orders`)

A pipeline that runs without errors can still produce silently wrong results. This challenge adds two professional layers of quality assurance:

1. **Data quality tests** — automated checks that catch bad data before it reaches dashboards
2. **Documentation** — descriptions for every model and column, plus a browsable lineage graph

These are the two practices that separate a production pipeline from a prototype.

## Objective

Add built-in dbt tests and descriptions across all four layers of your pipeline, then generate the documentation site.

**By the end of this challenge, you will be able to:**

- Write `unique`, `not_null`, `relationships`, and `accepted_values` tests in `schema.yml`
- Organise schema files by layer (one per folder)
- Run `dbt test` to validate data quality across the entire pipeline
- Write meaningful model and column descriptions
- Generate and browse the dbt documentation site with lineage graphs

## Prerequisites

A working `jaffle_shop_dbt/` directory containing all four layers:

- `models/staging/` — `stg_customers.sql`, `stg_orders.sql`, `stg_payments.sql`
- `models/intermediate/` — `int_orders_with_payments.sql`, `int_customer_order_summary.sql`
- `models/marts/` — `dim_customers.sql`, `fct_orders.sql`
- `dev.duckdb` symlink

## 0. Copy Your Work from the Previous Challenge

**📍 In your terminal**, navigate to this challenge directory and copy your project:

```bash
# Check the name of your previous challenge directory
ls ..

cp -rP ../../../{{ local_path_to("03-Data-Transformation/10-DBT-Advanced/01-Marts-Bridge") }}/jaffle_shop_dbt .

# Verify the symlink copied correctly
ls -l jaffle_shop_dbt/dev.duckdb
# Should show: dev.duckdb -> ../../../dbt-shared/dev.duckdb
```

**Verify all layers are present:**

```bash
ls jaffle_shop_dbt/models/staging/
ls jaffle_shop_dbt/models/intermediate/
ls jaffle_shop_dbt/models/marts/
```

---

## Part 1: Data Quality Tests

### Why Testing Matters

Tests ensure:

- **Primary keys are unique** — no duplicate records entering your marts
- **Required fields exist** — no null values in critical columns
- **Referential integrity** — foreign keys actually match
- **Business rules** — status values are valid, payment methods are recognised

dbt provides four built-in generic tests:

- `unique` — every value in the column is unique
- `not_null` — no nulls allowed
- `accepted_values` — values must come from a defined list
- `relationships` — a foreign key column must match a column in another model

You add these as a `tests:` list under each column in your `schema.yml` files.

### 1.1. Add Tests to Mart and Intermediate Models

**Your task:** Add tests to `models/intermediate/schema.yml` and `models/marts/schema.yml`.

**For `int_orders_with_payments`:**

- `order_id` — `unique`, `not_null`
- `customer_id` — `not_null`
- `status` — `not_null`, `accepted_values` (the 5 valid statuses)

**For `dim_customers`:**

- `customer_id` — `unique`, `not_null`

**For `fct_orders`:**

- `order_id` — `unique`, `not_null`
- `customer_id` — `not_null`, `relationships` (to `dim_customers`)

**📝 Open and update both schema files:**

```bash
code jaffle_shop_dbt/models/intermediate/schema.yml
code jaffle_shop_dbt/models/marts/schema.yml
```

<details>
<summary markdown="span">**💡 Hint: Test syntax for a column**</summary>

```yaml
columns:
  - name: order_id
    description: "Unique order identifier"
    tests:
      - unique
      - not_null
```

</details>

<details>
<summary markdown="span">**💡 Hint: accepted_values syntax**</summary>

```yaml
- name: status
  tests:
    - not_null
    - accepted_values:
        values: ['placed', '???', '???', '???', '???']
```

Check the Jaffle Shop data to find all valid status values:

```sql
SELECT DISTINCT status FROM main_intermediate.int_orders_with_payments;
```

</details>

<details>
<summary markdown="span">**💡 Hint: relationships (foreign key) syntax**</summary>

```yaml
- name: customer_id
  tests:
    - not_null
    - relationships:
        to: ref('dim_customers')
        field: customer_id
```

</details>

### 🧪 Checkpoint 1: Push Tests for Mart Models

**📍 In your terminal**, navigate to the challenge directory:

```bash
cd ..
```

Run the checkpoint 1 tests:

```bash
pytest tests/test_mart_tests.py -v
```

**If tests pass**, commit and push:

```bash
git add jaffle_shop_dbt/models/intermediate/schema.yml
git add jaffle_shop_dbt/models/marts/schema.yml
git commit -m "Add data quality tests to mart and intermediate models"
git push origin master
```

Mart and intermediate models now have `unique`, `not_null`, and `relationships` tests. Next you'll add the same coverage to staging — and reorganise the schema files so each layer has its own.

---

### 1.2. Add Tests to Staging Models

Staging models also need tests — catching bad source data early is better than letting it corrupt your marts.

**First, a reorganisation.** You documented your staging models in `models/schema.yml` earlier. Now that your project has four layers, move staging model documentation to its own file to keep things organised.

**Two steps:**

1. **Create** `models/staging/schema.yml` — move staging model docs here and add tests
2. **Remove** the `models:` entries from `models/schema.yml` — that file should contain only `sources:` definitions

> ⚠️ **Important:** If staging models are defined in both files, dbt will throw a duplicate model error.

**Minimum tests to add:**

- `stg_customers` — `customer_id`: `unique`, `not_null`
- `stg_orders` — `order_id`: `unique`, `not_null`
- `stg_orders` — `customer_id`: `not_null`
- `stg_payments` — `payment_id`: `unique`, `not_null`
- `stg_payments` — `payment_method`: `not_null`, `accepted_values` (4 valid methods)

**📝 Edit both files:**

```bash
# Create/edit the staging schema file
code jaffle_shop_dbt/models/staging/schema.yml

# Remove the staging models: section from the root schema file
code jaffle_shop_dbt/models/schema.yml
```

<details>
<summary markdown="span">**💡 Hint: Valid payment methods**</summary>

Check the Jaffle Shop data to find all payment methods:

```sql
SELECT DISTINCT payment_method FROM main_staging.stg_payments;
```

</details>

### 🧪 Checkpoint 2: Push Tests for Staging Models

**📍 In your terminal**, navigate to the challenge directory:

```bash
cd ..
```

Run the checkpoint 2 tests:

```bash
pytest tests/test_staging_tests.py -v
```

**If tests pass**, commit and push:

```bash
git add jaffle_shop_dbt/models/staging/schema.yml
git add jaffle_shop_dbt/models/schema.yml
git commit -m "Add data quality tests to staging models"
git push origin master
```

Every layer is now tested. The last step is to add descriptions to every model and column so the docs site has something to display.

---

### 1.3. Run All Tests

**🗄️ In DBeaver**, disconnect from your database first to avoid locks.

**📍 In your terminal:**

```bash
cd jaffle_shop_dbt
dbt test
```

<details>
<summary markdown="span">**Expected output** (may vary based on how many tests you added)</summary>

```plaintext
Done. PASS=22 WARN=0 ERROR=0 SKIP=0 TOTAL=22
```

</details>

You can also test a single layer:

```bash
dbt test --select 'staging.*'
dbt test --select 'marts.*'
```

---

## Part 2: Documentation

### Why Documentation Matters

Good descriptions explain *what* the data represents and *why* the calculation exists — not just what the column is named. A useful model description answers:

- **What is this model?** (grain: one row = one ?)
- **What business logic does it contain?** (transformations, aggregations)
- **Who uses it?** (dashboards, analysts, stakeholders)

### 2.1. Add Descriptions to Your Models

**Requirements:**

- Every model (`stg_*`, `int_*`, `dim_*`, `fct_*`) must have a model-level description
- Every column in each model must have a description
- Calculated columns must explain the formula or business rule (e.g. `lifetime_value`)

**📝 Update your schema files:**

```bash
code jaffle_shop_dbt/models/staging/schema.yml
code jaffle_shop_dbt/models/intermediate/schema.yml
code jaffle_shop_dbt/models/marts/schema.yml
```

<details>
<summary markdown="span">**💡 Hint: Example model description**</summary>

```yaml
- name: dim_customers
  description: >
    Customer dimension with lifetime order metrics.
    One row per customer. Aggregates order history from
    int_orders_with_payments. Used for customer segmentation
    and lifetime value reporting.
  columns:
    - name: customer_id
      description: "Unique customer identifier"
      tests:
        - unique
        - not_null
    - name: lifetime_value
      description: "Total amount paid across all orders. Sourced from int_orders_with_payments.total_amount (already in dollars)."
```

</details>

### 🧪 Checkpoint 3: Push Documentation

**📍 In your terminal**, navigate to the challenge directory:

```bash
cd ..
```

Run the checkpoint 3 tests:

```bash
pytest tests/test_documentation.py -v
```

**If tests pass**, commit and push:

```bash
git add jaffle_shop_dbt/models/
git commit -m "Add documentation to all pipeline models"
git push origin master
```

Tests and descriptions are in place across the whole pipeline. The final step is to generate the docs site and explore the lineage graph — the visual payoff for all the YAML work you just did.

---

## Part 3: Generate the Documentation Site

### 3.1. Generate and Serve Docs

```bash
cd jaffle_shop_dbt
dbt docs generate
dbt docs serve
```

Navigate to [http://localhost:8080](http://localhost:8080) in your browser.

<details>
<summary markdown="span">**⚠️ Port already in use? Try this**</summary>

If you see an error like `OSError: [Errno 48] Address already in use`, port 8080 is occupied. Run:

```bash
dbt docs serve --port 8081
```

Then navigate to [http://localhost:8081](http://localhost:8081) instead. If that port is also taken, try 8082, 8083, etc.

To find and stop the process using port 8080:

```bash
lsof -i :8080
kill <PID>   # replace <PID> with the process ID shown above
```

</details>

### 3.2. Explore the Lineage Graph

Find any mart model (`dim_customers` or `fct_orders`) in the left-hand panel. Click the blue lineage icon to open the DAG view.

This graph shows every source and model in your project, with arrows representing `ref()` and `source()` dependencies. It's a visual map of your entire pipeline.

![dbt lineage graph showing sources flowing through staging, intermediate, and mart layers to dim_customers and fct_orders](https://wagon-public-datasets.s3.amazonaws.com/data-analytics/03-Data-Transformation/10-DBT-Advanced/lineage-graph-marts.png)

**⚠️ Before continuing:** Press `Ctrl+C` in your terminal to stop the docs server.

### 🧪 Final Checkpoint: Push Complete Pipeline

**📍 In your terminal**, navigate to the challenge directory:

```bash
cd ..
```

Run all tests:

```bash
make
```

**If all tests pass**, commit and push your final state:

```bash
git add jaffle_shop_dbt/
git commit -m "Complete tested and documented Jaffle Shop pipeline"
git push origin master
```

---

Your pipeline is tested, documented, and the lineage graph is live.

## 🎉 Challenge Complete

Your Jaffle Shop pipeline is now production-quality: tested, documented, and visualised.

**Key takeaways:**

- Test primary keys on every model — `unique` + `not_null` catches data quality issues before they reach dashboards
- `relationships` tests enforce referential integrity across the pipeline
- `dbt docs` generates a live data dictionary from your code — descriptions, column docs, and the full lineage graph
