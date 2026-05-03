-- ============================================================
-- E-Commerce Real-Time Pipeline — PostgreSQL Schema
-- ============================================================

CREATE TABLE IF NOT EXISTS orders (
    id             SERIAL PRIMARY KEY,
    order_id       UUID UNIQUE NOT NULL,
    customer_id    VARCHAR(64),
    product        VARCHAR(128),
    category       VARCHAR(64),
    quantity       INTEGER,
    unit_price     NUMERIC(10, 2),
    total_amount   NUMERIC(10, 2),
    payment_method VARCHAR(64),
    status         VARCHAR(32),
    region         VARCHAR(64),
    event_time     TIMESTAMP,
    ingested_at    TIMESTAMP DEFAULT NOW()
);

-- Index for time-series queries
CREATE INDEX IF NOT EXISTS idx_orders_event_time  ON orders (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_orders_category    ON orders (category);
CREATE INDEX IF NOT EXISTS idx_orders_status      ON orders (status);
CREATE INDEX IF NOT EXISTS idx_orders_region      ON orders (region);

-- ── Materialized view: revenue by category ───────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_revenue_by_category AS
SELECT
    category,
    COUNT(*)                    AS total_orders,
    SUM(total_amount)           AS total_revenue,
    AVG(total_amount)           AS avg_order_value,
    SUM(quantity)               AS total_units_sold
FROM orders
GROUP BY category;

-- ── Materialized view: revenue by region ────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_revenue_by_region AS
SELECT
    region,
    COUNT(*)          AS total_orders,
    SUM(total_amount) AS total_revenue
FROM orders
GROUP BY region;

-- ── View: orders per minute (live feed) ─────────────────────────────────────
CREATE OR REPLACE VIEW vw_orders_per_minute AS
SELECT
    date_trunc('minute', event_time) AS minute,
    COUNT(*)                         AS order_count,
    SUM(total_amount)                AS revenue
FROM orders
WHERE event_time >= NOW() - INTERVAL '1 hour'
GROUP BY 1
ORDER BY 1 DESC;

-- ── View: status breakdown ───────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_status_breakdown AS
SELECT
    status,
    COUNT(*)                                        AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM orders
GROUP BY status
ORDER BY count DESC;

-- ── View: payment method breakdown ──────────────────────────────────────────
CREATE OR REPLACE VIEW vw_payment_methods AS
SELECT
    payment_method,
    COUNT(*)          AS total_transactions,
    SUM(total_amount) AS total_revenue
FROM orders
GROUP BY payment_method
ORDER BY total_transactions DESC;

-- ── View: top products ───────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_top_products AS
SELECT
    product,
    category,
    COUNT(*)          AS times_ordered,
    SUM(quantity)     AS total_units,
    SUM(total_amount) AS total_revenue
FROM orders
GROUP BY product, category
ORDER BY total_revenue DESC
LIMIT 20;

-- ── View: recent orders (live table) ────────────────────────────────────────
CREATE OR REPLACE VIEW vw_recent_orders AS
SELECT
    order_id,
    customer_id,
    product,
    category,
    quantity,
    total_amount,
    payment_method,
    status,
    region,
    event_time
FROM orders
ORDER BY event_time DESC
LIMIT 50;
