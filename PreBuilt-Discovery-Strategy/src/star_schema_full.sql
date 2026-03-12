CREATE SCHEMA IF NOT EXISTS newmodel3;

CREATE TABLE IF NOT EXISTS newmodel3.dim_individual (
    individual_key           BIGINT IDENTITY(1,1) PRIMARY KEY,
    individual_id            VARCHAR(36),
    individual_last_name     VARCHAR(255),
    individual_first_name    VARCHAR(255),
    individual_job_function  VARCHAR(255),
    individual_job_level     VARCHAR(255),
    individual_status        VARCHAR(50),
    individual_company_key   BIGINT,
    individual_geography_key BIGINT
    -- TODO: enable FK to dim_company, dim_geography after validation
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_company (
    company_key           BIGINT IDENTITY(1,1) PRIMARY KEY,
    company_id            VARCHAR(36),
    company_name          VARCHAR(255),
    company_type          VARCHAR(100),
    company_busines_area  VARCHAR(255),
    company_status        VARCHAR(50),
    company_geography_key BIGINT
    -- TODO: enable FK to dim_geography after validation
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_membership_type (
    membership_type          VARCHAR(100),
    membership_type_name     VARCHAR(255),
    membership_type_category VARCHAR(100),
    membership_type_entity   VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_memberships (
    memberships_key          BIGINT IDENTITY(1,1) PRIMARY KEY,
    membership_status        VARCHAR(50),
    membership_lifecycle     VARCHAR(100),
    membership_lifecycle_next VARCHAR(100),
    membership_entity        VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_chapter (
    chapter_key      BIGINT IDENTITY(1,1) PRIMARY KEY,
    chapter_code     VARCHAR(100),
    chapter_name     VARCHAR(255),
    chapter_type     VARCHAR(100),
    chapter_status   VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_geography (
    geography_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    "See current" VARCHAR(255)
    -- TODO: clarify geography attributes and rename column
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_date (
    date_key     INTEGER PRIMARY KEY,
    "See current" VARCHAR(255)
    -- TODO: replace placeholder column with proper calendar attributes
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_month (
    month_key    INTEGER PRIMARY KEY,
    "See current" VARCHAR(255)
    -- TODO: replace placeholder column with proper month attributes
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_year (
    year_key   INTEGER PRIMARY KEY,
    "New, TBD" VARCHAR(255)
    -- TODO: replace placeholder column with proper year attributes
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_product (
    product_key   BIGINT IDENTITY(1,1) PRIMARY KEY,
    product_code  VARCHAR(100),
    product_name  VARCHAR(255),
    product_type  VARCHAR(100),
    product_status VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_event (
    event_key       BIGINT IDENTITY(1,1) PRIMARY KEY,
    event_code      VARCHAR(100),
    event_name      VARCHAR(255),
    event_type      VARCHAR(100),
    event_status    VARCHAR(50),
    event_location  VARCHAR(255),
    event_start_date DATE,
    event_end_date   DATE
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_location (
    location_key   BIGINT IDENTITY(1,1) PRIMARY KEY,
    location_code  VARCHAR(100),
    location_name  VARCHAR(255),
    location_type  VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_time (
    time_key   INTEGER PRIMARY KEY,
    "See current" VARCHAR(255)
    -- TODO: replace placeholder column with proper time attributes
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_sales_lines (
    sales_lines_key   BIGINT IDENTITY(1,1) PRIMARY KEY,
    sales_line_status VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_sales_orders (
    sales_orders_key   BIGINT IDENTITY(1,1) PRIMARY KEY,
    sales_order_status VARCHAR(100),
    sales_order_entity VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS newmodel3.fact_event_registrations (
    registration_quantity   NUMERIC(18,4),
    registration_amount     NUMERIC(18,4),
    event_distance          NUMERIC(18,4),
    event_registration_date DATE,
    event_start_date        DATE,
    event_key               BIGINT,
    product_key             BIGINT,
    individual_key          BIGINT,
    company_key             BIGINT,
    geography_key           BIGINT,
    event_geography_key     BIGINT,
    event_registrations_key BIGINT
    -- TODO: add FK constraints after validation
);

CREATE TABLE IF NOT EXISTS newmodel3.fact_event_sessions (
    session_amount           NUMERIC(18,4),
    event_distance           NUMERIC(18,4),
    event_registration_date  DATE,
    session_registration_date DATE,
    session_start_date       DATE,
    session_end_date         DATE,
    session_start_time       VARCHAR(50),
    session_end_time         VARCHAR(50),
    session_key              BIGINT,
    event_key                BIGINT,
    product_key              BIGINT,
    location_key             BIGINT,
    individual_key           BIGINT,
    company_key              BIGINT,
    geography_key            BIGINT,
    event_geography_key      BIGINT,
    event_registration_key   BIGINT,
    event_sessions_key       BIGINT
    -- TODO: add FK constraints after validation
);

CREATE TABLE IF NOT EXISTS newmodel3.fact_event_purchases (
    purchase_quantity       NUMERIC(18,4),
    purchase_amount         NUMERIC(18,4),
    purchase_date           DATE,
    event_registration_date DATE,
    event_distance          NUMERIC(18,4),
    event_key               BIGINT,
    product_key             BIGINT,
    individual_key          BIGINT,
    company_key             BIGINT,
    geography_key           BIGINT,
    event_geography_key     BIGINT,
    event_registration_key  BIGINT,
    event_sessions_key      BIGINT
    -- TODO: add FK constraints after validation
);

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Events & Registrations]
-- [TYPE   : fact]
-- [TARGET : newmodel3.fact_event_registrations]
-- [SOURCES: source.ams_rem_shopping_event]  -- TODO: confirm full source set
-- [GRAIN  : TODO: confirm grain for event registrations]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel3.fact_event_registrations
(
    registration_quantity,
    registration_amount,
    event_distance,
    event_registration_date,
    event_start_date,
    event_key,
    product_key,
    individual_key,
    company_key,
    geography_key,
    event_geography_key,
    event_registrations_key
)
SELECT
    NULL::NUMERIC(18,4) AS registration_quantity,    -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS registration_amount,      -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS event_distance,           -- TODO: no source mapping identified
    NULL::DATE          AS event_registration_date,  -- TODO: no source mapping identified
    NULL::DATE          AS event_start_date,         -- TODO: no source mapping identified
    NULL::BIGINT        AS event_key,                -- TODO: natural key not resolved to dim_event
    NULL::BIGINT        AS product_key,              -- TODO: natural key not resolved to dim_product
    NULL::BIGINT        AS individual_key,           -- TODO: natural key not resolved to dim_individual
    NULL::BIGINT        AS company_key,              -- TODO: natural key not resolved to dim_company
    NULL::BIGINT        AS geography_key,            -- TODO: natural key not resolved to dim_geography
    NULL::BIGINT        AS event_geography_key,      -- TODO: natural key not resolved to geography
    NULL::BIGINT        AS event_registrations_key   -- TODO: surrogate/business key not resolved
WHERE 1 = 0;  -- prevent empty insert from running until source is confirmed

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Events & Registrations]
-- [TYPE   : fact]
-- [TARGET : newmodel3.fact_event_sessions]
-- [SOURCES: source.ams_rem_shopping_session]  -- TODO: confirm full source set
-- [GRAIN  : TODO: confirm grain for event sessions]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel3.fact_event_sessions
(
    session_amount,
    event_distance,
    event_registration_date,
    session_registration_date,
    session_start_date,
    session_end_date,
    session_start_time,
    session_end_time,
    session_key,
    event_key,
    product_key,
    location_key,
    individual_key,
    company_key,
    geography_key,
    event_geography_key,
    event_registration_key,
    event_sessions_key
)
SELECT
    NULL::NUMERIC(18,4) AS session_amount,           -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS event_distance,           -- TODO: no source mapping identified
    NULL::DATE          AS event_registration_date,  -- TODO: no source mapping identified
    NULL::DATE          AS session_registration_date,-- TODO: no source mapping identified
    NULL::DATE          AS session_start_date,       -- TODO: no source mapping identified
    NULL::DATE          AS session_end_date,         -- TODO: no source mapping identified
    NULL::VARCHAR(50)   AS session_start_time,       -- TODO: no source mapping identified
    NULL::VARCHAR(50)   AS session_end_time,         -- TODO: no source mapping identified
    NULL::BIGINT        AS session_key,              -- TODO: natural key not resolved to session dimension
    NULL::BIGINT        AS event_key,                -- TODO: natural key not resolved to dim_event
    NULL::BIGINT        AS product_key,              -- TODO: natural key not resolved to dim_product
    NULL::BIGINT        AS location_key,             -- TODO: natural key not resolved to dim_location
    NULL::BIGINT        AS individual_key,           -- TODO: natural key not resolved to dim_individual
    NULL::BIGINT        AS company_key,              -- TODO: natural key not resolved to dim_company
    NULL::BIGINT        AS geography_key,            -- TODO: natural key not resolved to dim_geography
    NULL::BIGINT        AS event_geography_key,      -- TODO: natural key not resolved to geography
    NULL::BIGINT        AS event_registration_key,   -- TODO: natural key not resolved to registrations
    NULL::BIGINT        AS event_sessions_key        -- TODO: surrogate/business key not resolved
WHERE 1 = 0;  -- prevent empty insert from running until source is confirmed

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Events & Registrations]
-- [TYPE   : fact]
-- [TARGET : newmodel3.fact_event_purchases]
-- [SOURCES: source.ams_rem_purchase_registrationpurchase]  -- TODO: confirm full source set
-- [GRAIN  : TODO: confirm grain for event purchases]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel3.fact_event_purchases
(
    purchase_quantity,
    purchase_amount,
    purchase_date,
    event_registration_date,
    event_distance,
    event_key,
    product_key,
    individual_key,
    company_key,
    geography_key,
    event_geography_key,
    event_registration_key,
    event_sessions_key
)
SELECT
    NULL::NUMERIC(18,4) AS purchase_quantity,        -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS purchase_amount,          -- TODO: no source mapping identified
    NULL::DATE          AS purchase_date,            -- TODO: no source mapping identified
    NULL::DATE          AS event_registration_date,  -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS event_distance,           -- TODO: no source mapping identified
    NULL::BIGINT        AS event_key,                -- TODO: natural key not resolved to dim_event
    NULL::BIGINT        AS product_key,              -- TODO: natural key not resolved to dim_product
    NULL::BIGINT        AS individual_key,           -- TODO: natural key not resolved to dim_individual
    NULL::BIGINT        AS company_key,              -- TODO: natural key not resolved to dim_company
    NULL::BIGINT        AS geography_key,            -- TODO: natural key not resolved to dim_geography
    NULL::BIGINT        AS event_geography_key,      -- TODO: natural key not resolved to geography
    NULL::BIGINT        AS event_registration_key,   -- TODO: natural key not resolved to registrations
    NULL::BIGINT        AS event_sessions_key        -- TODO: natural key not resolved to sessions
WHERE 1 = 0;  -- prevent empty insert from running until source is confirmed

CREATE TABLE IF NOT EXISTS newmodel3.fact_sales_lines (
    sales_line_quantity NUMERIC(18,4),
    sales_line_amount   NUMERIC(18,4),
    sales_order_date    DATE,
    product_key         BIGINT,
    individual_key      BIGINT,
    company_key         BIGINT,
    geography_key       BIGINT,
    sales_orders_key    BIGINT,
    sales_lines_key     BIGINT
    -- TODO: add FK constraints after validation
);

CREATE TABLE IF NOT EXISTS newmodel3.fact_sales_orders (
    sales_orders_quantity NUMERIC(18,4),
    sales_orders_amount   NUMERIC(18,4),
    sales_order_date      DATE,
    individual_key        BIGINT,
    company_key           BIGINT,
    geography_key         BIGINT,
    sales_orders_key      BIGINT
    -- TODO: add FK constraints after validation
);

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Commerce & Sales Orders]
-- [TYPE   : fact]
-- [TARGET : newmodel3.fact_sales_orders]
-- [SOURCES: source.ams_rem_accounting_orders]  -- TODO: confirm full source set
-- [GRAIN  : TODO: confirm grain for sales orders]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel3.fact_sales_orders
(
    sales_orders_quantity,
    sales_orders_amount,
    sales_order_date,
    individual_key,
    company_key,
    geography_key,
    sales_orders_key
)
SELECT
    NULL::NUMERIC(18,4) AS sales_orders_quantity,   -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS sales_orders_amount,     -- TODO: no source mapping identified
    NULL::DATE          AS sales_order_date,        -- TODO: no source mapping identified
    NULL::BIGINT        AS individual_key,          -- TODO: natural key not resolved to dim_individual
    NULL::BIGINT        AS company_key,             -- TODO: natural key not resolved to dim_company
    NULL::BIGINT        AS geography_key,           -- TODO: natural key not resolved to dim_geography
    NULL::BIGINT        AS sales_orders_key         -- TODO: surrogate/business key not resolved
WHERE 1 = 0;  -- prevent empty insert from running until source is confirmed

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Commerce & Sales Orders]
-- [TYPE   : fact]
-- [TARGET : newmodel3.fact_sales_lines]
-- [SOURCES: source.ams_rem_accounting_lineitem]  -- TODO: confirm full source set
-- [GRAIN  : TODO: confirm grain for sales line items]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel3.fact_sales_lines
(
    sales_line_quantity,
    sales_line_amount,
    sales_order_date,
    product_key,
    individual_key,
    company_key,
    geography_key,
    sales_orders_key,
    sales_lines_key
)
SELECT
    NULL::NUMERIC(18,4) AS sales_line_quantity,   -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS sales_line_amount,     -- TODO: no source mapping identified
    NULL::DATE          AS sales_order_date,      -- TODO: no source mapping identified
    NULL::BIGINT        AS product_key,           -- TODO: natural key not resolved to dim_product
    NULL::BIGINT        AS individual_key,        -- TODO: natural key not resolved to dim_individual
    NULL::BIGINT        AS company_key,           -- TODO: natural key not resolved to dim_company
    NULL::BIGINT        AS geography_key,         -- TODO: natural key not resolved to dim_geography
    NULL::BIGINT        AS sales_orders_key,      -- TODO: natural key not resolved to dim_sales_orders
    NULL::BIGINT        AS sales_lines_key        -- TODO: surrogate/business key not resolved
WHERE 1 = 0;  -- prevent empty insert from running until source is confirmed

-- =======================================================
-- EMAIL MARKETING DOMAIN
-- =======================================================

CREATE TABLE IF NOT EXISTS newmodel3.dim_email_sends (
    email_sends_key   BIGINT IDENTITY(1,1) PRIMARY KEY,
    email_send_status VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_email_opens (
    email_opens_key       BIGINT IDENTITY(1,1) PRIMARY KEY,
    email_first_open_flag VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_email_clicks (
    email_clicks_key       BIGINT IDENTITY(1,1) PRIMARY KEY,
    email_first_click_flag VARCHAR(10),
    email_click_url        VARCHAR(500),
    email_click_domain     VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_email_summaries (
    email_summaries_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    email_summaries     VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_campaign (
    campaign_key    BIGINT IDENTITY(1,1) PRIMARY KEY,
    campaign_code   VARCHAR(100),
    campaign_name   VARCHAR(255),
    campaign_type   VARCHAR(100),
    campaign_status VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_email (
    email_key             BIGINT IDENTITY(1,1) PRIMARY KEY,
    email_code            VARCHAR(100),
    email_name            VARCHAR(255),
    email_type            VARCHAR(100),
    email_subject         VARCHAR(255),
    email_status          VARCHAR(100),
    email_send_first_date DATE,
    email_send_last_date  DATE
);

CREATE TABLE IF NOT EXISTS newmodel3.fact_email_sends (
    email_send_date       DATE,
    email_send_time       VARCHAR(50),
    email_key             BIGINT,
    email_send_status_key BIGINT,
    campaign_key          BIGINT,
    individual_key        BIGINT,
    company_key           BIGINT,
    geography_key         BIGINT,
    email_sends_key       BIGINT
    -- TODO: add FK constraints after validation
);

CREATE TABLE IF NOT EXISTS newmodel3.fact_email_opens (
    email_open_date  DATE,
    email_open_time  VARCHAR(50),
    email_key        BIGINT,
    campaign_key     BIGINT,
    individual_key   BIGINT,
    company_key      BIGINT,
    geography_key    BIGINT,
    email_opens_key  BIGINT
    -- TODO: add FK constraints after validation
);

CREATE TABLE IF NOT EXISTS newmodel3.fact_email_clicks (
    email_click_date  DATE,
    email_click_time  VARCHAR(50),
    email_key         BIGINT,
    campaign_key      BIGINT,
    individual_key    BIGINT,
    company_key       BIGINT,
    geography_key     BIGINT,
    email_clicks_key  BIGINT
    -- TODO: add FK constraints after validation
);

CREATE TABLE IF NOT EXISTS newmodel3.fact_email_summaries (
    email_sends        NUMERIC(18,4),
    email_opens        NUMERIC(18,4),
    email_clicks       NUMERIC(18,4),
    email_bounces      NUMERIC(18,4),
    email_deliveries   NUMERIC(18,4),
    email_key          BIGINT,
    campaign_key       BIGINT,
    email_summaries_key BIGINT
    -- TODO: add FK constraints after validation
);

-- Email facts INSERT skeletons

INSERT INTO newmodel3.fact_email_sends
(
    email_send_date,
    email_send_time,
    email_key,
    email_send_status_key,
    campaign_key,
    individual_key,
    company_key,
    geography_key,
    email_sends_key
)
SELECT
    NULL::DATE        AS email_send_date,        -- TODO: no source mapping identified
    NULL::VARCHAR(50) AS email_send_time,        -- TODO: no source mapping identified
    NULL::BIGINT      AS email_key,              -- TODO: natural key not resolved to dim_email
    NULL::BIGINT      AS email_send_status_key,  -- TODO: natural key not resolved to dim_email_sends
    NULL::BIGINT      AS campaign_key,           -- TODO: natural key not resolved to dim_campaign
    NULL::BIGINT      AS individual_key,         -- TODO: natural key not resolved to dim_individual
    NULL::BIGINT      AS company_key,            -- TODO: natural key not resolved to dim_company
    NULL::BIGINT      AS geography_key,          -- TODO: natural key not resolved to dim_geography
    NULL::BIGINT      AS email_sends_key         -- TODO: surrogate/business key not resolved
WHERE 1 = 0;

INSERT INTO newmodel3.fact_email_opens
(
    email_open_date,
    email_open_time,
    email_key,
    campaign_key,
    individual_key,
    company_key,
    geography_key,
    email_opens_key
)
SELECT
    NULL::DATE        AS email_open_date,        -- TODO: no source mapping identified
    NULL::VARCHAR(50) AS email_open_time,        -- TODO: no source mapping identified
    NULL::BIGINT      AS email_key,              -- TODO: natural key not resolved to dim_email
    NULL::BIGINT      AS campaign_key,           -- TODO: natural key not resolved to dim_campaign
    NULL::BIGINT      AS individual_key,         -- TODO: natural key not resolved to dim_individual
    NULL::BIGINT      AS company_key,            -- TODO: natural key not resolved to dim_company
    NULL::BIGINT      AS geography_key,          -- TODO: natural key not resolved to dim_geography
    NULL::BIGINT      AS email_opens_key         -- TODO: surrogate/business key not resolved
WHERE 1 = 0;

INSERT INTO newmodel3.fact_email_clicks
(
    email_click_date,
    email_click_time,
    email_key,
    campaign_key,
    individual_key,
    company_key,
    geography_key,
    email_clicks_key
)
SELECT
    NULL::DATE        AS email_click_date,       -- TODO: no source mapping identified
    NULL::VARCHAR(50) AS email_click_time,       -- TODO: no source mapping identified
    NULL::BIGINT      AS email_key,              -- TODO: natural key not resolved to dim_email
    NULL::BIGINT      AS campaign_key,           -- TODO: natural key not resolved to dim_campaign
    NULL::BIGINT      AS individual_key,         -- TODO: natural key not resolved to dim_individual
    NULL::BIGINT      AS company_key,            -- TODO: natural key not resolved to dim_company
    NULL::BIGINT      AS geography_key,          -- TODO: natural key not resolved to dim_geography
    NULL::BIGINT      AS email_clicks_key        -- TODO: surrogate/business key not resolved
WHERE 1 = 0;

INSERT INTO newmodel3.fact_email_summaries
(
    email_sends,
    email_opens,
    email_clicks,
    email_bounces,
    email_deliveries,
    email_key,
    campaign_key,
    email_summaries_key
)
SELECT
    NULL::NUMERIC(18,4) AS email_sends,          -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS email_opens,          -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS email_clicks,         -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS email_bounces,        -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS email_deliveries,     -- TODO: no source mapping identified
    NULL::BIGINT        AS email_key,            -- TODO: natural key not resolved to dim_email
    NULL::BIGINT        AS campaign_key,         -- TODO: natural key not resolved to dim_campaign
    NULL::BIGINT        AS email_summaries_key   -- TODO: surrogate/business key not resolved
WHERE 1 = 0;

-- =======================================================
-- COMMUNITY ENGAGEMENT DOMAIN
-- =======================================================

CREATE TABLE IF NOT EXISTS newmodel3.dim_community_activities (
    community_activities_key     BIGINT IDENTITY(1,1) PRIMARY KEY,
    community_activity_description VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_community_discussion_posts (
    community_discussion_posts_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    discussion_post_subject        VARCHAR(255),
    discussion_post_content        VARCHAR(4000),
    discussion_post_type           VARCHAR(100),
    discussion_post_status         VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_community_discussions (
    community_discussions_key BIGINT IDENTITY(1,1) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_community_memberships (
    community_memberships_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    community_membership_type VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_community (
    community_key    BIGINT IDENTITY(1,1) PRIMARY KEY,
    community_code   VARCHAR(100),
    community_name   VARCHAR(255),
    community_type   VARCHAR(100),
    community_status VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS newmodel3.fact_community_activities (
    community_activity_date      DATE,
    community_activity_time      VARCHAR(50),
    community_activity_type_key  BIGINT,
    individual_key               BIGINT,
    company_key                  BIGINT,
    geography_key                BIGINT,
    community_activities_key     BIGINT
    -- TODO: add FK constraints after validation
);

CREATE TABLE IF NOT EXISTS newmodel3.fact_community_discussion_posts (
    community_discussion_post_date  DATE,
    community_discussion_post_time  VARCHAR(50),
    community_discussions_key       BIGINT,
    individual_key                  BIGINT,
    company_key                     BIGINT,
    geography_key                   BIGINT,
    community_discussion_posts_key  BIGINT
    -- TODO: add FK constraints after validation
);

CREATE TABLE IF NOT EXISTS newmodel3.fact_community_discussions (
    community_discussion_post_count NUMERIC(18,4),
    community_discussion_first_date DATE,
    community_discussion_first_time VARCHAR(50),
    community_discussion_last_date  DATE,
    community_discussion_last_time  VARCHAR(50),
    community_key                   BIGINT,
    community_discussions_key       BIGINT
    -- TODO: add FK constraints after validation
);

CREATE TABLE IF NOT EXISTS newmodel3.fact_community_memberships (
    community_membership_date  DATE,
    community_key              BIGINT,
    individual_key             BIGINT,
    company_key                BIGINT,
    geography_key              BIGINT,
    community_memberships_key  BIGINT
    -- TODO: add FK constraints after validation
);

-- Community facts INSERT skeletons

INSERT INTO newmodel3.fact_community_activities
(
    community_activity_date,
    community_activity_time,
    community_activity_type_key,
    individual_key,
    company_key,
    geography_key,
    community_activities_key
)
SELECT
    NULL::DATE        AS community_activity_date,      -- TODO: no source mapping identified
    NULL::VARCHAR(50) AS community_activity_time,      -- TODO: no source mapping identified
    NULL::BIGINT      AS community_activity_type_key,  -- TODO: natural key not resolved to dim_community_activities
    NULL::BIGINT      AS individual_key,               -- TODO: natural key not resolved to dim_individual
    NULL::BIGINT      AS company_key,                  -- TODO: natural key not resolved to dim_company
    NULL::BIGINT      AS geography_key,                -- TODO: natural key not resolved to dim_geography
    NULL::BIGINT      AS community_activities_key      -- TODO: surrogate/business key not resolved
WHERE 1 = 0;

INSERT INTO newmodel3.fact_community_discussion_posts
(
    community_discussion_post_date,
    community_discussion_post_time,
    community_discussions_key,
    individual_key,
    company_key,
    geography_key,
    community_discussion_posts_key
)
SELECT
    NULL::DATE        AS community_discussion_post_date,  -- TODO: no source mapping identified
    NULL::VARCHAR(50) AS community_discussion_post_time,  -- TODO: no source mapping identified
    NULL::BIGINT      AS community_discussions_key,       -- TODO: natural key not resolved to dim_community_discussions
    NULL::BIGINT      AS individual_key,                  -- TODO: natural key not resolved to dim_individual
    NULL::BIGINT      AS company_key,                     -- TODO: natural key not resolved to dim_company
    NULL::BIGINT      AS geography_key,                   -- TODO: natural key not resolved to dim_geography
    NULL::BIGINT      AS community_discussion_posts_key   -- TODO: surrogate/business key not resolved
WHERE 1 = 0;

INSERT INTO newmodel3.fact_community_discussions
(
    community_discussion_post_count,
    community_discussion_first_date,
    community_discussion_first_time,
    community_discussion_last_date,
    community_discussion_last_time,
    community_key,
    community_discussions_key
)
SELECT
    NULL::NUMERIC(18,4) AS community_discussion_post_count, -- TODO: no source mapping identified
    NULL::DATE          AS community_discussion_first_date, -- TODO: no source mapping identified
    NULL::VARCHAR(50)   AS community_discussion_first_time, -- TODO: no source mapping identified
    NULL::DATE          AS community_discussion_last_date,  -- TODO: no source mapping identified
    NULL::VARCHAR(50)   AS community_discussion_last_time,  -- TODO: no source mapping identified
    NULL::BIGINT        AS community_key,                   -- TODO: natural key not resolved to dim_community
    NULL::BIGINT        AS community_discussions_key        -- TODO: surrogate/business key not resolved
WHERE 1 = 0;

INSERT INTO newmodel3.fact_community_memberships
(
    community_membership_date,
    community_key,
    individual_key,
    company_key,
    geography_key,
    community_memberships_key
)
SELECT
    NULL::DATE    AS community_membership_date, -- TODO: no source mapping identified
    NULL::BIGINT  AS community_key,             -- TODO: natural key not resolved to dim_community
    NULL::BIGINT  AS individual_key,            -- TODO: natural key not resolved to dim_individual
    NULL::BIGINT  AS company_key,               -- TODO: natural key not resolved to dim_company
    NULL::BIGINT  AS geography_key,             -- TODO: natural key not resolved to dim_geography
    NULL::BIGINT  AS community_memberships_key  -- TODO: surrogate/business key not resolved
WHERE 1 = 0;

-- =======================================================
-- SPONSORSHIPS & EXHIBITS DOMAIN
-- =======================================================

CREATE TABLE IF NOT EXISTS newmodel3.dim_sponsorships (
    sponsorships_key   BIGINT IDENTITY(1,1) PRIMARY KEY,
    sponsorship_status VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_sponsorship (
    sponsorship_key   BIGINT IDENTITY(1,1) PRIMARY KEY,
    sponsorship_code  VARCHAR(100),
    sponsorship_name  VARCHAR(255),
    sponsorship_type  VARCHAR(100),
    sponsorship_level VARCHAR(100),
    sponsorship_status VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_exhibit (
    exhibit_key   BIGINT IDENTITY(1,1) PRIMARY KEY,
    exhibit_code  VARCHAR(100),
    exhibit_name  VARCHAR(255),
    exhibit_type  VARCHAR(100),
    exhibit_status VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_exhibitor (
    exhibitor_key          BIGINT IDENTITY(1,1) PRIMARY KEY,
    exhibitor_id           VARCHAR(100),
    exhibitor_name         VARCHAR(255),
    exhibitor_type         VARCHAR(100),
    exhibitor_busines_area VARCHAR(255),
    exhibitor_status       VARCHAR(100),
    exhibitor_geography_key BIGINT
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_sponsor (
    sponsor_key          BIGINT IDENTITY(1,1) PRIMARY KEY,
    sponsor_id           VARCHAR(100),
    sponsor_name         VARCHAR(255),
    sponsor_type         VARCHAR(100),
    sponsor_busines_area VARCHAR(255),
    sponsor_status       VARCHAR(100),
    sponsor_geography_key BIGINT
);

CREATE TABLE IF NOT EXISTS newmodel3.fact_sponsorships (
    sponsorship_quanity   NUMERIC(18,4),
    sponsorship_amount    NUMERIC(18,4),
    sponsorship_purchase_date DATE,
    sponsorship_start_date    DATE,
    sponsorship_end_date      DATE,
    product_key               BIGINT,
    sponsor_key               BIGINT,
    sponsorships_key          BIGINT
    -- TODO: add FK constraints after validation
);

CREATE TABLE IF NOT EXISTS newmodel3.fact_event_exhibits (
    event_exhibit_quantity NUMERIC(18,4),
    event_exhibit_amount   NUMERIC(18,4),
    event_exhibit_date     DATE,
    event_start_date       DATE,
    event_key              BIGINT,
    product_key            BIGINT,
    event_exhibit_key      BIGINT,
    exhibitor_key          BIGINT,
    event_exhibits_key     BIGINT
    -- TODO: add FK constraints after validation
);

CREATE TABLE IF NOT EXISTS newmodel3.fact_event_exhibit_purchases (
    event_exhibit_purchase_quantity NUMERIC(18,4),
    event_exhibit_purchase_amount   NUMERIC(18,4),
    event_exhibit_purchase_date     DATE,
    event_start_date                DATE,
    event_key                       BIGINT,
    product_key                     BIGINT,
    event_exhibit_key               BIGINT,
    exhibitor_key                   BIGINT,
    event_exhibits_key              BIGINT,
    event_exhibit_purchases_key     BIGINT
    -- TODO: add FK constraints after validation
);

-- Sponsorship & exhibit facts INSERT skeletons

INSERT INTO newmodel3.fact_sponsorships
(
    sponsorship_quanity,
    sponsorship_amount,
    sponsorship_purchase_date,
    sponsorship_start_date,
    sponsorship_end_date,
    product_key,
    sponsor_key,
    sponsorships_key
)
SELECT
    NULL::NUMERIC(18,4) AS sponsorship_quanity,   -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS sponsorship_amount,    -- TODO: no source mapping identified
    NULL::DATE          AS sponsorship_purchase_date, -- TODO: no source mapping identified
    NULL::DATE          AS sponsorship_start_date,    -- TODO: no source mapping identified
    NULL::DATE          AS sponsorship_end_date,      -- TODO: no source mapping identified
    NULL::BIGINT        AS product_key,               -- TODO: natural key not resolved to dim_product
    NULL::BIGINT        AS sponsor_key,               -- TODO: natural key not resolved to dim_sponsor
    NULL::BIGINT        AS sponsorships_key           -- TODO: surrogate/business key not resolved
WHERE 1 = 0;

INSERT INTO newmodel3.fact_event_exhibits
(
    event_exhibit_quantity,
    event_exhibit_amount,
    event_exhibit_date,
    event_start_date,
    event_key,
    product_key,
    event_exhibit_key,
    exhibitor_key,
    event_exhibits_key
)
SELECT
    NULL::NUMERIC(18,4) AS event_exhibit_quantity, -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS event_exhibit_amount,   -- TODO: no source mapping identified
    NULL::DATE          AS event_exhibit_date,     -- TODO: no source mapping identified
    NULL::DATE          AS event_start_date,       -- TODO: no source mapping identified
    NULL::BIGINT        AS event_key,              -- TODO: natural key not resolved to dim_event
    NULL::BIGINT        AS product_key,            -- TODO: natural key not resolved to dim_product
    NULL::BIGINT        AS event_exhibit_key,      -- TODO: natural key not resolved to dim_exhibit
    NULL::BIGINT        AS exhibitor_key,          -- TODO: natural key not resolved to dim_exhibitor
    NULL::BIGINT        AS event_exhibits_key      -- TODO: surrogate/business key not resolved
WHERE 1 = 0;

INSERT INTO newmodel3.fact_event_exhibit_purchases
(
    event_exhibit_purchase_quantity,
    event_exhibit_purchase_amount,
    event_exhibit_purchase_date,
    event_start_date,
    event_key,
    product_key,
    event_exhibit_key,
    exhibitor_key,
    event_exhibits_key,
    event_exhibit_purchases_key
)
SELECT
    NULL::NUMERIC(18,4) AS event_exhibit_purchase_quantity, -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS event_exhibit_purchase_amount,   -- TODO: no source mapping identified
    NULL::DATE          AS event_exhibit_purchase_date,     -- TODO: no source mapping identified
    NULL::DATE          AS event_start_date,                -- TODO: no source mapping identified
    NULL::BIGINT        AS event_key,                       -- TODO: natural key not resolved to dim_event
    NULL::BIGINT        AS product_key,                     -- TODO: natural key not resolved to dim_product
    NULL::BIGINT        AS event_exhibit_key,               -- TODO: natural key not resolved to dim_exhibit
    NULL::BIGINT        AS exhibitor_key,                   -- TODO: natural key not resolved to dim_exhibitor
    NULL::BIGINT        AS event_exhibits_key,              -- TODO: natural key not resolved to fact_event_exhibits
    NULL::BIGINT        AS event_exhibit_purchases_key      -- TODO: surrogate/business key not resolved
WHERE 1 = 0;

-- =======================================================
-- WEB ANALYTICS & ACTIVITIES DOMAIN
-- =======================================================

CREATE TABLE IF NOT EXISTS newmodel3.dim_web_sessions (
    web_sessions_key  BIGINT IDENTITY(1,1) PRIMARY KEY,
    session_id        VARCHAR(255),
    traffic_source_type VARCHAR(100),
    traffic_medium    VARCHAR(100),
    traffic_source    VARCHAR(255),
    hostname          VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_web_pageviews (
    web_pageviews_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    page_sequence     VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_web_page (
    web_page_key    BIGINT IDENTITY(1,1) PRIMARY KEY,
    page_title      VARCHAR(255),
    page_location_1 VARCHAR(255),
    page_location_2 VARCHAR(255),
    page_location_3 VARCHAR(255),
    page_location_4 VARCHAR(255),
    page_location_5 VARCHAR(255),
    page_referrer   VARCHAR(500)
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_activities (
    activities_key   BIGINT IDENTITY(1,1) PRIMARY KEY,
    activity_status  VARCHAR(100),
    activity_entity  VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS newmodel3.dim_activity_level (
    activity_level_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    activity_level_1   VARCHAR(100),
    activity_level_2   VARCHAR(100),
    activity_level_3   VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS newmodel3.fact_web_sessions (
    session_duration_seconds NUMERIC(18,4),
    session_date             DATE,
    session_time             VARCHAR(50),
    session_datetime         TIMESTAMP,
    geography_key            BIGINT,
    web_sessions_key         BIGINT
    -- TODO: add FK constraints after validation
);

CREATE TABLE IF NOT EXISTS newmodel3.fact_web_pageviews (
    pageview_duration_seconds NUMERIC(18,4),
    pageview_start_date       DATE,
    pageview_start_time       VARCHAR(50),
    pageview_start_datetime   TIMESTAMP,
    geography_key             BIGINT,
    web_page_key              BIGINT,
    web_sessions_key          BIGINT,
    web_pageviews_key         BIGINT
    -- TODO: add FK constraints after validation
);

CREATE TABLE IF NOT EXISTS newmodel3.fact_activities (
    activity_quantity   NUMERIC(18,4),
    activity_amount     NUMERIC(18,4),
    activity_duration   NUMERIC(18,4),
    activity_date       DATE,
    activity_start_date DATE,
    activity_end_date   DATE,
    activity_level_key  BIGINT,
    individual_key      BIGINT,
    company_key         BIGINT,
    geography_key       BIGINT,
    activities_key      BIGINT
    -- TODO: add FK constraints after validation
);

-- Web & activities facts INSERT skeletons

INSERT INTO newmodel3.fact_web_sessions
(
    session_duration_seconds,
    session_date,
    session_time,
    session_datetime,
    geography_key,
    web_sessions_key
)
SELECT
    NULL::NUMERIC(18,4) AS session_duration_seconds, -- TODO: no source mapping identified
    NULL::DATE          AS session_date,             -- TODO: no source mapping identified
    NULL::VARCHAR(50)   AS session_time,             -- TODO: no source mapping identified
    NULL::TIMESTAMP     AS session_datetime,         -- TODO: no source mapping identified
    NULL::BIGINT        AS geography_key,            -- TODO: natural key not resolved to dim_geography
    NULL::BIGINT        AS web_sessions_key          -- TODO: surrogate/business key not resolved
WHERE 1 = 0;

INSERT INTO newmodel3.fact_web_pageviews
(
    pageview_duration_seconds,
    pageview_start_date,
    pageview_start_time,
    pageview_start_datetime,
    geography_key,
    web_page_key,
    web_sessions_key,
    web_pageviews_key
)
SELECT
    NULL::NUMERIC(18,4) AS pageview_duration_seconds, -- TODO: no source mapping identified
    NULL::DATE          AS pageview_start_date,       -- TODO: no source mapping identified
    NULL::VARCHAR(50)   AS pageview_start_time,       -- TODO: no source mapping identified
    NULL::TIMESTAMP     AS pageview_start_datetime,   -- TODO: no source mapping identified
    NULL::BIGINT        AS geography_key,             -- TODO: natural key not resolved to dim_geography
    NULL::BIGINT        AS web_page_key,              -- TODO: natural key not resolved to dim_web_page
    NULL::BIGINT        AS web_sessions_key,          -- TODO: natural key not resolved to dim_web_sessions
    NULL::BIGINT        AS web_pageviews_key          -- TODO: surrogate/business key not resolved
WHERE 1 = 0;

INSERT INTO newmodel3.fact_activities
(
    activity_quantity,
    activity_amount,
    activity_duration,
    activity_date,
    activity_start_date,
    activity_end_date,
    activity_level_key,
    individual_key,
    company_key,
    geography_key,
    activities_key
)
SELECT
    NULL::NUMERIC(18,4) AS activity_quantity,   -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS activity_amount,     -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS activity_duration,   -- TODO: no source mapping identified
    NULL::DATE          AS activity_date,       -- TODO: no source mapping identified
    NULL::DATE          AS activity_start_date, -- TODO: no source mapping identified
    NULL::DATE          AS activity_end_date,   -- TODO: no source mapping identified
    NULL::BIGINT        AS activity_level_key,  -- TODO: natural key not resolved to dim_activity_level
    NULL::BIGINT        AS individual_key,      -- TODO: natural key not resolved to dim_individual
    NULL::BIGINT        AS company_key,         -- TODO: natural key not resolved to dim_company
    NULL::BIGINT        AS geography_key,       -- TODO: natural key not resolved to dim_geography
    NULL::BIGINT        AS activities_key       -- TODO: surrogate/business key not resolved
WHERE 1 = 0;

CREATE TABLE IF NOT EXISTS newmodel3.fact_memberships (
    membership_quantity      NUMERIC(18,4),
    membership_amount        NUMERIC(18,4),
    membership_start_date    DATE,
    membership_end_date      DATE,
    membership_grace_date    DATE,
    membership_type_key      BIGINT,
    product_key              BIGINT,
    individual_key           BIGINT,
    company_key              BIGINT,
    geography_key            BIGINT,
    memberships_key          BIGINT
    -- TODO: add FK constraints after validation
);

CREATE TABLE IF NOT EXISTS newmodel3.fact_chapter_memberships (
    chapter_membership_quantity   NUMERIC(18,4),
    chapter_membership_amount     NUMERIC(18,4),
    chapter_membership_start_date DATE,
    chapter_membership_end_date   DATE,
    chapter_membership_grace_date DATE,
    chapter_key                   BIGINT,
    individual_key                BIGINT,
    company_key                   BIGINT,
    geography_key                 BIGINT,
    chapter_memberships_key       BIGINT
    -- TODO: add FK constraints after validation
);

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Membership & Customers]
-- [TYPE   : fact]
-- [TARGET : newmodel3.fact_memberships]
-- [SOURCES: source.ams_rem_crm_membershiplog, source.ams_rem_shopping_membership]
-- [GRAIN  : one row per membership term log entry]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel3.fact_memberships
(
    membership_quantity,
    membership_amount,
    membership_start_date,
    membership_end_date,
    membership_grace_date,
    membership_type_key,
    product_key,
    individual_key,
    company_key,
    geography_key,
    memberships_key
)
SELECT
    1::NUMERIC(18,4)                                                              AS membership_quantity,      -- derived: one record per membership log row
    0::NUMERIC(18,4)                                                              AS membership_amount,        -- TODO: no reliable source mapping identified yet
    CASE
        WHEN LENGTH(TRIM(s1.effectivedate)) >= 10
            THEN CAST(SUBSTRING(s1.effectivedate, 1, 10) AS DATE)
        WHEN LENGTH(TRIM(s1.joindate)) >= 10
            THEN CAST(SUBSTRING(s1.joindate, 1, 10) AS DATE)
        ELSE NULL::DATE
    END                                                                           AS membership_start_date,    -- source: ams_rem_crm_membershiplog.effectivedate / joindate
    CASE
        WHEN LENGTH(TRIM(s1.expiredate)) >= 10
            THEN CAST(SUBSTRING(s1.expiredate, 1, 10) AS DATE)
        ELSE NULL::DATE
    END                                                                           AS membership_end_date,      -- source: ams_rem_crm_membershiplog.expiredate
    CASE
        WHEN LENGTH(TRIM(s1.terminatedate)) >= 10
            THEN CAST(SUBSTRING(s1.terminatedate, 1, 10) AS DATE)
        ELSE NULL::DATE
    END                                                                           AS membership_grace_date,    -- source: ams_rem_crm_membershiplog.terminatedate
    NULL::BIGINT                                                                  AS membership_type_key,      -- TODO: natural key not resolved between membership log and dim_membership_type
    NULL::BIGINT                                                                  AS product_key,              -- TODO: natural key not resolved between membershipproductid/code and dim_product
    NULL::BIGINT                                                                  AS individual_key,           -- TODO: natural key not resolved between customerid and dim_individual
    NULL::BIGINT                                                                  AS company_key,              -- TODO: natural key not resolved between customerid and dim_company
    NULL::BIGINT                                                                  AS geography_key,            -- TODO: derive from customer address / geography dimension
    NULL::BIGINT                                                                  AS memberships_key           -- TODO: natural key not resolved between membership status and dim_memberships
FROM source.ams_rem_crm_membershiplog s1
LEFT JOIN source.ams_rem_shopping_membership s2
    ON s1.membershipproductid = s2.id
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE
    AND s1.id IS NOT NULL
    AND (s1.meta_record_status IS NULL OR s1.meta_record_status = 'A');

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Membership & Customers]
-- [TYPE   : fact]
-- [TARGET : newmodel3.fact_chapter_memberships]
-- [SOURCES: source.ams_rem_crm_membershiplog, source.ams_rem_shopping_membership]
-- [GRAIN  : one row per chapter membership term log entry]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel3.fact_chapter_memberships
(
    chapter_membership_quantity,
    chapter_membership_amount,
    chapter_membership_start_date,
    chapter_membership_end_date,
    chapter_membership_grace_date,
    chapter_key,
    individual_key,
    company_key,
    geography_key,
    chapter_memberships_key
)
SELECT
    1::NUMERIC(18,4)                                                              AS chapter_membership_quantity,   -- derived: one record per chapter membership log row
    0::NUMERIC(18,4)                                                              AS chapter_membership_amount,     -- TODO: no reliable source mapping identified yet
    CASE
        WHEN LENGTH(TRIM(s1.effectivedate)) >= 10
            THEN CAST(SUBSTRING(s1.effectivedate, 1, 10) AS DATE)
        WHEN LENGTH(TRIM(s1.joindate)) >= 10
            THEN CAST(SUBSTRING(s1.joindate, 1, 10) AS DATE)
        ELSE NULL::DATE
    END                                                                           AS chapter_membership_start_date, -- source: ams_rem_crm_membershiplog.effectivedate / joindate
    CASE
        WHEN LENGTH(TRIM(s1.expiredate)) >= 10
            THEN CAST(SUBSTRING(s1.expiredate, 1, 10) AS DATE)
        ELSE NULL::DATE
    END                                                                           AS chapter_membership_end_date,   -- source: ams_rem_crm_membershiplog.expiredate
    CASE
        WHEN LENGTH(TRIM(s1.terminatedate)) >= 10
            THEN CAST(SUBSTRING(s1.terminatedate, 1, 10) AS DATE)
        ELSE NULL::DATE
    END                                                                           AS chapter_membership_grace_date, -- source: ams_rem_crm_membershiplog.terminatedate
    NULL::BIGINT                                                                  AS chapter_key,                   -- TODO: natural key not resolved between membership product and dim_chapter
    NULL::BIGINT                                                                  AS individual_key,                -- TODO: natural key not resolved between customerid and dim_individual
    NULL::BIGINT                                                                  AS company_key,                   -- TODO: natural key not resolved between customerid and dim_company
    NULL::BIGINT                                                                  AS geography_key,                 -- TODO: derive from customer address / geography dimension
    NULL::BIGINT                                                                  AS chapter_memberships_key        -- TODO: natural key not resolved for chapter-membership status dimension
FROM source.ams_rem_crm_membershiplog s1
LEFT JOIN source.ams_rem_shopping_membership s2
    ON s1.membershipproductid = s2.id
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE
    AND s1.id IS NOT NULL
    AND (s1.meta_record_status IS NULL OR s1.meta_record_status = 'A')
    AND COALESCE(s2.ischapter, FALSE) = TRUE;