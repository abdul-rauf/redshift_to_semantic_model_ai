CREATE SCHEMA IF NOT EXISTS newmodel2;

CREATE TABLE IF NOT EXISTS newmodel2.dim_individual (
    individual_key           BIGINT        NOT NULL,
    individual_id            VARCHAR(36)   NOT NULL,
    individual_last_name     VARCHAR(1000),
    individual_first_name    VARCHAR(1000),
    individual_job_function  VARCHAR(1000),
    individual_job_level     VARCHAR(1000),
    individual_status        VARCHAR(50),
    individual_company_key   BIGINT,
    individual_geography_key BIGINT,
    PRIMARY KEY (individual_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_company (
    company_key           BIGINT        NOT NULL,
    company_id            VARCHAR(36)   NOT NULL,
    company_name          VARCHAR(1000),
    company_type          VARCHAR(255),
    company_busines_area  VARCHAR(255),
    company_status        VARCHAR(50),
    company_geography_key BIGINT,
    PRIMARY KEY (company_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_membership_type (
    membership_type          VARCHAR(50)   NOT NULL,
    membership_type_name     VARCHAR(255),
    membership_type_category VARCHAR(255),
    membership_type_entity   VARCHAR(255),
    PRIMARY KEY (membership_type)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_product (
    product_key   BIGINT        NOT NULL,
    product_code  VARCHAR(100),
    product_name  VARCHAR(1000),
    product_type  VARCHAR(255),
    product_status VARCHAR(50),
    PRIMARY KEY (product_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_activity_level (
    activity_level_key BIGINT       NOT NULL,
    activity_level_1   VARCHAR(255),
    activity_level_2   VARCHAR(255),
    activity_level_3   VARCHAR(255),
    PRIMARY KEY (activity_level_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_activities (
    activities_key     BIGINT       NOT NULL,
    activity_status    VARCHAR(50),
    activity_entity    VARCHAR(255),
    PRIMARY KEY (activities_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_geography (
    geography_key BIGINT       NOT NULL,
    PRIMARY KEY (geography_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_date (
    date_key      INTEGER      NOT NULL,
    PRIMARY KEY (date_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_month (
    month_key     INTEGER      NOT NULL,
    PRIMARY KEY (month_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_year (
    year_key      INTEGER      NOT NULL,
    PRIMARY KEY (year_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.fact_memberships (
    membership_quantity      NUMERIC(18,4),
    membership_amount        NUMERIC(18,4),
    membership_start_date    DATE,
    membership_end_date      DATE,
    membership_grace_date    DATE,
    membership_type_key      VARCHAR(50),
    product_key              BIGINT,
    individual_key           BIGINT,
    company_key              BIGINT,
    geography_key            BIGINT,
    memberships_key          BIGINT,
    PRIMARY KEY (memberships_key)
    -- TODO: enable FK after validation
);

CREATE TABLE IF NOT EXISTS newmodel2.fact_chapter_memberships (
    chapter_membership_quantity   NUMERIC(18,4),
    chapter_membership_amount     NUMERIC(18,4),
    chapter_membership_start_date DATE,
    chapter_membership_end_date   DATE,
    chapter_membership_grace_date DATE,
    chapter_key                   BIGINT,
    individual_key                BIGINT,
    company_key                   BIGINT,
    geography_key                 BIGINT,
    chapter_memberships_key       BIGINT,
    PRIMARY KEY (chapter_memberships_key)
    -- TODO: enable FK after validation
);

CREATE TABLE IF NOT EXISTS newmodel2.fact_activities (
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
    activities_key      BIGINT,
    PRIMARY KEY (activities_key)
    -- TODO: enable FK after validation
);

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Customers & Organizations]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_individual]
-- [SOURCES: source.ams_rem_crm_individual]
-- [GRAIN  : One row per CRM individual record (grain unclear in EDA).]
-- ────────────────────────────────────────────────────────
-- WARNING: source table source.ams_rem_crm_individual has modeling_readiness = "low" in EDA.

INSERT INTO newmodel2.dim_individual
(
    individual_key,
    individual_id,
    individual_last_name,
    individual_first_name,
    individual_job_function,
    individual_job_level,
    individual_status,
    individual_company_key,
    individual_geography_key
)
SELECT
    CAST(s1.ams_rem_crm_individual_key_sk AS BIGINT)        AS individual_key,          -- source: ams_rem_crm_individual.ams_rem_crm_individual_key_sk
    CAST(s1.id AS VARCHAR(36))                              AS individual_id,           -- source: ams_rem_crm_individual.id
    CAST(s1.lastname AS VARCHAR(1000))                      AS individual_last_name,    -- source: ams_rem_crm_individual.lastname
    CAST(s1.firstname AS VARCHAR(1000))                     AS individual_first_name,   -- source: ams_rem_crm_individual.firstname
    NULL::VARCHAR(1000)                                     AS individual_job_function, -- TODO: no source mapping identified
    NULL::VARCHAR(1000)                                     AS individual_job_level,    -- TODO: no source mapping identified
    CAST(s1.meta_record_status AS VARCHAR(50))              AS individual_status,       -- source: ams_rem_crm_individual.meta_record_status
    NULL::BIGINT                                            AS individual_company_key,  -- TODO: natural key not resolved between individual and organization
    NULL::BIGINT                                            AS individual_geography_key -- TODO: geography not resolved from address/region tables
FROM source.ams_rem_crm_individual AS s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE
    AND s1.id IS NOT NULL;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Customers & Organizations]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_company]
-- [SOURCES: source.ams_rem_crm_organization]
-- [GRAIN  : One row per CRM organization record (grain unclear in EDA).]
-- ────────────────────────────────────────────────────────
-- WARNING: source table source.ams_rem_crm_organization has modeling_readiness = "low" in EDA.

INSERT INTO newmodel2.dim_company
(
    company_key,
    company_id,
    company_name,
    company_type,
    company_busines_area,
    company_status,
    company_geography_key
)
SELECT
    CAST(s1.ams_rem_crm_organization_key_sk AS BIGINT)      AS company_key,           -- source: ams_rem_crm_organization.ams_rem_crm_organization_key_sk
    CAST(s1.id AS VARCHAR(36))                              AS company_id,            -- source: ams_rem_crm_organization.id
    CAST(s1.name AS VARCHAR(1000))                          AS company_name,          -- source: ams_rem_crm_organization.name
    NULL::VARCHAR(255)                                      AS company_type,          -- TODO: no source mapping identified
    NULL::VARCHAR(255)                                      AS company_busines_area,  -- TODO: no source mapping identified
    CAST(s1.meta_record_status AS VARCHAR(50))              AS company_status,        -- source: ams_rem_crm_organization.meta_record_status
    NULL::BIGINT                                            AS company_geography_key  -- TODO: geography not resolved from address/region tables
FROM source.ams_rem_crm_organization AS s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE
    AND s1.id IS NOT NULL;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Customers & Organizations]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_membership_type]
-- [SOURCES: source.ams_rem_shopping_membership]
-- [GRAIN  : One row per membership configuration / type definition.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_membership_type
(
    membership_type,
    membership_type_name,
    membership_type_category,
    membership_type_entity
)
SELECT
    CAST(s1.id AS VARCHAR(50))              AS membership_type,          -- source: ams_rem_shopping_membership.id (treat as type identifier)
    CAST(s1.type AS VARCHAR(255))           AS membership_type_name,     -- source: ams_rem_shopping_membership.type
    NULL::VARCHAR(255)                      AS membership_type_category, -- TODO: no source mapping identified
    CASE WHEN s1.ischapter THEN 'Chapter'   -- derived: chapter vs national indicator
         ELSE 'National'
    END                                     AS membership_type_entity    -- derived: entity based on ischapter flag
FROM source.ams_rem_shopping_membership AS s1
WHERE
    COALESCE(s1.ischapter, FALSE) IN (TRUE, FALSE);

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Customers & Organizations]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_product]
-- [SOURCES: source.ams_rem_shopping_price]
-- [GRAIN  : One row per product/price configuration (approximation).]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_product
(
    product_key,
    product_code,
    product_name,
    product_type,
    product_status
)
SELECT
    CAST(s1.ams_rem_shopping_price_key_sk AS BIGINT) AS product_key,   -- source: ams_rem_shopping_price.ams_rem_shopping_price_key_sk
    CAST(s1.productid AS VARCHAR(100))               AS product_code,  -- source: ams_rem_shopping_price.productid
    NULL::VARCHAR(1000)                              AS product_name,  -- TODO: no source mapping identified
    NULL::VARCHAR(255)                               AS product_type,  -- TODO: no source mapping identified
    NULL::VARCHAR(50)                                AS product_status -- TODO: no source mapping identified
FROM source.ams_rem_shopping_price AS s1
WHERE
    s1.productid IS NOT NULL;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Customers & Organizations]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_activity_level]
-- [SOURCES: source.ams_rem_crm_customeractivitylog]
-- [GRAIN  : One row per activity category/level derived from customer activity log.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_activity_level
(
    activity_level_key,
    activity_level_1,
    activity_level_2,
    activity_level_3
)
SELECT
    ROW_NUMBER() OVER (ORDER BY COALESCE(activity, category)) AS activity_level_key, -- derived: surrogate key over distinct activity/category
    CAST(activity AS VARCHAR(255))                            AS activity_level_1,   -- source: ams_rem_crm_customeractivitylog.activity
    CAST(category AS VARCHAR(255))                            AS activity_level_2,   -- source: ams_rem_crm_customeractivitylog.category
    NULL::VARCHAR(255)                                        AS activity_level_3    -- TODO: no source mapping identified
FROM (
    SELECT DISTINCT
        activity,
        category
    FROM source.ams_rem_crm_customeractivitylog
) s1;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Customers & Organizations]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_activities]
-- [SOURCES: source.ams_rem_crm_customeractivitylog]
-- [GRAIN  : One row per distinct customer activity from log.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_activities
(
    activities_key,
    activity_status,
    activity_entity
)
SELECT
    CAST(s1.ams_rem_crm_customeractivitylog_key_sk AS BIGINT) AS activities_key,     -- source: ams_rem_crm_customeractivitylog.ams_rem_crm_customeractivitylog_key_sk
    CAST(s1.meta_record_status AS VARCHAR(50))                AS activity_status,    -- source: ams_rem_crm_customeractivitylog.meta_record_status
    CAST(s1.activity AS VARCHAR(255))                         AS activity_entity     -- source: ams_rem_crm_customeractivitylog.activity
FROM source.ams_rem_crm_customeractivitylog AS s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Customers & Organizations]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_geography]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : Geography dimension to be populated later.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_geography
(
    geography_key
)
SELECT
    NULL::BIGINT AS geography_key  -- TODO: no source mapping identified
WHERE 1 = 0; -- prevent empty insert until geography modeling is defined

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Customers & Organizations]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_date]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per calendar date.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_date
(
    date_key
)
SELECT
    NULL::INTEGER AS date_key  -- TODO: populate from calendar generation process
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Customers & Organizations]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_month]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per calendar month.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_month
(
    month_key
)
SELECT
    NULL::INTEGER AS month_key  -- TODO: populate from calendar generation process
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Customers & Organizations]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_year]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per calendar year.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_year
(
    year_key
)
SELECT
    NULL::INTEGER AS year_key  -- TODO: populate from calendar generation process
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Customers & Organizations]
-- [TYPE   : fact]
-- [TARGET : newmodel2.fact_memberships]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : Membership transactions fact to be defined.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.fact_memberships
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
    NULL::NUMERIC(18,4) AS membership_quantity,      -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS membership_amount,        -- TODO: no source mapping identified
    NULL::DATE          AS membership_start_date,    -- TODO: no source mapping identified
    NULL::DATE          AS membership_end_date,      -- TODO: no source mapping identified
    NULL::DATE          AS membership_grace_date,    -- TODO: no source mapping identified
    NULL::VARCHAR(50)   AS membership_type_key,      -- TODO: no source mapping identified
    NULL::BIGINT        AS product_key,              -- TODO: no source mapping identified
    NULL::BIGINT        AS individual_key,           -- TODO: no source mapping identified
    NULL::BIGINT        AS company_key,              -- TODO: no source mapping identified
    NULL::BIGINT        AS geography_key,            -- TODO: no source mapping identified
    NULL::BIGINT        AS memberships_key           -- TODO: no source mapping identified
WHERE 1 = 0; -- prevent empty insert until source is confirmed

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Customers & Organizations]
-- [TYPE   : fact]
-- [TARGET : newmodel2.fact_chapter_memberships]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : Chapter membership transactions fact to be defined.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.fact_chapter_memberships
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
    NULL::NUMERIC(18,4) AS chapter_membership_quantity,   -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS chapter_membership_amount,     -- TODO: no source mapping identified
    NULL::DATE          AS chapter_membership_start_date, -- TODO: no source mapping identified
    NULL::DATE          AS chapter_membership_end_date,   -- TODO: no source mapping identified
    NULL::DATE          AS chapter_membership_grace_date, -- TODO: no source mapping identified
    NULL::BIGINT        AS chapter_key,                   -- TODO: no source mapping identified
    NULL::BIGINT        AS individual_key,                -- TODO: no source mapping identified
    NULL::BIGINT        AS company_key,                   -- TODO: no source mapping identified
    NULL::BIGINT        AS geography_key,                 -- TODO: no source mapping identified
    NULL::BIGINT        AS chapter_memberships_key        -- TODO: no source mapping identified
WHERE 1 = 0; -- prevent empty insert until source is confirmed

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Customers & Organizations]
-- [TYPE   : fact]
-- [TARGET : newmodel2.fact_activities]
-- [SOURCES: source.ams_rem_crm_customeractivitylog]
-- [GRAIN  : One row per customer activity log entry.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.fact_activities
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
    1::NUMERIC(18,4)                                 AS activity_quantity,   -- derived: one row per activity
    0::NUMERIC(18,4)                                 AS activity_amount,     -- TODO: no monetary mapping identified
    NULL::NUMERIC(18,4)                              AS activity_duration,   -- TODO: no duration mapping identified
    CAST(NULL AS DATE)                               AS activity_date,       -- TODO: map from createdon when date format confirmed
    CAST(NULL AS DATE)                               AS activity_start_date, -- TODO: no source mapping identified
    CAST(NULL AS DATE)                               AS activity_end_date,   -- TODO: no source mapping identified
    NULL::BIGINT                                     AS activity_level_key,  -- TODO: join to dim_activity_level via activity/category
    NULL::BIGINT                                     AS individual_key,      -- TODO: natural key not resolved between activity and individual/company
    NULL::BIGINT                                     AS company_key,         -- TODO: natural key not resolved between activity and company
    NULL::BIGINT                                     AS geography_key,       -- TODO: geography not resolved from address/region tables
    CAST(s1.ams_rem_crm_customeractivitylog_key_sk AS BIGINT) AS activities_key -- source: ams_rem_crm_customeractivitylog.ams_rem_crm_customeractivitylog_key_sk
FROM source.ams_rem_crm_customeractivitylog AS s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE;

CREATE TABLE IF NOT EXISTS newmodel2.dim_event (
    event_key       BIGINT       NOT NULL,
    event_code      VARCHAR(100),
    event_name      VARCHAR(1000),
    event_type      VARCHAR(255),
    event_status    VARCHAR(50),
    event_location  VARCHAR(1000),
    event_start_date DATE,
    event_end_date   DATE,
    PRIMARY KEY (event_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_event_registrations (
    event_registrations_key BIGINT       NOT NULL,
    event_registration_status VARCHAR(50),
    PRIMARY KEY (event_registrations_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_event_sessions (
    event_sessions_key   BIGINT       NOT NULL,
    event_session_status VARCHAR(50),
    PRIMARY KEY (event_sessions_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_event_purchases (
    event_purchases_key   BIGINT       NOT NULL,
    event_purchase_status VARCHAR(50),
    PRIMARY KEY (event_purchases_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_event_exhibits (
    event_exhibits_key           BIGINT       NOT NULL,
    event_exhibit_status         VARCHAR(50),
    event_exhibit_transaction_type VARCHAR(50),
    PRIMARY KEY (event_exhibits_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_event_exhibit_purchases (
    event_exhibit_purchases_key   BIGINT       NOT NULL,
    event_exhibit_purchase_status VARCHAR(50),
    PRIMARY KEY (event_exhibit_purchases_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.fact_event_registrations (
    registration_quantity      NUMERIC(18,4),
    registration_amount        NUMERIC(18,4),
    event_distance             NUMERIC(18,4),
    event_registration_date    DATE,
    event_start_date           DATE,
    event_key                  BIGINT,
    product_key                BIGINT,
    individual_key             BIGINT,
    company_key                BIGINT,
    geography_key              BIGINT,
    event_geography_key        BIGINT,
    event_registrations_key    BIGINT,
    PRIMARY KEY (event_registrations_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.fact_event_sessions (
    session_amount             NUMERIC(18,4),
    event_distance             NUMERIC(18,4),
    event_registration_date    DATE,
    session_registration_date  DATE,
    session_start_date         DATE,
    session_end_date           DATE,
    session_start_time         VARCHAR(50),
    session_end_time           VARCHAR(50),
    session_key                BIGINT,
    event_key                  BIGINT,
    product_key                BIGINT,
    location_key               BIGINT,
    individual_key             BIGINT,
    company_key                BIGINT,
    geography_key              BIGINT,
    event_geography_key        BIGINT,
    event_registration_key     BIGINT,
    event_sessions_key         BIGINT,
    PRIMARY KEY (event_sessions_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.fact_event_purchases (
    purchase_quantity          NUMERIC(18,4),
    purchase_amount            NUMERIC(18,4),
    purchase_date              DATE,
    event_registration_date    DATE,
    event_distance             NUMERIC(18,4),
    event_key                  BIGINT,
    product_key                BIGINT,
    individual_key             BIGINT,
    company_key                BIGINT,
    geography_key              BIGINT,
    event_geography_key        BIGINT,
    event_registration_key     BIGINT,
    event_sessions_key         BIGINT,
    PRIMARY KEY (event_registration_key, event_sessions_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.fact_event_exhibits (
    event_exhibit_quantity     NUMERIC(18,4),
    event_exhibit_amount       NUMERIC(18,4),
    event_exhibit_date         DATE,
    event_start_date           DATE,
    event_key                  BIGINT,
    product_key                BIGINT,
    event_exhibit_key          BIGINT,
    exhibitor_key              BIGINT,
    event_exhibits_key         BIGINT,
    PRIMARY KEY (event_exhibits_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.fact_event_exhibit_purchases (
    event_exhibit_purchase_quantity NUMERIC(18,4),
    event_exhibit_purchase_amount   NUMERIC(18,4),
    event_exhibit_purchase_date     DATE,
    event_start_date                DATE,
    event_key                       BIGINT,
    product_key                     BIGINT,
    event_exhibit_key               BIGINT,
    exhibitor_key                   BIGINT,
    event_exhibits_key              BIGINT,
    event_exhibit_purchases_key     BIGINT,
    PRIMARY KEY (event_exhibit_purchases_key)
);

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Events]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_event]
-- [SOURCES: source.ams_rem_shopping_event]
-- [GRAIN  : One row per event in the shopping catalog.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_event
(
    event_key,
    event_code,
    event_name,
    event_type,
    event_status,
    event_location,
    event_start_date,
    event_end_date
)
SELECT
    CAST(s1.ams_rem_shopping_event_key_sk AS BIGINT)       AS event_key,       -- source: ams_rem_shopping_event.ams_rem_shopping_event_key_sk
    CAST(s1.id AS VARCHAR(100))                            AS event_code,      -- source: ams_rem_shopping_event.id
    NULL::VARCHAR(1000)                                    AS event_name,      -- TODO: no source mapping identified
    CAST(s1.type AS VARCHAR(255))                          AS event_type,      -- source: ams_rem_shopping_event.type
    NULL::VARCHAR(50)                                      AS event_status,    -- TODO: no source mapping identified
    NULL::VARCHAR(1000)                                    AS event_location,  -- TODO: map from venue/location tables
    CAST(NULL AS DATE)                                     AS event_start_date, -- TODO: map from eventstartdate when format confirmed
    CAST(NULL AS DATE)                                     AS event_end_date    -- TODO: map from eventenddate when format confirmed
FROM source.ams_rem_shopping_event AS s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Events]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_event_registrations]
-- [SOURCES: source.ams_rem_purchase_registrationpurchase]
-- [GRAIN  : One row per event registration purchase.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_event_registrations
(
    event_registrations_key,
    event_registration_status
)
SELECT
    CAST(s1.ams_rem_purchase_registrationpurchase_key_sk AS BIGINT) AS event_registrations_key,  -- source: ams_rem_purchase_registrationpurchase.ams_rem_purchase_registrationpurchase_key_sk
    CAST(s1.meta_record_status AS VARCHAR(50))                      AS event_registration_status -- source: ams_rem_purchase_registrationpurchase.meta_record_status
FROM source.ams_rem_purchase_registrationpurchase AS s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Events]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_event_sessions]
-- [SOURCES: source.ams_rem_shopping_session]
-- [GRAIN  : One row per event session.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_event_sessions
(
    event_sessions_key,
    event_session_status
)
SELECT
    CAST(s1.ams_rem_shopping_session_key_sk AS BIGINT) AS event_sessions_key,   -- source: ams_rem_shopping_session.ams_rem_shopping_session_key_sk
    CAST(s1.meta_record_status AS VARCHAR(50))         AS event_session_status  -- source: ams_rem_shopping_session.meta_record_status
FROM source.ams_rem_shopping_session AS s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Events]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_event_purchases]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per event purchase header.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_event_purchases
(
    event_purchases_key,
    event_purchase_status
)
SELECT
    NULL::BIGINT      AS event_purchases_key,   -- TODO: no source mapping identified
    NULL::VARCHAR(50) AS event_purchase_status  -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Events]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_event_exhibits]
-- [SOURCES: source.ams_rem_shopping_booth]
-- [GRAIN  : One row per event exhibit/booth definition.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_event_exhibits
(
    event_exhibits_key,
    event_exhibit_status,
    event_exhibit_transaction_type
)
SELECT
    CAST(s1.ams_rem_shopping_booth_key_sk AS BIGINT) AS event_exhibits_key,          -- source: ams_rem_shopping_booth.ams_rem_shopping_booth_key_sk
    NULL::VARCHAR(50)                                AS event_exhibit_status,        -- TODO: no source mapping identified
    NULL::VARCHAR(50)                                AS event_exhibit_transaction_type -- TODO: no source mapping identified
FROM source.ams_rem_shopping_booth AS s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Events]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_event_exhibit_purchases]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per event exhibit purchase header.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_event_exhibit_purchases
(
    event_exhibit_purchases_key,
    event_exhibit_purchase_status
)
SELECT
    NULL::BIGINT      AS event_exhibit_purchases_key,   -- TODO: no source mapping identified
    NULL::VARCHAR(50) AS event_exhibit_purchase_status  -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Events]
-- [TYPE   : fact]
-- [TARGET : newmodel2.fact_event_registrations]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per event registration transaction.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.fact_event_registrations
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
    NULL::NUMERIC(18,4) AS registration_quantity,   -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS registration_amount,     -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS event_distance,          -- TODO: no source mapping identified
    NULL::DATE          AS event_registration_date, -- TODO: no source mapping identified
    NULL::DATE          AS event_start_date,        -- TODO: no source mapping identified
    NULL::BIGINT        AS event_key,               -- TODO: no source mapping identified
    NULL::BIGINT        AS product_key,             -- TODO: no source mapping identified
    NULL::BIGINT        AS individual_key,          -- TODO: no source mapping identified
    NULL::BIGINT        AS company_key,             -- TODO: no source mapping identified
    NULL::BIGINT        AS geography_key,           -- TODO: no source mapping identified
    NULL::BIGINT        AS event_geography_key,     -- TODO: no source mapping identified
    NULL::BIGINT        AS event_registrations_key  -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Events]
-- [TYPE   : fact]
-- [TARGET : newmodel2.fact_event_sessions]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per event session transaction.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.fact_event_sessions
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
    NULL::NUMERIC(18,4) AS session_amount,            -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS event_distance,            -- TODO: no source mapping identified
    NULL::DATE          AS event_registration_date,   -- TODO: no source mapping identified
    NULL::DATE          AS session_registration_date, -- TODO: no source mapping identified
    NULL::DATE          AS session_start_date,        -- TODO: no source mapping identified
    NULL::DATE          AS session_end_date,          -- TODO: no source mapping identified
    NULL::VARCHAR(50)   AS session_start_time,        -- TODO: no source mapping identified
    NULL::VARCHAR(50)   AS session_end_time,          -- TODO: no source mapping identified
    NULL::BIGINT        AS session_key,               -- TODO: no source mapping identified
    NULL::BIGINT        AS event_key,                 -- TODO: no source mapping identified
    NULL::BIGINT        AS product_key,               -- TODO: no source mapping identified
    NULL::BIGINT        AS location_key,              -- TODO: no source mapping identified
    NULL::BIGINT        AS individual_key,            -- TODO: no source mapping identified
    NULL::BIGINT        AS company_key,               -- TODO: no source mapping identified
    NULL::BIGINT        AS geography_key,             -- TODO: no source mapping identified
    NULL::BIGINT        AS event_geography_key,       -- TODO: no source mapping identified
    NULL::BIGINT        AS event_registration_key,    -- TODO: no source mapping identified
    NULL::BIGINT        AS event_sessions_key         -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Events]
-- [TYPE   : fact]
-- [TARGET : newmodel2.fact_event_purchases]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per event purchase transaction.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.fact_event_purchases
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
    NULL::BIGINT        AS event_key,                -- TODO: no source mapping identified
    NULL::BIGINT        AS product_key,              -- TODO: no source mapping identified
    NULL::BIGINT        AS individual_key,           -- TODO: no source mapping identified
    NULL::BIGINT        AS company_key,              -- TODO: no source mapping identified
    NULL::BIGINT        AS geography_key,            -- TODO: no source mapping identified
    NULL::BIGINT        AS event_geography_key,      -- TODO: no source mapping identified
    NULL::BIGINT        AS event_registration_key,   -- TODO: no source mapping identified
    NULL::BIGINT        AS event_sessions_key        -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Events]
-- [TYPE   : fact]
-- [TARGET : newmodel2.fact_event_exhibits]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per event exhibit transaction.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.fact_event_exhibits
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
    NULL::BIGINT        AS event_key,              -- TODO: no source mapping identified
    NULL::BIGINT        AS product_key,            -- TODO: no source mapping identified
    NULL::BIGINT        AS event_exhibit_key,      -- TODO: no source mapping identified
    NULL::BIGINT        AS exhibitor_key,          -- TODO: no source mapping identified
    NULL::BIGINT        AS event_exhibits_key      -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Events]
-- [TYPE   : fact]
-- [TARGET : newmodel2.fact_event_exhibit_purchases]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per event exhibit purchase transaction.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.fact_event_exhibit_purchases
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
    NULL::BIGINT        AS event_key,                       -- TODO: no source mapping identified
    NULL::BIGINT        AS product_key,                     -- TODO: no source mapping identified
    NULL::BIGINT        AS event_exhibit_key,               -- TODO: no source mapping identified
    NULL::BIGINT        AS exhibitor_key,                   -- TODO: no source mapping identified
    NULL::BIGINT        AS event_exhibits_key,              -- TODO: no source mapping identified
    NULL::BIGINT        AS event_exhibit_purchases_key      -- TODO: no source mapping identified
WHERE 1 = 0;

CREATE TABLE IF NOT EXISTS newmodel2.dim_sales_orders (
    sales_orders_key     BIGINT       NOT NULL,
    sales_order_status   VARCHAR(50),
    sales_order_entity   VARCHAR(50),
    PRIMARY KEY (sales_orders_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_sales_lines (
    sales_lines_key   BIGINT       NOT NULL,
    sales_line_status VARCHAR(50),
    PRIMARY KEY (sales_lines_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.fact_sales_orders (
    sales_orders_quantity NUMERIC(18,4),
    sales_orders_amount   NUMERIC(18,4),
    sales_order_date      DATE,
    individual_key        BIGINT,
    company_key           BIGINT,
    geography_key         BIGINT,
    sales_orders_key      BIGINT,
    PRIMARY KEY (sales_orders_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.fact_sales_lines (
    sales_line_quantity NUMERIC(18,4),
    sales_line_amount   NUMERIC(18,4),
    sales_order_date    DATE,
    product_key         BIGINT,
    individual_key      BIGINT,
    company_key         BIGINT,
    geography_key       BIGINT,
    sales_orders_key    BIGINT,
    sales_lines_key     BIGINT,
    PRIMARY KEY (sales_lines_key)
);

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Sales]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_sales_orders]
-- [SOURCES: source.ams_rem_accounting_orders]
-- [GRAIN  : One row per accounting order header.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_sales_orders
(
    sales_orders_key,
    sales_order_status,
    sales_order_entity
)
SELECT
    CAST(s1.ams_rem_accounting_orders_key_sk AS BIGINT) AS sales_orders_key,   -- source: ams_rem_accounting_orders.ams_rem_accounting_orders_key_sk
    NULL::VARCHAR(50)                                   AS sales_order_status, -- TODO: no source mapping identified
    NULL::VARCHAR(50)                                   AS sales_order_entity  -- TODO: no source mapping identified
FROM source.ams_rem_accounting_orders AS s1;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Sales]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_sales_lines]
-- [SOURCES: source.ams_rem_accounting_lineitem]
-- [GRAIN  : One row per accounting line item.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_sales_lines
(
    sales_lines_key,
    sales_line_status
)
SELECT
    CAST(s1.ams_rem_accounting_lineitem_key_sk AS BIGINT) AS sales_lines_key,   -- source: ams_rem_accounting_lineitem.ams_rem_accounting_lineitem_key_sk
    NULL::VARCHAR(50)                                     AS sales_line_status  -- TODO: no source mapping identified
FROM source.ams_rem_accounting_lineitem AS s1;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Sales]
-- [TYPE   : fact]
-- [TARGET : newmodel2.fact_sales_orders]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per sales order fact.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.fact_sales_orders
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
    NULL::NUMERIC(18,4) AS sales_orders_quantity, -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS sales_orders_amount,   -- TODO: no source mapping identified
    NULL::DATE          AS sales_order_date,      -- TODO: no source mapping identified
    NULL::BIGINT        AS individual_key,        -- TODO: no source mapping identified
    NULL::BIGINT        AS company_key,           -- TODO: no source mapping identified
    NULL::BIGINT        AS geography_key,         -- TODO: no source mapping identified
    NULL::BIGINT        AS sales_orders_key       -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Sales]
-- [TYPE   : fact]
-- [TARGET : newmodel2.fact_sales_lines]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per sales line fact.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.fact_sales_lines
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
    NULL::NUMERIC(18,4) AS sales_line_quantity, -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS sales_line_amount,   -- TODO: no source mapping identified
    NULL::DATE          AS sales_order_date,    -- TODO: no source mapping identified
    NULL::BIGINT        AS product_key,         -- TODO: no source mapping identified
    NULL::BIGINT        AS individual_key,      -- TODO: no source mapping identified
    NULL::BIGINT        AS company_key,         -- TODO: no source mapping identified
    NULL::BIGINT        AS geography_key,       -- TODO: no source mapping identified
    NULL::BIGINT        AS sales_orders_key,    -- TODO: no source mapping identified
    NULL::BIGINT        AS sales_lines_key      -- TODO: no source mapping identified
WHERE 1 = 0;

CREATE TABLE IF NOT EXISTS newmodel2.dim_email (
    email_key              BIGINT       NOT NULL,
    email_code             VARCHAR(100),
    email_name             VARCHAR(1000),
    email_type             VARCHAR(255),
    email_subject          VARCHAR(1000),
    email_status           VARCHAR(50),
    email_send_first_date  DATE,
    email_send_last_date   DATE,
    PRIMARY KEY (email_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_email_sends (
    email_sends_key   BIGINT       NOT NULL,
    email_send_status VARCHAR(50),
    PRIMARY KEY (email_sends_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_email_opens (
    email_opens_key       BIGINT       NOT NULL,
    email_first_open_flag BOOLEAN,
    PRIMARY KEY (email_opens_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_email_clicks (
    email_clicks_key        BIGINT       NOT NULL,
    email_first_click_flag  BOOLEAN,
    email_click_url         VARCHAR(1000),
    email_click_domain      VARCHAR(255),
    PRIMARY KEY (email_clicks_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_email_summaries (
    email_summaries_key BIGINT       NOT NULL,
    email_summaries     VARCHAR(255),
    PRIMARY KEY (email_summaries_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.fact_email_sends (
    email_send_date       DATE,
    email_send_time       VARCHAR(50),
    email_key             BIGINT,
    email_send_status_key BIGINT,
    campaign_key          BIGINT,
    individual_key        BIGINT,
    company_key           BIGINT,
    geography_key         BIGINT,
    email_sends_key       BIGINT,
    PRIMARY KEY (email_sends_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.fact_email_opens (
    email_open_date  DATE,
    email_open_time  VARCHAR(50),
    email_key        BIGINT,
    campaign_key     BIGINT,
    individual_key   BIGINT,
    company_key      BIGINT,
    geography_key    BIGINT,
    email_opens_key  BIGINT,
    PRIMARY KEY (email_opens_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.fact_email_clicks (
    email_click_date  DATE,
    email_click_time  VARCHAR(50),
    email_key         BIGINT,
    campaign_key      BIGINT,
    individual_key    BIGINT,
    company_key       BIGINT,
    geography_key     BIGINT,
    email_clicks_key  BIGINT,
    PRIMARY KEY (email_clicks_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.fact_email_summaries (
    email_sends          NUMERIC(18,4),
    email_opens          NUMERIC(18,4),
    email_clicks         NUMERIC(18,4),
    email_bounces        NUMERIC(18,4),
    email_deliveries     NUMERIC(18,4),
    email_key            BIGINT,
    campaign_key         BIGINT,
    email_summaries_key  BIGINT,
    PRIMARY KEY (email_summaries_key)
);
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Email]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_email]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per email definition.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_email
(
    email_key,
    email_code,
    email_name,
    email_type,
    email_subject,
    email_status,
    email_send_first_date,
    email_send_last_date
)
SELECT
    NULL::BIGINT       AS email_key,              -- TODO: no source mapping identified
    NULL::VARCHAR(100) AS email_code,             -- TODO: no source mapping identified
    NULL::VARCHAR(1000) AS email_name,            -- TODO: no source mapping identified
    NULL::VARCHAR(255)  AS email_type,            -- TODO: no source mapping identified
    NULL::VARCHAR(1000) AS email_subject,         -- TODO: no source mapping identified
    NULL::VARCHAR(50)   AS email_status,          -- TODO: no source mapping identified
    NULL::DATE          AS email_send_first_date, -- TODO: no source mapping identified
    NULL::DATE          AS email_send_last_date   -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Email]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_email_sends]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per email send status.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_email_sends
(
    email_sends_key,
    email_send_status
)
SELECT
    NULL::BIGINT      AS email_sends_key,   -- TODO: no source mapping identified
    NULL::VARCHAR(50) AS email_send_status  -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Email]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_email_opens]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per email open classification.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_email_opens
(
    email_opens_key,
    email_first_open_flag
)
SELECT
    NULL::BIGINT AS email_opens_key,        -- TODO: no source mapping identified
    NULL::BOOLEAN AS email_first_open_flag  -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Email]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_email_clicks]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per email click classification.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_email_clicks
(
    email_clicks_key,
    email_first_click_flag,
    email_click_url,
    email_click_domain
)
SELECT
    NULL::BIGINT       AS email_clicks_key,       -- TODO: no source mapping identified
    NULL::BOOLEAN      AS email_first_click_flag, -- TODO: no source mapping identified
    NULL::VARCHAR(1000) AS email_click_url,       -- TODO: no source mapping identified
    NULL::VARCHAR(255)  AS email_click_domain     -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Email]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_email_summaries]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per email summary classification.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_email_summaries
(
    email_summaries_key,
    email_summaries
)
SELECT
    NULL::BIGINT      AS email_summaries_key, -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS email_summaries     -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Email]
-- [TYPE   : fact]
-- [TARGET : newmodel2.fact_email_sends]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per email send event.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.fact_email_sends
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
    NULL::DATE        AS email_send_date,       -- TODO: no source mapping identified
    NULL::VARCHAR(50) AS email_send_time,       -- TODO: no source mapping identified
    NULL::BIGINT      AS email_key,             -- TODO: no source mapping identified
    NULL::BIGINT      AS email_send_status_key, -- TODO: no source mapping identified
    NULL::BIGINT      AS campaign_key,          -- TODO: no source mapping identified
    NULL::BIGINT      AS individual_key,        -- TODO: no source mapping identified
    NULL::BIGINT      AS company_key,           -- TODO: no source mapping identified
    NULL::BIGINT      AS geography_key,         -- TODO: no source mapping identified
    NULL::BIGINT      AS email_sends_key        -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Email]
-- [TYPE   : fact]
-- [TARGET : newmodel2.fact_email_opens]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per email open event.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.fact_email_opens
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
    NULL::DATE        AS email_open_date,   -- TODO: no source mapping identified
    NULL::VARCHAR(50) AS email_open_time,   -- TODO: no source mapping identified
    NULL::BIGINT      AS email_key,         -- TODO: no source mapping identified
    NULL::BIGINT      AS campaign_key,      -- TODO: no source mapping identified
    NULL::BIGINT      AS individual_key,    -- TODO: no source mapping identified
    NULL::BIGINT      AS company_key,       -- TODO: no source mapping identified
    NULL::BIGINT      AS geography_key,     -- TODO: no source mapping identified
    NULL::BIGINT      AS email_opens_key    -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Email]
-- [TYPE   : fact]
-- [TARGET : newmodel2.fact_email_clicks]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per email click event.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.fact_email_clicks
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
    NULL::DATE        AS email_click_date,  -- TODO: no source mapping identified
    NULL::VARCHAR(50) AS email_click_time,  -- TODO: no source mapping identified
    NULL::BIGINT      AS email_key,         -- TODO: no source mapping identified
    NULL::BIGINT      AS campaign_key,      -- TODO: no source mapping identified
    NULL::BIGINT      AS individual_key,    -- TODO: no source mapping identified
    NULL::BIGINT      AS company_key,       -- TODO: no source mapping identified
    NULL::BIGINT      AS geography_key,     -- TODO: no source mapping identified
    NULL::BIGINT      AS email_clicks_key   -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Email]
-- [TYPE   : fact]
-- [TARGET : newmodel2.fact_email_summaries]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per email engagement summary.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.fact_email_summaries
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
    NULL::NUMERIC(18,4) AS email_sends,         -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS email_opens,         -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS email_clicks,        -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS email_bounces,       -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS email_deliveries,    -- TODO: no source mapping identified
    NULL::BIGINT        AS email_key,           -- TODO: no source mapping identified
    NULL::BIGINT        AS campaign_key,        -- TODO: no source mapping identified
    NULL::BIGINT        AS email_summaries_key  -- TODO: no source mapping identified
WHERE 1 = 0;

CREATE TABLE IF NOT EXISTS newmodel2.dim_web_sessions (
    web_sessions_key      BIGINT       NOT NULL,
    session_id            VARCHAR(255),
    traffic_source_type   VARCHAR(255),
    traffic_medium        VARCHAR(255),
    traffic_source        VARCHAR(255),
    hostname              VARCHAR(255),
    PRIMARY KEY (web_sessions_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_web_pageviews (
    web_pageviews_key BIGINT       NOT NULL,
    page_sequence     INTEGER,
    PRIMARY KEY (web_pageviews_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_web_page (
    web_page_key     BIGINT       NOT NULL,
    page_title       VARCHAR(1000),
    page_location_1  VARCHAR(255),
    page_location_2  VARCHAR(255),
    page_location_3  VARCHAR(255),
    page_location_4  VARCHAR(255),
    page_location_5  VARCHAR(255),
    page_referrer    VARCHAR(1000),
    PRIMARY KEY (web_page_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.fact_web_sessions (
    session_duration_seconds NUMERIC(18,4),
    session_date             DATE,
    session_time             VARCHAR(50),
    session_datetime         TIMESTAMP,
    geography_key            BIGINT,
    web_sessions_key         BIGINT,
    PRIMARY KEY (web_sessions_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.fact_web_pageviews (
    pageview_duration_seconds NUMERIC(18,4),
    pageview_start_date       DATE,
    pageview_start_time       VARCHAR(50),
    pageview_start_datetime   TIMESTAMP,
    geography_key             BIGINT,
    web_page_key              BIGINT,
    web_sessions_key          BIGINT,
    web_pageviews_key         BIGINT,
    PRIMARY KEY (web_pageviews_key)
);

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Web]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_web_sessions]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per web session.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_web_sessions
(
    web_sessions_key,
    session_id,
    traffic_source_type,
    traffic_medium,
    traffic_source,
    hostname
)
SELECT
    NULL::BIGINT       AS web_sessions_key,    -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS session_id,          -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS traffic_source_type, -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS traffic_medium,      -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS traffic_source,      -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS hostname             -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Web]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_web_pageviews]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per web pageview sequence.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_web_pageviews
(
    web_pageviews_key,
    page_sequence
)
SELECT
    NULL::BIGINT AS web_pageviews_key, -- TODO: no source mapping identified
    NULL::INTEGER AS page_sequence     -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Web]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_web_page]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per web page.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_web_page
(
    web_page_key,
    page_title,
    page_location_1,
    page_location_2,
    page_location_3,
    page_location_4,
    page_location_5,
    page_referrer
)
SELECT
    NULL::BIGINT       AS web_page_key,     -- TODO: no source mapping identified
    NULL::VARCHAR(1000) AS page_title,      -- TODO: no source mapping identified
    NULL::VARCHAR(255)  AS page_location_1, -- TODO: no source mapping identified
    NULL::VARCHAR(255)  AS page_location_2, -- TODO: no source mapping identified
    NULL::VARCHAR(255)  AS page_location_3, -- TODO: no source mapping identified
    NULL::VARCHAR(255)  AS page_location_4, -- TODO: no source mapping identified
    NULL::VARCHAR(255)  AS page_location_5, -- TODO: no source mapping identified
    NULL::VARCHAR(1000) AS page_referrer    -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Web]
-- [TYPE   : fact]
-- [TARGET : newmodel2.fact_web_sessions]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per web session event.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.fact_web_sessions
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
    NULL::BIGINT        AS geography_key,            -- TODO: no source mapping identified
    NULL::BIGINT        AS web_sessions_key          -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Web]
-- [TYPE   : fact]
-- [TARGET : newmodel2.fact_web_pageviews]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per web pageview event.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.fact_web_pageviews
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
    NULL::BIGINT        AS geography_key,             -- TODO: no source mapping identified
    NULL::BIGINT        AS web_page_key,              -- TODO: no source mapping identified
    NULL::BIGINT        AS web_sessions_key,          -- TODO: no source mapping identified
    NULL::BIGINT        AS web_pageviews_key          -- TODO: no source mapping identified
WHERE 1 = 0;

CREATE TABLE IF NOT EXISTS newmodel2.dim_community_activities (
    community_activities_key    BIGINT       NOT NULL,
    community_activity_description VARCHAR(1000),
    PRIMARY KEY (community_activities_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_community_discussion_posts (
    community_discussion_posts_key BIGINT       NOT NULL,
    discussion_post_subject        VARCHAR(1000),
    discussion_post_content        VARCHAR(4000),
    discussion_post_type           VARCHAR(255),
    discussion_post_status         VARCHAR(50),
    PRIMARY KEY (community_discussion_posts_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_community_discussions (
    community_discussions_key BIGINT       NOT NULL,
    PRIMARY KEY (community_discussions_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_community_memberships (
    community_memberships_key BIGINT       NOT NULL,
    community_membership_type VARCHAR(255),
    PRIMARY KEY (community_memberships_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.fact_community_activities (
    community_activity_date      DATE,
    community_activity_time      VARCHAR(50),
    community_activity_type_key  BIGINT,
    individual_key               BIGINT,
    company_key                  BIGINT,
    geography_key                BIGINT,
    community_activities_key     BIGINT,
    PRIMARY KEY (community_activities_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.fact_community_discussion_posts (
    community_discussion_post_date DATE,
    community_discussion_post_time VARCHAR(50),
    community_discussions_key      BIGINT,
    individual_key                 BIGINT,
    company_key                    BIGINT,
    geography_key                  BIGINT,
    community_discussion_posts_key BIGINT,
    PRIMARY KEY (community_discussion_posts_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.fact_community_discussions (
    community_discussion_post_count  NUMERIC(18,4),
    community_discussion_first_date  DATE,
    community_discussion_first_time  VARCHAR(50),
    community_discussion_last_date   DATE,
    community_discussion_last_time   VARCHAR(50),
    community_key                    BIGINT,
    community_discussions_key        BIGINT,
    PRIMARY KEY (community_discussions_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.fact_community_memberships (
    community_membership_date  DATE,
    community_key              BIGINT,
    individual_key             BIGINT,
    company_key                BIGINT,
    geography_key              BIGINT,
    community_memberships_key  BIGINT,
    PRIMARY KEY (community_memberships_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_sponsorships (
    sponsorships_key   BIGINT       NOT NULL,
    sponsorship_status VARCHAR(50),
    PRIMARY KEY (sponsorships_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_sponsorship (
    sponsorship_key   BIGINT       NOT NULL,
    sponsorship_code  VARCHAR(100),
    sponsorship_name  VARCHAR(1000),
    sponsorship_type  VARCHAR(255),
    sponsorship_level VARCHAR(255),
    sponsorship_status VARCHAR(50),
    PRIMARY KEY (sponsorship_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_exhibit (
    exhibit_key   BIGINT       NOT NULL,
    exhibit_code  VARCHAR(100),
    exhibit_name  VARCHAR(1000),
    exhibit_type  VARCHAR(255),
    exhibit_status VARCHAR(50),
    PRIMARY KEY (exhibit_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_exhibitor (
    exhibitor_key          BIGINT       NOT NULL,
    exhibitor_id           VARCHAR(100),
    exhibitor_name         VARCHAR(1000),
    exhibitor_type         VARCHAR(255),
    exhibitor_busines_area VARCHAR(255),
    exhibitor_status       VARCHAR(50),
    exhibitor_geography_key BIGINT,
    PRIMARY KEY (exhibitor_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.dim_sponsor (
    sponsor_key          BIGINT       NOT NULL,
    sponsor_id           VARCHAR(100),
    sponsor_name         VARCHAR(1000),
    sponsor_type         VARCHAR(255),
    sponsor_busines_area VARCHAR(255),
    sponsor_status       VARCHAR(50),
    sponsor_geography_key BIGINT,
    PRIMARY KEY (sponsor_key)
);

CREATE TABLE IF NOT EXISTS newmodel2.fact_sponsorships (
    sponsorship_quanity      NUMERIC(18,4),
    sponsorship_amount       NUMERIC(18,4),
    sponsorship_purchase_date DATE,
    sponsorship_start_date    DATE,
    sponsorship_end_date      DATE,
    product_key              BIGINT,
    sponsor_key              BIGINT,
    sponsorships_key         BIGINT,
    PRIMARY KEY (sponsorships_key)
);

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Community]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_community_activities]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per community activity type.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_community_activities
(
    community_activities_key,
    community_activity_description
)
SELECT
    NULL::BIGINT        AS community_activities_key,    -- TODO: no source mapping identified
    NULL::VARCHAR(1000) AS community_activity_description -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Community]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_community_discussion_posts]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per community discussion post.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_community_discussion_posts
(
    community_discussion_posts_key,
    discussion_post_subject,
    discussion_post_content,
    discussion_post_type,
    discussion_post_status
)
SELECT
    NULL::BIGINT        AS community_discussion_posts_key, -- TODO: no source mapping identified
    NULL::VARCHAR(1000) AS discussion_post_subject,        -- TODO: no source mapping identified
    NULL::VARCHAR(4000) AS discussion_post_content,        -- TODO: no source mapping identified
    NULL::VARCHAR(255)  AS discussion_post_type,           -- TODO: no source mapping identified
    NULL::VARCHAR(50)   AS discussion_post_status          -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Community]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_community_discussions]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per community discussion thread.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_community_discussions
(
    community_discussions_key
)
SELECT
    NULL::BIGINT AS community_discussions_key -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Community]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_community_memberships]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per community membership type.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_community_memberships
(
    community_memberships_key,
    community_membership_type
)
SELECT
    NULL::BIGINT      AS community_memberships_key, -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS community_membership_type -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Community]
-- [TYPE   : fact]
-- [TARGET : newmodel2.fact_community_activities]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per community activity event.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.fact_community_activities
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
    NULL::DATE        AS community_activity_date,     -- TODO: no source mapping identified
    NULL::VARCHAR(50) AS community_activity_time,     -- TODO: no source mapping identified
    NULL::BIGINT      AS community_activity_type_key, -- TODO: no source mapping identified
    NULL::BIGINT      AS individual_key,              -- TODO: no source mapping identified
    NULL::BIGINT      AS company_key,                 -- TODO: no source mapping identified
    NULL::BIGINT      AS geography_key,               -- TODO: no source mapping identified
    NULL::BIGINT      AS community_activities_key     -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Community]
-- [TYPE   : fact]
-- [TARGET : newmodel2.fact_community_discussion_posts]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per community discussion post event.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.fact_community_discussion_posts
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
    NULL::DATE        AS community_discussion_post_date, -- TODO: no source mapping identified
    NULL::VARCHAR(50) AS community_discussion_post_time, -- TODO: no source mapping identified
    NULL::BIGINT      AS community_discussions_key,      -- TODO: no source mapping identified
    NULL::BIGINT      AS individual_key,                 -- TODO: no source mapping identified
    NULL::BIGINT      AS company_key,                    -- TODO: no source mapping identified
    NULL::BIGINT      AS geography_key,                  -- TODO: no source mapping identified
    NULL::BIGINT      AS community_discussion_posts_key  -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Community]
-- [TYPE   : fact]
-- [TARGET : newmodel2.fact_community_discussions]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per community discussion aggregate.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.fact_community_discussions
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
    NULL::BIGINT        AS community_key,                   -- TODO: no source mapping identified
    NULL::BIGINT        AS community_discussions_key        -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Community]
-- [TYPE   : fact]
-- [TARGET : newmodel2.fact_community_memberships]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per community membership event.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.fact_community_memberships
(
    community_membership_date,
    community_key,
    individual_key,
    company_key,
    geography_key,
    community_memberships_key
)
SELECT
    NULL::DATE   AS community_membership_date, -- TODO: no source mapping identified
    NULL::BIGINT AS community_key,             -- TODO: no source mapping identified
    NULL::BIGINT AS individual_key,            -- TODO: no source mapping identified
    NULL::BIGINT AS company_key,               -- TODO: no source mapping identified
    NULL::BIGINT AS geography_key,             -- TODO: no source mapping identified
    NULL::BIGINT AS community_memberships_key  -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Sponsorships]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_sponsorships]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per sponsorship status.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_sponsorships
(
    sponsorships_key,
    sponsorship_status
)
SELECT
    NULL::BIGINT     AS sponsorships_key,   -- TODO: no source mapping identified
    NULL::VARCHAR(50) AS sponsorship_status -- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Sponsorships]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_sponsorship]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per sponsorship product.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_sponsorship
(
    sponsorship_key,
    sponsorship_code,
    sponsorship_name,
    sponsorship_type,
    sponsorship_level,
    sponsorship_status
)
SELECT
    NULL::BIGINT       AS sponsorship_key,   -- TODO: no source mapping identified
    NULL::VARCHAR(100) AS sponsorship_code,  -- TODO: no source mapping identified
    NULL::VARCHAR(1000) AS sponsorship_name, -- TODO: no source mapping identified
    NULL::VARCHAR(255)  AS sponsorship_type, -- TODO: no source mapping identified
    NULL::VARCHAR(255)  AS sponsorship_level,-- TODO: no source mapping identified
    NULL::VARCHAR(50)   AS sponsorship_status-- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Sponsorships]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_exhibit]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per exhibit product.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_exhibit
(
    exhibit_key,
    exhibit_code,
    exhibit_name,
    exhibit_type,
    exhibit_status
)
SELECT
    NULL::BIGINT       AS exhibit_key,   -- TODO: no source mapping identified
    NULL::VARCHAR(100) AS exhibit_code,  -- TODO: no source mapping identified
    NULL::VARCHAR(1000) AS exhibit_name, -- TODO: no source mapping identified
    NULL::VARCHAR(255)  AS exhibit_type, -- TODO: no source mapping identified
    NULL::VARCHAR(50)   AS exhibit_status-- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Sponsorships]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_exhibitor]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per exhibitor organization.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_exhibitor
(
    exhibitor_key,
    exhibitor_id,
    exhibitor_name,
    exhibitor_type,
    exhibitor_busines_area,
    exhibitor_status,
    exhibitor_geography_key
)
SELECT
    NULL::BIGINT       AS exhibitor_key,          -- TODO: no source mapping identified
    NULL::VARCHAR(100) AS exhibitor_id,           -- TODO: no source mapping identified
    NULL::VARCHAR(1000) AS exhibitor_name,        -- TODO: no source mapping identified
    NULL::VARCHAR(255)  AS exhibitor_type,        -- TODO: no source mapping identified
    NULL::VARCHAR(255)  AS exhibitor_busines_area,-- TODO: no source mapping identified
    NULL::VARCHAR(50)   AS exhibitor_status,      -- TODO: no source mapping identified
    NULL::BIGINT        AS exhibitor_geography_key-- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Sponsorships]
-- [TYPE   : dimension]
-- [TARGET : newmodel2.dim_sponsor]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per sponsor organization.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.dim_sponsor
(
    sponsor_key,
    sponsor_id,
    sponsor_name,
    sponsor_type,
    sponsor_busines_area,
    sponsor_status,
    sponsor_geography_key
)
SELECT
    NULL::BIGINT       AS sponsor_key,          -- TODO: no source mapping identified
    NULL::VARCHAR(100) AS sponsor_id,           -- TODO: no source mapping identified
    NULL::VARCHAR(1000) AS sponsor_name,        -- TODO: no source mapping identified
    NULL::VARCHAR(255)  AS sponsor_type,        -- TODO: no source mapping identified
    NULL::VARCHAR(255)  AS sponsor_busines_area,-- TODO: no source mapping identified
    NULL::VARCHAR(50)   AS sponsor_status,      -- TODO: no source mapping identified
    NULL::BIGINT        AS sponsor_geography_key-- TODO: no source mapping identified
WHERE 1 = 0;

-- ────────────────────────────────────────────────────────
-- [DOMAIN : Sponsorships]
-- [TYPE   : fact]
-- [TARGET : newmodel2.fact_sponsorships]
-- [SOURCES: (no reliable source identified)]
-- [GRAIN  : One row per sponsorship purchase event.]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel2.fact_sponsorships
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
    NULL::NUMERIC(18,4) AS sponsorship_quanity,      -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS sponsorship_amount,       -- TODO: no source mapping identified
    NULL::DATE          AS sponsorship_purchase_date,-- TODO: no source mapping identified
    NULL::DATE          AS sponsorship_start_date,   -- TODO: no source mapping identified
    NULL::DATE          AS sponsorship_end_date,     -- TODO: no source mapping identified
    NULL::BIGINT        AS product_key,              -- TODO: no source mapping identified
    NULL::BIGINT        AS sponsor_key,              -- TODO: no source mapping identified
    NULL::BIGINT        AS sponsorships_key          -- TODO: no source mapping identified
WHERE 1 = 0;


