CREATE SCHEMA IF NOT EXISTS newmodel;

-- ===============================================================
-- Membership Domain - DDL
-- ===============================================================

-- Dimension: dim_individual
CREATE TABLE IF NOT EXISTS newmodel.dim_individual
(
    individual_key         BIGINT       NOT NULL,
    individual_id          VARCHAR(36)  NOT NULL,
    individual_last_name   VARCHAR(255),
    individual_first_name  VARCHAR(255),
    individual_job_function VARCHAR(255),
    individual_job_level   VARCHAR(255),
    individual_status      VARCHAR(50),
    individual_company_key BIGINT,
    individual_geography_key BIGINT,
    PRIMARY KEY (individual_key)
);

-- Dimension: dim_company
CREATE TABLE IF NOT EXISTS newmodel.dim_company
(
    company_key          BIGINT       NOT NULL,
    company_id           VARCHAR(36)  NOT NULL,
    company_name         VARCHAR(255),
    company_type         VARCHAR(255),
    company_busines_area VARCHAR(255),
    company_status       VARCHAR(50),
    company_geography_key BIGINT,
    PRIMARY KEY (company_key)
);

-- Dimension: dim_membership_type
CREATE TABLE IF NOT EXISTS newmodel.dim_membership_type
(
    membership_type          BIGINT      NOT NULL,
    membership_type_name     VARCHAR(255),
    membership_type_category VARCHAR(255),
    membership_type_entity   VARCHAR(255),
    PRIMARY KEY (membership_type)
);

-- Dimension: dim_memberships
CREATE TABLE IF NOT EXISTS newmodel.dim_memberships
(
    memberships_key         BIGINT      NOT NULL,
    membership_status       VARCHAR(255),
    membership_lifecycle    VARCHAR(255),
    membership_lifecycle_next VARCHAR(255),
    membership_entity       VARCHAR(255),
    PRIMARY KEY (memberships_key)
);

-- Fact: fact_memberships
CREATE TABLE IF NOT EXISTS newmodel.fact_memberships
(
    memberships_key         BIGINT      NOT NULL,
    membership_quantity     NUMERIC(18,4),
    membership_amount       NUMERIC(18,4),
    membership_start_date   DATE,
    membership_end_date     DATE,
    membership_grace_date   DATE,
    membership_type_key     BIGINT,
    product_key             BIGINT,
    individual_key          BIGINT,
    company_key             BIGINT,
    geography_key           BIGINT,
    PRIMARY KEY (memberships_key)
    -- TODO: enable foreign keys after data validation
    -- , FOREIGN KEY (membership_type_key) REFERENCES newmodel.dim_membership_type (membership_type)
    -- , FOREIGN KEY (individual_key) REFERENCES newmodel.dim_individual (individual_key)
    -- , FOREIGN KEY (company_key) REFERENCES newmodel.dim_company (company_key)
);

-- ===============================================================
-- Membership Domain - ETL
-- ===============================================================

-- Dimension: dim_individual
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Membership]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_individual]
-- [SOURCES: source.ams_rem_crm_individual]
-- [GRAIN  : one row per individual record in ams_rem_crm_individual]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_individual
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
    CAST(s1.ams_rem_crm_individual_key_sk AS BIGINT) AS individual_key,  -- source: ams_rem_crm_individual.ams_rem_crm_individual_key_sk
    CAST(s1.id AS VARCHAR(36))                         AS individual_id,  -- source: ams_rem_crm_individual.id
    CAST(s1.lastname AS VARCHAR(255))                  AS individual_last_name,  -- source: ams_rem_crm_individual.lastname
    CAST(s1.firstname AS VARCHAR(255))                 AS individual_first_name, -- source: ams_rem_crm_individual.firstname
    NULL::VARCHAR(255)                                 AS individual_job_function, -- TODO: derive from job-related tables (e.g., ams_rem_crm_customerjobrole)
    NULL::VARCHAR(255)                                 AS individual_job_level,    -- TODO: derive from job-related tables (e.g., ams_rem_crm_customerjobrole)
    CAST(s1.meta_record_status AS VARCHAR(50))         AS individual_status,      -- source: ams_rem_crm_individual.meta_record_status
    NULL::BIGINT                                       AS individual_company_key,  -- TODO: join to organization/company dimension once available
    NULL::BIGINT                                       AS individual_geography_key -- TODO: join to geography dimension once available
FROM source.ams_rem_crm_individual s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE
    AND s1.id IS NOT NULL;


-- Dimension: dim_company
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Membership]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_company]
-- [SOURCES: source.ams_rem_crm_organization]
-- [GRAIN  : one row per organization record in ams_rem_crm_organization]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_company
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
    CAST(s1.ams_rem_crm_organization_key_sk AS BIGINT) AS company_key,   -- source: ams_rem_crm_organization.ams_rem_crm_organization_key_sk
    CAST(s1.id AS VARCHAR(36))                        AS company_id,     -- source: ams_rem_crm_organization.id
    CAST(s1.name AS VARCHAR(255))                     AS company_name,   -- source: ams_rem_crm_organization.name
    NULL::VARCHAR(255)                                AS company_type,   -- TODO: derive from organization type columns if available
    NULL::VARCHAR(255)                                AS company_busines_area, -- TODO: derive from related CRM attributes
    CAST(s1.meta_record_status AS VARCHAR(50))        AS company_status, -- source: ams_rem_crm_organization.meta_record_status
    NULL::BIGINT                                      AS company_geography_key  -- TODO: join to geography dimension once available
FROM source.ams_rem_crm_organization s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE
    AND s1.id IS NOT NULL;


-- Dimension: dim_membership_type
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Membership]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_membership_type]
-- [SOURCES: source.ams_rem_shopping_membership]
-- [GRAIN  : one row per membership product definition in ams_rem_shopping_membership]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_membership_type
(
    membership_type,
    membership_type_name,
    membership_type_category,
    membership_type_entity
)
SELECT
    CAST(s1.ams_rem_shopping_membership_key_sk AS BIGINT) AS membership_type,      -- source: ams_rem_shopping_membership.ams_rem_shopping_membership_key_sk
    CAST(s1.name AS VARCHAR(255))                         AS membership_type_name, -- source: ams_rem_shopping_membership.name
    CAST(s1.renewalproductname AS VARCHAR(255))           AS membership_type_category, -- derived: use renewalproductname as a proxy for category
    CASE                                                  -- derived: rough entity classification based on ischapter flag
        WHEN COALESCE(s1.ischapter, FALSE) = TRUE THEN 'Chapter'
        ELSE 'Core'
    END::VARCHAR(255)                                     AS membership_type_entity
FROM source.ams_rem_shopping_membership s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE
    AND s1.id IS NOT NULL;


-- Dimension: dim_memberships
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Membership]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_memberships]
-- [SOURCES: source.ams_rem_crm_membershipbenefit]
-- [GRAIN  : one row per customer-membership benefit record in ams_rem_crm_membershipbenefit]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_memberships
(
    memberships_key,
    membership_status,
    membership_lifecycle,
    membership_lifecycle_next,
    membership_entity
)
SELECT
    CAST(s1.ams_rem_crm_membershipbenefit_key_sk AS BIGINT) AS memberships_key,   -- source: ams_rem_crm_membershipbenefit.ams_rem_crm_membershipbenefit_key_sk
    CAST(s1.currentstatusname AS VARCHAR(255))              AS membership_status, -- source: ams_rem_crm_membershipbenefit.currentstatusname
    CAST(s1.effectivedate AS VARCHAR(255))                  AS membership_lifecycle,      -- derived: placeholder using effectivedate; refine lifecycle logic later
    CAST(s1.expiredate AS VARCHAR(255))                     AS membership_lifecycle_next, -- derived: placeholder using expiredate; refine lifecycle-next logic later
    CAST(s1.membershipname AS VARCHAR(255))                 AS membership_entity          -- derived: use membershipname to describe membership entity
FROM source.ams_rem_crm_membershipbenefit s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE
    AND s1.id IS NOT NULL;


-- Fact: fact_memberships
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Membership]
-- [TYPE   : fact]
-- [TARGET : newmodel.fact_memberships]
-- [SOURCES: source.ams_rem_crm_membershipbenefit]
-- [GRAIN  : one row per customer-membership benefit record (membershipbenefit_key_sk) in ams_rem_crm_membershipbenefit]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.fact_memberships
(
    memberships_key,
    membership_quantity,
    membership_amount,
    membership_start_date,
    membership_end_date,
    membership_grace_date,
    membership_type_key,
    product_key,
    individual_key,
    company_key,
    geography_key
)
SELECT
    CAST(s1.ams_rem_crm_membershipbenefit_key_sk AS BIGINT)                     AS memberships_key,       -- source: ams_rem_crm_membershipbenefit.ams_rem_crm_membershipbenefit_key_sk
    1::NUMERIC(18,4)                                                            AS membership_quantity,   -- derived: assume one unit per membership benefit record
    NULL::NUMERIC(18,4)                                                         AS membership_amount,     -- TODO: join to accounting line item amounts for membership revenue
    COALESCE(TO_DATE(s1.joindate, 'YYYY-MM-DD'), NULL)::DATE                    AS membership_start_date, -- source: ams_rem_crm_membershipbenefit.joindate
    COALESCE(TO_DATE(s1.expiredate, 'YYYY-MM-DD'), NULL)::DATE                  AS membership_end_date,   -- source: ams_rem_crm_membershipbenefit.expiredate
    COALESCE(TO_DATE(s1.terminationdate, 'YYYY-MM-DD'), NULL)::DATE             AS membership_grace_date, -- derived: placeholder using terminationdate; refine grace rules later
    CAST(s2.ams_rem_shopping_membership_key_sk AS BIGINT)                       AS membership_type_key,   -- derived: join membershipproductid to shopping membership
    NULL::BIGINT                                                                AS product_key,           -- TODO: map to dim_product using productid/membershipproductid
    NULL::BIGINT                                                                AS individual_key,        -- TODO: derive via customerid -> individual
    NULL::BIGINT                                                                AS company_key,           -- TODO: derive via customerid -> organization
    NULL::BIGINT                                                                AS geography_key          -- TODO: derive via address/region tables
FROM source.ams_rem_crm_membershipbenefit s1
LEFT JOIN source.ams_rem_shopping_membership s2
    ON s1.membershipproductid = s2.id
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE
    AND s1.customerid IS NOT NULL
    AND s1.meta_record_status = 'A';

-- ===============================================================
-- Event Domain - DDL
-- ===============================================================

-- Dimension: dim_event
CREATE TABLE IF NOT EXISTS newmodel.dim_event
(
    event_key       BIGINT      NOT NULL,
    event_code      VARCHAR(100),
    event_name      VARCHAR(255),
    event_type      VARCHAR(100),
    event_status    VARCHAR(50),
    event_location  VARCHAR(255),
    event_start_date DATE,
    event_end_date   DATE,
    PRIMARY KEY (event_key)
);

-- Fact: fact_event_registrations
CREATE TABLE IF NOT EXISTS newmodel.fact_event_registrations
(
    event_registrations_key BIGINT      NOT NULL,
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
    PRIMARY KEY (event_registrations_key)
);

-- Fact: fact_event_sessions
CREATE TABLE IF NOT EXISTS newmodel.fact_event_sessions
(
    event_sessions_key        BIGINT      NOT NULL,
    session_amount            NUMERIC(18,4),
    event_distance            NUMERIC(18,4),
    event_registration_date   DATE,
    session_registration_date DATE,
    session_start_date        DATE,
    session_end_date          DATE,
    session_start_time        VARCHAR(20),
    session_end_time          VARCHAR(20),
    session_key               BIGINT,
    event_key                 BIGINT,
    product_key               BIGINT,
    location_key              BIGINT,
    individual_key            BIGINT,
    company_key               BIGINT,
    geography_key             BIGINT,
    event_geography_key       BIGINT,
    event_registration_key    BIGINT,
    PRIMARY KEY (event_sessions_key)
);

-- Fact: fact_event_purchases
CREATE TABLE IF NOT EXISTS newmodel.fact_event_purchases
(
    event_purchases_key    BIGINT      NOT NULL,
    purchase_quantity      NUMERIC(18,4),
    purchase_amount        NUMERIC(18,4),
    purchase_date          DATE,
    event_registration_date DATE,
    event_distance         NUMERIC(18,4),
    event_key              BIGINT,
    product_key            BIGINT,
    individual_key         BIGINT,
    company_key            BIGINT,
    geography_key          BIGINT,
    event_geography_key    BIGINT,
    event_registration_key BIGINT,
    event_sessions_key     BIGINT,
    PRIMARY KEY (event_purchases_key)
);

-- ===============================================================
-- Event Domain - ETL
-- ===============================================================

-- Dimension: dim_event
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Events]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_event]
-- [SOURCES: source.ams_rem_shopping_event]
-- [GRAIN  : one row per event in ams_rem_shopping_event]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_event
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
    CAST(s1.ams_rem_shopping_event_key_sk AS BIGINT)                  AS event_key,      -- source: ams_rem_shopping_event.ams_rem_shopping_event_key_sk
    CAST(s1.id AS VARCHAR(100))                                       AS event_code,     -- source: ams_rem_shopping_event.id (acts as event code)
    CAST(s1.name AS VARCHAR(255))                                     AS event_name,     -- source: ams_rem_shopping_event.name
    CAST(s1.type AS VARCHAR(100))                                     AS event_type,     -- source: ams_rem_shopping_event.type
    CAST(s1.meta_record_status AS VARCHAR(50))                        AS event_status,   -- source: ams_rem_shopping_event.meta_record_status
    CAST(s1.timezonename AS VARCHAR(255))                             AS event_location, -- derived: placeholder, refine with venue/location joins later
    COALESCE(TO_DATE(s1.eventstartdate, 'YYYY-MM-DD'), NULL)::DATE    AS event_start_date, -- source: ams_rem_shopping_event.eventstartdate
    COALESCE(TO_DATE(s1.eventenddate, 'YYYY-MM-DD'), NULL)::DATE      AS event_end_date    -- source: ams_rem_shopping_event.eventenddate
FROM source.ams_rem_shopping_event s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE
    AND s1.id IS NOT NULL;


-- Fact: fact_event_registrations
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Events]
-- [TYPE   : fact]
-- [TARGET : newmodel.fact_event_registrations]
-- [SOURCES: source.ams_rem_purchase_allregistrations]
-- [GRAIN  : one row per registration record in ams_rem_purchase_allregistrations]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.fact_event_registrations
(
    event_registrations_key,
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
    event_geography_key
)
SELECT
    CAST(s1.ams_rem_purchase_allregistrations_key_sk AS BIGINT)       AS event_registrations_key, -- source: ams_rem_purchase_allregistrations_key_sk
    1::NUMERIC(18,4)                                                  AS registration_quantity,   -- derived: assume one attendee per row
    NULL::NUMERIC(18,4)                                               AS registration_amount,     -- TODO: join to accounting line items for revenue
    NULL::NUMERIC(18,4)                                               AS event_distance,          -- TODO: derive using geography data if available
    COALESCE(TO_DATE(s1.createdon, 'YYYY-MM-DD'), NULL)::DATE         AS event_registration_date, -- source: createdon
    COALESCE(TO_DATE(s1.attendeddate, 'YYYY-MM-DD'), NULL)::DATE      AS event_start_date,        -- derived: use attendeddate as event start proxy
    NULL::BIGINT                                                      AS event_key,               -- TODO: join to dim_event via event/product ids
    NULL::BIGINT                                                      AS product_key,             -- TODO: derive from lineitemid/product tables
    NULL::BIGINT                                                      AS individual_key,          -- TODO: customerid -> individual/company resolution
    NULL::BIGINT                                                      AS company_key,             -- TODO: customerid -> organization resolution
    NULL::BIGINT                                                      AS geography_key,           -- TODO: map from badge address fields
    NULL::BIGINT                                                      AS event_geography_key      -- TODO: map from venue/location
FROM source.ams_rem_purchase_allregistrations s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE
    AND s1.meta_record_status = 'A';


-- Fact: fact_event_sessions
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Events]
-- [TYPE   : fact]
-- [TARGET : newmodel.fact_event_sessions]
-- [SOURCES: source.ams_rem_purchase_registrationpurchase]
-- [GRAIN  : one row per registration purchase (session-level) in ams_rem_purchase_registrationpurchase]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.fact_event_sessions
(
    event_sessions_key,
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
    event_registration_key
)
SELECT
    CAST(s1.ams_rem_purchase_registrationpurchase_key_sk AS BIGINT)   AS event_sessions_key,      -- source: ams_rem_purchase_registrationpurchase_key_sk
    NULL::NUMERIC(18,4)                                               AS session_amount,          -- TODO: join to accounting line items for session revenue
    NULL::NUMERIC(18,4)                                               AS event_distance,          -- TODO: derive using geography data
    COALESCE(TO_DATE(s1.createdon, 'YYYY-MM-DD'), NULL)::DATE         AS event_registration_date, -- source: createdon
    COALESCE(TO_DATE(s1.createdon, 'YYYY-MM-DD'), NULL)::DATE         AS session_registration_date, -- derived: use createdon as registration date
    COALESCE(TO_DATE(s1.attendeddate, 'YYYY-MM-DD'), NULL)::DATE      AS session_start_date,      -- source: attendeddate
    COALESCE(TO_DATE(s1.canceldate, 'YYYY-MM-DD'), NULL)::DATE        AS session_end_date,        -- source: canceldate
    NULL::VARCHAR(20)                                                 AS session_start_time,      -- TODO: derive from time fields if/when available
    NULL::VARCHAR(20)                                                 AS session_end_time,        -- TODO: derive from time fields if/when available
    NULL::BIGINT                                                      AS session_key,             -- TODO: join to session/product tables
    NULL::BIGINT                                                      AS event_key,               -- TODO: join to dim_event
    NULL::BIGINT                                                      AS product_key,             -- TODO: derive from lineitemid/product tables
    NULL::BIGINT                                                      AS location_key,            -- TODO: join via venue/registrationproductvenue
    NULL::BIGINT                                                      AS individual_key,          -- TODO: map customerid
    NULL::BIGINT                                                      AS company_key,             -- TODO: map customerid to organization
    NULL::BIGINT                                                      AS geography_key,           -- TODO: derive from badge address
    NULL::BIGINT                                                      AS event_geography_key,     -- TODO: derive from venue/location
    NULL::BIGINT                                                      AS event_registration_key   -- TODO: link back to fact_event_registrations
FROM source.ams_rem_purchase_registrationpurchase s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE
    AND s1.meta_record_status = 'A';


-- Fact: fact_event_purchases
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Events]
-- [TYPE   : fact]
-- [TARGET : newmodel.fact_event_purchases]
-- [SOURCES: source.ams_rem_purchase_registrationpurchase]
-- [GRAIN  : one row per add-on purchase tied to a registration purchase]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.fact_event_purchases
(
    event_purchases_key,
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
    CAST(s1.ams_rem_purchase_registrationpurchase_key_sk AS BIGINT)   AS event_purchases_key,     -- source: ams_rem_purchase_registrationpurchase_key_sk
    1::NUMERIC(18,4)                                                  AS purchase_quantity,       -- derived: assume one unit per purchase row
    NULL::NUMERIC(18,4)                                               AS purchase_amount,         -- TODO: join to accounting amounts
    COALESCE(TO_DATE(s1.createdon, 'YYYY-MM-DD'), NULL)::DATE         AS purchase_date,           -- source: createdon
    COALESCE(TO_DATE(s1.createdon, 'YYYY-MM-DD'), NULL)::DATE         AS event_registration_date, -- derived: registration date proxy
    NULL::NUMERIC(18,4)                                               AS event_distance,          -- TODO: derive via geography
    NULL::BIGINT                                                      AS event_key,               -- TODO: join to dim_event
    NULL::BIGINT                                                      AS product_key,             -- TODO: derive from lineitemid/product
    NULL::BIGINT                                                      AS individual_key,          -- TODO: map customerid
    NULL::BIGINT                                                      AS company_key,             -- TODO: map customerid to organization
    NULL::BIGINT                                                      AS geography_key,           -- TODO: badge-based geography
    NULL::BIGINT                                                      AS event_geography_key,     -- TODO: venue/location
    NULL::BIGINT                                                      AS event_registration_key,  -- TODO: join to fact_event_registrations
    CAST(s1.ams_rem_purchase_registrationpurchase_key_sk AS BIGINT)   AS event_sessions_key       -- derived: temporary self-reference until proper session grain established
FROM source.ams_rem_purchase_registrationpurchase s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE
    AND s1.meta_record_status = 'A';

-- ===============================================================
-- Sales Domain - DDL
-- ===============================================================

-- Dimension: dim_sales_orders
CREATE TABLE IF NOT EXISTS newmodel.dim_sales_orders
(
    sales_orders_key   BIGINT      NOT NULL,
    sales_order_status VARCHAR(50),
    sales_order_entity VARCHAR(255),
    PRIMARY KEY (sales_orders_key)
);

-- Dimension: dim_sales_lines
CREATE TABLE IF NOT EXISTS newmodel.dim_sales_lines
(
    sales_lines_key  BIGINT      NOT NULL,
    sales_line_status VARCHAR(50),
    PRIMARY KEY (sales_lines_key)
);

-- Fact: fact_sales_orders
CREATE TABLE IF NOT EXISTS newmodel.fact_sales_orders
(
    sales_orders_key     BIGINT      NOT NULL,
    sales_orders_quantity NUMERIC(18,4),
    sales_orders_amount   NUMERIC(18,4),
    sales_order_date      DATE,
    individual_key        BIGINT,
    company_key           BIGINT,
    geography_key         BIGINT,
    PRIMARY KEY (sales_orders_key)
);

-- Fact: fact_sales_lines
CREATE TABLE IF NOT EXISTS newmodel.fact_sales_lines
(
    sales_lines_key     BIGINT      NOT NULL,
    sales_line_quantity NUMERIC(18,4),
    sales_line_amount   NUMERIC(18,4),
    sales_order_date    DATE,
    product_key         BIGINT,
    individual_key      BIGINT,
    company_key         BIGINT,
    geography_key       BIGINT,
    sales_orders_key    BIGINT,
    PRIMARY KEY (sales_lines_key)
);

-- ===============================================================
-- Sales Domain - ETL
-- ===============================================================

-- Dimension: dim_sales_orders
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Sales]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_sales_orders]
-- [SOURCES: source.ams_rem_accounting_orders]
-- [GRAIN  : one row per order header in ams_rem_accounting_orders]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_sales_orders
(
    sales_orders_key,
    sales_order_status,
    sales_order_entity
)
SELECT
    CAST(s1.ams_rem_accounting_orders_key_sk AS BIGINT) AS sales_orders_key,   -- source: ams_rem_accounting_orders.ams_rem_accounting_orders_key_sk
    CAST(s1.ordersource AS VARCHAR(50))                 AS sales_order_status, -- derived: placeholder using ordersource; refine with true status when available
    CAST(s1.name AS VARCHAR(255))                       AS sales_order_entity  -- source: ams_rem_accounting_orders.name
FROM source.ams_rem_accounting_orders s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE
    AND s1.orderid IS NOT NULL;


-- Dimension: dim_sales_lines
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Sales]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_sales_lines]
-- [SOURCES: source.ams_rem_accounting_lineitem]
-- [GRAIN  : one row per line item in ams_rem_accounting_lineitem]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_sales_lines
(
    sales_lines_key,
    sales_line_status
)
SELECT
    CAST(s1.ams_rem_accounting_lineitem_key_sk AS BIGINT) AS sales_lines_key,  -- source: ams_rem_accounting_lineitem.ams_rem_accounting_lineitem_key_sk
    CAST(s1.pricecode AS VARCHAR(50))                     AS sales_line_status -- derived: placeholder using pricecode as a status-like attribute
FROM source.ams_rem_accounting_lineitem s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE
    AND s1.id IS NOT NULL;


-- Fact: fact_sales_orders
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Sales]
-- [TYPE   : fact]
-- [TARGET : newmodel.fact_sales_orders]
-- [SOURCES: source.ams_rem_accounting_orders]
-- [GRAIN  : one row per order header in ams_rem_accounting_orders]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.fact_sales_orders
(
    sales_orders_key,
    sales_orders_quantity,
    sales_orders_amount,
    sales_order_date,
    individual_key,
    company_key,
    geography_key
)
SELECT
    CAST(s1.ams_rem_accounting_orders_key_sk AS BIGINT)       AS sales_orders_key,     -- source: ams_rem_accounting_orders.ams_rem_accounting_orders_key_sk
    NULL::NUMERIC(18,4)                                       AS sales_orders_quantity, -- TODO: derive from summed line item quantities
    NULL::NUMERIC(18,4)                                       AS sales_orders_amount,   -- TODO: derive from summed line item amounts
    COALESCE(TO_DATE(s1.createdon, 'YYYY-MM-DD'), NULL)::DATE AS sales_order_date,     -- source: ams_rem_accounting_orders.createdon
    NULL::BIGINT                                              AS individual_key,       -- TODO: resolve customerid to individual/company
    NULL::BIGINT                                              AS company_key,          -- TODO: resolve customerid to organization
    NULL::BIGINT                                              AS geography_key         -- TODO: derive from billing/shipping address
FROM source.ams_rem_accounting_orders s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE
    AND s1.orderid IS NOT NULL;


-- Fact: fact_sales_lines
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Sales]
-- [TYPE   : fact]
-- [TARGET : newmodel.fact_sales_lines]
-- [SOURCES: source.ams_rem_accounting_lineitem]
-- [GRAIN  : one row per order line item in ams_rem_accounting_lineitem]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.fact_sales_lines
(
    sales_lines_key,
    sales_line_quantity,
    sales_line_amount,
    sales_order_date,
    product_key,
    individual_key,
    company_key,
    geography_key,
    sales_orders_key
)
SELECT
    CAST(s1.ams_rem_accounting_lineitem_key_sk AS BIGINT)     AS sales_lines_key,      -- source: ams_rem_accounting_lineitem.ams_rem_accounting_lineitem_key_sk
    COALESCE(CAST(s1.quantity AS NUMERIC(18,4)), 0)           AS sales_line_quantity,  -- source: ams_rem_accounting_lineitem.quantity
    NULL::NUMERIC(18,4)                                       AS sales_line_amount,    -- TODO: join to ams_rem_accounting_lineitemamount for monetary values
    COALESCE(TO_DATE(s1.createdon, 'YYYY-MM-DD'), NULL)::DATE AS sales_order_date,     -- source: ams_rem_accounting_lineitem.createdon
    NULL::BIGINT                                              AS product_key,          -- TODO: map productpriceid to dim_product
    NULL::BIGINT                                              AS individual_key,       -- TODO: resolve customerid
    NULL::BIGINT                                              AS company_key,          -- TODO: resolve customerid to organization
    NULL::BIGINT                                              AS geography_key,        -- TODO: derive from customer address
    NULL::BIGINT                                              AS sales_orders_key      -- TODO: join to dim_sales_orders via order header linkage
FROM source.ams_rem_accounting_lineitem s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE
    AND s1.id IS NOT NULL;


-- ===============================================================
-- Email Domain - DDL
-- ===============================================================

-- Dimension: dim_campaign
CREATE TABLE IF NOT EXISTS newmodel.dim_campaign
(
    campaign_key    BIGINT      NOT NULL,
    campaign_code   VARCHAR(100),
    campaign_name   VARCHAR(255),
    campaign_type   VARCHAR(100),
    campaign_status VARCHAR(50),
    PRIMARY KEY (campaign_key)
);

-- Dimension: dim_email
CREATE TABLE IF NOT EXISTS newmodel.dim_email
(
    email_key             BIGINT      NOT NULL,
    email_code            VARCHAR(100),
    email_name            VARCHAR(255),
    email_type            VARCHAR(100),
    email_subject         VARCHAR(255),
    email_status          VARCHAR(50),
    email_send_first_date DATE,
    email_send_last_date  DATE,
    PRIMARY KEY (email_key)
);

-- Dimension: dim_email_sends
CREATE TABLE IF NOT EXISTS newmodel.dim_email_sends
(
    email_sends_key  BIGINT      NOT NULL,
    email_send_status VARCHAR(50),
    PRIMARY KEY (email_sends_key)
);

-- Dimension: dim_email_opens
CREATE TABLE IF NOT EXISTS newmodel.dim_email_opens
(
    email_opens_key     BIGINT      NOT NULL,
    email_first_open_flag BOOLEAN,
    PRIMARY KEY (email_opens_key)
);

-- Dimension: dim_email_clicks
CREATE TABLE IF NOT EXISTS newmodel.dim_email_clicks
(
    email_clicks_key      BIGINT      NOT NULL,
    email_first_click_flag BOOLEAN,
    email_click_url       VARCHAR(500),
    email_click_domain    VARCHAR(255),
    PRIMARY KEY (email_clicks_key)
);

-- Dimension: dim_email_summaries
CREATE TABLE IF NOT EXISTS newmodel.dim_email_summaries
(
    email_summaries_key BIGINT      NOT NULL,
    email_summaries     VARCHAR(255),
    PRIMARY KEY (email_summaries_key)
);

-- Fact: fact_email_sends
CREATE TABLE IF NOT EXISTS newmodel.fact_email_sends
(
    email_sends_key       BIGINT      NOT NULL,
    email_send_date       DATE,
    email_send_time       VARCHAR(20),
    email_key             BIGINT,
    email_send_status_key BIGINT,
    campaign_key          BIGINT,
    individual_key        BIGINT,
    company_key           BIGINT,
    geography_key         BIGINT,
    PRIMARY KEY (email_sends_key)
);

-- Fact: fact_email_opens
CREATE TABLE IF NOT EXISTS newmodel.fact_email_opens
(
    email_opens_key  BIGINT      NOT NULL,
    email_open_date  DATE,
    email_open_time  VARCHAR(20),
    email_key        BIGINT,
    campaign_key     BIGINT,
    individual_key   BIGINT,
    company_key      BIGINT,
    geography_key    BIGINT,
    PRIMARY KEY (email_opens_key)
);

-- Fact: fact_email_clicks
CREATE TABLE IF NOT EXISTS newmodel.fact_email_clicks
(
    email_clicks_key BIGINT      NOT NULL,
    email_click_date DATE,
    email_click_time VARCHAR(20),
    email_key        BIGINT,
    campaign_key     BIGINT,
    individual_key   BIGINT,
    company_key      BIGINT,
    geography_key    BIGINT,
    PRIMARY KEY (email_clicks_key)
);

-- Fact: fact_email_summaries
CREATE TABLE IF NOT EXISTS newmodel.fact_email_summaries
(
    email_summaries_key BIGINT      NOT NULL,
    email_sends         NUMERIC(18,4),
    email_opens         NUMERIC(18,4),
    email_clicks        NUMERIC(18,4),
    email_bounces       NUMERIC(18,4),
    email_deliveries    NUMERIC(18,4),
    email_key           BIGINT,
    campaign_key        BIGINT,
    PRIMARY KEY (email_summaries_key)
);

-- ===============================================================
-- Email Domain - ETL
-- ===============================================================

-- NOTE: Source email tracking tables are not clearly identifiable in erd.json.
-- All mappings below are placeholders with NULLs and TODO comments until
-- explicit email source tables are confirmed.

-- Dimension: dim_campaign
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Email]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_campaign]
-- [SOURCES: TODO]
-- [GRAIN  : placeholder; one row per campaign]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_campaign
(
    campaign_key,
    campaign_code,
    campaign_name,
    campaign_type,
    campaign_status
)
SELECT
    NULL::BIGINT        AS campaign_key,    -- TODO: no source mapping identified
    NULL::VARCHAR(100)  AS campaign_code,   -- TODO: no source mapping identified
    NULL::VARCHAR(255)  AS campaign_name,   -- TODO: no source mapping identified
    NULL::VARCHAR(100)  AS campaign_type,   -- TODO: no source mapping identified
    NULL::VARCHAR(50)   AS campaign_status  -- TODO: no source mapping identified
WHERE 1 = 0;  -- prevent population until source is confirmed


-- Dimension: dim_email
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Email]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_email]
-- [SOURCES: TODO]
-- [GRAIN  : placeholder; one row per email definition]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_email
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
    NULL::BIGINT        AS email_key,             -- TODO: no source mapping identified
    NULL::VARCHAR(100)  AS email_code,            -- TODO: no source mapping identified
    NULL::VARCHAR(255)  AS email_name,            -- TODO: no source mapping identified
    NULL::VARCHAR(100)  AS email_type,            -- TODO: no source mapping identified
    NULL::VARCHAR(255)  AS email_subject,         -- TODO: no source mapping identified
    NULL::VARCHAR(50)   AS email_status,          -- TODO: no source mapping identified
    NULL::DATE          AS email_send_first_date, -- TODO: no source mapping identified
    NULL::DATE          AS email_send_last_date   -- TODO: no source mapping identified
WHERE 1 = 0;  -- prevent population until source is confirmed


-- Dimension: dim_email_sends
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Email]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_email_sends]
-- [SOURCES: TODO]
-- [GRAIN  : placeholder; one row per send status]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_email_sends
(
    email_sends_key,
    email_send_status
)
SELECT
    NULL::BIGINT       AS email_sends_key,  -- TODO: no source mapping identified
    NULL::VARCHAR(50)  AS email_send_status -- TODO: no source mapping identified
WHERE 1 = 0;


-- Dimension: dim_email_opens
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Email]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_email_opens]
-- [SOURCES: TODO]
-- [GRAIN  : placeholder; one row per open status]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_email_opens
(
    email_opens_key,
    email_first_open_flag
)
SELECT
    NULL::BIGINT  AS email_opens_key,       -- TODO: no source mapping identified
    NULL::BOOLEAN AS email_first_open_flag  -- TODO: no source mapping identified
WHERE 1 = 0;


-- Dimension: dim_email_clicks
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Email]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_email_clicks]
-- [SOURCES: TODO]
-- [GRAIN  : placeholder; one row per click status/url]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_email_clicks
(
    email_clicks_key,
    email_first_click_flag,
    email_click_url,
    email_click_domain
)
SELECT
    NULL::BIGINT        AS email_clicks_key,      -- TODO: no source mapping identified
    NULL::BOOLEAN       AS email_first_click_flag,-- TODO: no source mapping identified
    NULL::VARCHAR(500)  AS email_click_url,       -- TODO: no source mapping identified
    NULL::VARCHAR(255)  AS email_click_domain     -- TODO: no source mapping identified
WHERE 1 = 0;


-- Dimension: dim_email_summaries
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Email]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_email_summaries]
-- [SOURCES: TODO]
-- [GRAIN  : placeholder; one row per summary rollup]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_email_summaries
(
    email_summaries_key,
    email_summaries
)
SELECT
    NULL::BIGINT       AS email_summaries_key,  -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS email_summaries       -- TODO: no source mapping identified
WHERE 1 = 0;


-- Fact: fact_email_sends
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Email]
-- [TYPE   : fact]
-- [TARGET : newmodel.fact_email_sends]
-- [SOURCES: TODO]
-- [GRAIN  : placeholder; one row per email send]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.fact_email_sends
(
    email_sends_key,
    email_send_date,
    email_send_time,
    email_key,
    email_send_status_key,
    campaign_key,
    individual_key,
    company_key,
    geography_key
)
SELECT
    NULL::BIGINT      AS email_sends_key,       -- TODO: no source mapping identified
    NULL::DATE        AS email_send_date,       -- TODO: no source mapping identified
    NULL::VARCHAR(20) AS email_send_time,       -- TODO: no source mapping identified
    NULL::BIGINT      AS email_key,             -- TODO: no source mapping identified
    NULL::BIGINT      AS email_send_status_key, -- TODO: no source mapping identified
    NULL::BIGINT      AS campaign_key,          -- TODO: no source mapping identified
    NULL::BIGINT      AS individual_key,        -- TODO: no source mapping identified
    NULL::BIGINT      AS company_key,           -- TODO: no source mapping identified
    NULL::BIGINT      AS geography_key          -- TODO: no source mapping identified
WHERE 1 = 0;


-- Fact: fact_email_opens
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Email]
-- [TYPE   : fact]
-- [TARGET : newmodel.fact_email_opens]
-- [SOURCES: TODO]
-- [GRAIN  : placeholder; one row per email open]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.fact_email_opens
(
    email_opens_key,
    email_open_date,
    email_open_time,
    email_key,
    campaign_key,
    individual_key,
    company_key,
    geography_key
)
SELECT
    NULL::BIGINT      AS email_opens_key,   -- TODO: no source mapping identified
    NULL::DATE        AS email_open_date,   -- TODO: no source mapping identified
    NULL::VARCHAR(20) AS email_open_time,   -- TODO: no source mapping identified
    NULL::BIGINT      AS email_key,         -- TODO: no source mapping identified
    NULL::BIGINT      AS campaign_key,      -- TODO: no source mapping identified
    NULL::BIGINT      AS individual_key,    -- TODO: no source mapping identified
    NULL::BIGINT      AS company_key,       -- TODO: no source mapping identified
    NULL::BIGINT      AS geography_key      -- TODO: no source mapping identified
WHERE 1 = 0;


-- Fact: fact_email_clicks
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Email]
-- [TYPE   : fact]
-- [TARGET : newmodel.fact_email_clicks]
-- [SOURCES: TODO]
-- [GRAIN  : placeholder; one row per email click]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.fact_email_clicks
(
    email_clicks_key,
    email_click_date,
    email_click_time,
    email_key,
    campaign_key,
    individual_key,
    company_key,
    geography_key
)
SELECT
    NULL::BIGINT      AS email_clicks_key,  -- TODO: no source mapping identified
    NULL::DATE        AS email_click_date,  -- TODO: no source mapping identified
    NULL::VARCHAR(20) AS email_click_time,  -- TODO: no source mapping identified
    NULL::BIGINT      AS email_key,         -- TODO: no source mapping identified
    NULL::BIGINT      AS campaign_key,      -- TODO: no source mapping identified
    NULL::BIGINT      AS individual_key,    -- TODO: no source mapping identified
    NULL::BIGINT      AS company_key,       -- TODO: no source mapping identified
    NULL::BIGINT      AS geography_key      -- TODO: no source mapping identified
WHERE 1 = 0;


-- Fact: fact_email_summaries
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Email]
-- [TYPE   : fact]
-- [TARGET : newmodel.fact_email_summaries]
-- [SOURCES: TODO]
-- [GRAIN  : placeholder; one row per email/campaign daily summary]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.fact_email_summaries
(
    email_summaries_key,
    email_sends,
    email_opens,
    email_clicks,
    email_bounces,
    email_deliveries,
    email_key,
    campaign_key
)
SELECT
    NULL::BIGINT       AS email_summaries_key, -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS email_sends,        -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS email_opens,        -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS email_clicks,       -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS email_bounces,      -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS email_deliveries,   -- TODO: no source mapping identified
    NULL::BIGINT        AS email_key,          -- TODO: no source mapping identified
    NULL::BIGINT        AS campaign_key        -- TODO: no source mapping identified
WHERE 1 = 0;


-- ===============================================================
-- Community Domain - DDL
-- ===============================================================

-- Dimension: dim_community_activities
CREATE TABLE IF NOT EXISTS newmodel.dim_community_activities
(
    community_activities_key   BIGINT       NOT NULL,
    community_activity_description VARCHAR(500),
    PRIMARY KEY (community_activities_key)
);

-- Dimension: dim_community_discussion_posts
CREATE TABLE IF NOT EXISTS newmodel.dim_community_discussion_posts
(
    community_discussion_posts_key BIGINT       NOT NULL,
    discussion_post_subject        VARCHAR(255),
    discussion_post_content        VARCHAR(4000),
    discussion_post_type           VARCHAR(100),
    discussion_post_status         VARCHAR(50),
    PRIMARY KEY (community_discussion_posts_key)
);

-- Dimension: dim_community_discussions
CREATE TABLE IF NOT EXISTS newmodel.dim_community_discussions
(
    community_discussions_key BIGINT NOT NULL,
    PRIMARY KEY (community_discussions_key)
);

-- Dimension: dim_community_memberships
CREATE TABLE IF NOT EXISTS newmodel.dim_community_memberships
(
    community_memberships_key BIGINT       NOT NULL,
    community_membership_type VARCHAR(100),
    PRIMARY KEY (community_memberships_key)
);

-- Dimension: dim_community
CREATE TABLE IF NOT EXISTS newmodel.dim_community
(
    community_key   BIGINT       NOT NULL,
    community_code  VARCHAR(100),
    community_name  VARCHAR(255),
    community_type  VARCHAR(100),
    community_status VARCHAR(50),
    PRIMARY KEY (community_key)
);

-- Fact: fact_community_activities
CREATE TABLE IF NOT EXISTS newmodel.fact_community_activities
(
    community_activity_date      DATE,
    community_activity_time      VARCHAR(20),
    community_activity_type_key  BIGINT,
    individual_key               BIGINT,
    company_key                  BIGINT,
    geography_key                BIGINT,
    community_activities_key     BIGINT      NOT NULL,
    PRIMARY KEY (community_activities_key)
);

-- Fact: fact_community_discussion_posts
CREATE TABLE IF NOT EXISTS newmodel.fact_community_discussion_posts
(
    community_discussion_post_date DATE,
    community_discussion_post_time VARCHAR(20),
    community_discussions_key      BIGINT,
    individual_key                 BIGINT,
    company_key                    BIGINT,
    geography_key                  BIGINT,
    community_discussion_posts_key BIGINT      NOT NULL,
    PRIMARY KEY (community_discussion_posts_key)
);

-- Fact: fact_community_discussions
CREATE TABLE IF NOT EXISTS newmodel.fact_community_discussions
(
    community_discussion_post_count   NUMERIC(18,4),
    community_discussion_first_date   DATE,
    community_discussion_first_time   VARCHAR(20),
    community_discussion_last_date    DATE,
    community_discussion_last_time    VARCHAR(20),
    community_key                     BIGINT,
    community_discussions_key         BIGINT      NOT NULL,
    PRIMARY KEY (community_discussions_key)
);

-- Fact: fact_community_memberships
CREATE TABLE IF NOT EXISTS newmodel.fact_community_memberships
(
    community_membership_date  DATE,
    community_key              BIGINT,
    individual_key             BIGINT,
    company_key                BIGINT,
    geography_key              BIGINT,
    community_memberships_key  BIGINT      NOT NULL,
    PRIMARY KEY (community_memberships_key)
);


-- ===============================================================
-- Community Domain - ETL (Unmappable Skeletons)
-- ===============================================================

-- Dimension: dim_community_activities
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Community]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_community_activities]
-- [SOURCES: TODO]
-- [GRAIN  : one row per community activity type]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_community_activities
(
    community_activities_key,
    community_activity_description
)
SELECT
    NULL::BIGINT       AS community_activities_key,    -- TODO: no source mapping identified
    NULL::VARCHAR(500) AS community_activity_description -- TODO: no source mapping identified
WHERE 1 = 0;  -- prevent empty insert from running until source is confirmed


-- Dimension: dim_community_discussion_posts
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Community]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_community_discussion_posts]
-- [SOURCES: TODO]
-- [GRAIN  : one row per community discussion post definition]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_community_discussion_posts
(
    community_discussion_posts_key,
    discussion_post_subject,
    discussion_post_content,
    discussion_post_type,
    discussion_post_status
)
SELECT
    NULL::BIGINT        AS community_discussion_posts_key, -- TODO: no source mapping identified
    NULL::VARCHAR(255)  AS discussion_post_subject,        -- TODO: no source mapping identified
    NULL::VARCHAR(4000) AS discussion_post_content,        -- TODO: no source mapping identified
    NULL::VARCHAR(100)  AS discussion_post_type,           -- TODO: no source mapping identified
    NULL::VARCHAR(50)   AS discussion_post_status          -- TODO: no source mapping identified
WHERE 1 = 0;


-- Dimension: dim_community_discussions
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Community]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_community_discussions]
-- [SOURCES: TODO]
-- [GRAIN  : one row per community discussion thread]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_community_discussions
(
    community_discussions_key
)
SELECT
    NULL::BIGINT AS community_discussions_key   -- TODO: no source mapping identified
WHERE 1 = 0;


-- Dimension: dim_community_memberships
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Community]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_community_memberships]
-- [SOURCES: TODO]
-- [GRAIN  : one row per community membership type]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_community_memberships
(
    community_memberships_key,
    community_membership_type
)
SELECT
    NULL::BIGINT      AS community_memberships_key, -- TODO: no source mapping identified
    NULL::VARCHAR(100) AS community_membership_type -- TODO: no source mapping identified
WHERE 1 = 0;


-- Dimension: dim_community
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Community]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_community]
-- [SOURCES: TODO]
-- [GRAIN  : one row per community entity]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_community
(
    community_key,
    community_code,
    community_name,
    community_type,
    community_status
)
SELECT
    NULL::BIGINT      AS community_key,    -- TODO: no source mapping identified
    NULL::VARCHAR(100) AS community_code,  -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS community_name,  -- TODO: no source mapping identified
    NULL::VARCHAR(100) AS community_type,  -- TODO: no source mapping identified
    NULL::VARCHAR(50)  AS community_status -- TODO: no source mapping identified
WHERE 1 = 0;


-- Fact: fact_community_activities
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Community]
-- [TYPE   : fact]
-- [TARGET : newmodel.fact_community_activities]
-- [SOURCES: TODO]
-- [GRAIN  : one row per community activity event]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.fact_community_activities
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
    NULL::VARCHAR(20) AS community_activity_time,     -- TODO: no source mapping identified
    NULL::BIGINT      AS community_activity_type_key, -- TODO: no source mapping identified
    NULL::BIGINT      AS individual_key,              -- TODO: no source mapping identified
    NULL::BIGINT      AS company_key,                 -- TODO: no source mapping identified
    NULL::BIGINT      AS geography_key,               -- TODO: no source mapping identified
    NULL::BIGINT      AS community_activities_key     -- TODO: no source mapping identified
WHERE 1 = 0;


-- Fact: fact_community_discussion_posts
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Community]
-- [TYPE   : fact]
-- [TARGET : newmodel.fact_community_discussion_posts]
-- [SOURCES: TODO]
-- [GRAIN  : one row per community discussion post instance]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.fact_community_discussion_posts
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
    NULL::VARCHAR(20) AS community_discussion_post_time, -- TODO: no source mapping identified
    NULL::BIGINT      AS community_discussions_key,      -- TODO: no source mapping identified
    NULL::BIGINT      AS individual_key,                 -- TODO: no source mapping identified
    NULL::BIGINT      AS company_key,                    -- TODO: no source mapping identified
    NULL::BIGINT      AS geography_key,                  -- TODO: no source mapping identified
    NULL::BIGINT      AS community_discussion_posts_key  -- TODO: no source mapping identified
WHERE 1 = 0;


-- Fact: fact_community_discussions
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Community]
-- [TYPE   : fact]
-- [TARGET : newmodel.fact_community_discussions]
-- [SOURCES: TODO]
-- [GRAIN  : one row per community discussion thread summary]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.fact_community_discussions
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
    NULL::VARCHAR(20)   AS community_discussion_first_time, -- TODO: no source mapping identified
    NULL::DATE          AS community_discussion_last_date,  -- TODO: no source mapping identified
    NULL::VARCHAR(20)   AS community_discussion_last_time,  -- TODO: no source mapping identified
    NULL::BIGINT        AS community_key,                   -- TODO: no source mapping identified
    NULL::BIGINT        AS community_discussions_key        -- TODO: no source mapping identified
WHERE 1 = 0;


-- Fact: fact_community_memberships
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Community]
-- [TYPE   : fact]
-- [TARGET : newmodel.fact_community_memberships]
-- [SOURCES: TODO]
-- [GRAIN  : one row per community membership event]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.fact_community_memberships
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


-- ===============================================================
-- Sponsorships & Exhibits Domain - DDL
-- ===============================================================

-- Dimension: dim_sponsorships
CREATE TABLE IF NOT EXISTS newmodel.dim_sponsorships
(
    sponsorships_key   BIGINT      NOT NULL,
    sponsorship_status VARCHAR(50),
    PRIMARY KEY (sponsorships_key)
);

-- Dimension: dim_sponsorship
CREATE TABLE IF NOT EXISTS newmodel.dim_sponsorship
(
    sponsorship_key   BIGINT      NOT NULL,
    sponsorship_code  VARCHAR(100),
    sponsorship_name  VARCHAR(255),
    sponsorship_type  VARCHAR(100),
    sponsorship_level VARCHAR(100),
    sponsorship_status VARCHAR(50),
    PRIMARY KEY (sponsorship_key)
);

-- Dimension: dim_exhibit
CREATE TABLE IF NOT EXISTS newmodel.dim_exhibit
(
    exhibit_key   BIGINT      NOT NULL,
    exhibit_code  VARCHAR(100),
    exhibit_name  VARCHAR(255),
    exhibit_type  VARCHAR(100),
    exhibit_status VARCHAR(50),
    PRIMARY KEY (exhibit_key)
);

-- Dimension: dim_exhibitor
CREATE TABLE IF NOT EXISTS newmodel.dim_exhibitor
(
    exhibitor_key          BIGINT      NOT NULL,
    exhibitor_id           VARCHAR(100),
    exhibitor_name         VARCHAR(255),
    exhibitor_type         VARCHAR(100),
    exhibitor_busines_area VARCHAR(255),
    exhibitor_status       VARCHAR(50),
    exhibitor_geography_key BIGINT,
    PRIMARY KEY (exhibitor_key)
);

-- Fact: fact_sponsorships
CREATE TABLE IF NOT EXISTS newmodel.fact_sponsorships
(
    sponsorships_key        BIGINT      NOT NULL,
    sponsorship_quanity     NUMERIC(18,4),
    sponsorship_amount      NUMERIC(18,4),
    sponsorship_purchase_date DATE,
    sponsorship_start_date  DATE,
    sponsorship_end_date    DATE,
    product_key             BIGINT,
    sponsor_key             BIGINT,
    PRIMARY KEY (sponsorships_key)
);

-- Fact: fact_event_exhibits
CREATE TABLE IF NOT EXISTS newmodel.fact_event_exhibits
(
    event_exhibits_key   BIGINT      NOT NULL,
    event_exhibit_quantity NUMERIC(18,4),
    event_exhibit_amount NUMERIC(18,4),
    event_exhibit_date   DATE,
    event_start_date     DATE,
    event_key            BIGINT,
    product_key          BIGINT,
    event_exhibit_key    BIGINT,
    exhibitor_key        BIGINT,
    PRIMARY KEY (event_exhibits_key)
);

-- Fact: fact_event_exhibit_purchases
CREATE TABLE IF NOT EXISTS newmodel.fact_event_exhibit_purchases
(
    event_exhibit_purchases_key    BIGINT      NOT NULL,
    event_exhibit_purchase_quantity NUMERIC(18,4),
    event_exhibit_purchase_amount  NUMERIC(18,4),
    event_exhibit_purchase_date    DATE,
    event_start_date               DATE,
    event_key                      BIGINT,
    product_key                    BIGINT,
    event_exhibit_key              BIGINT,
    exhibitor_key                  BIGINT,
    event_exhibits_key             BIGINT,
    PRIMARY KEY (event_exhibit_purchases_key)
);


-- ===============================================================
-- Sponsorships & Exhibits Domain - ETL (Partially Sourced)
-- ===============================================================

-- Dimension: dim_sponsorship
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Sponsorships & Exhibits]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_sponsorship]
-- [SOURCES: source.ams_rem_shopping_sponsorship]
-- [GRAIN  : one row per sponsorship product in ams_rem_shopping_sponsorship]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_sponsorship
(
    sponsorship_key,
    sponsorship_code,
    sponsorship_name,
    sponsorship_type,
    sponsorship_level,
    sponsorship_status
)
SELECT
    CAST(s1.ams_rem_shopping_sponsorship_key_sk AS BIGINT) AS sponsorship_key,   -- source: ams_rem_shopping_sponsorship_key_sk
    CAST(s1.code AS VARCHAR(100))                          AS sponsorship_code,  -- source: code
    CAST(s1.name AS VARCHAR(255))                          AS sponsorship_name,  -- source: name
    CAST(s1.type AS VARCHAR(100))                          AS sponsorship_type,  -- source: type
    NULL::VARCHAR(100)                                     AS sponsorship_level, -- TODO: derive level from pricing or category if available
    CAST(s1.meta_record_status AS VARCHAR(50))             AS sponsorship_status -- source: meta_record_status
FROM source.ams_rem_shopping_sponsorship s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE
    AND s1.id IS NOT NULL;


-- Dimension: dim_sponsorships
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Sponsorships & Exhibits]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_sponsorships]
-- [SOURCES: TODO]
-- [GRAIN  : one row per sponsorship status/type aggregate]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_sponsorships
(
    sponsorships_key,
    sponsorship_status
)
SELECT
    NULL::BIGINT     AS sponsorships_key,   -- TODO: no source mapping identified
    NULL::VARCHAR(50) AS sponsorship_status -- TODO: no source mapping identified
WHERE 1 = 0;


-- Dimension: dim_exhibit
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Sponsorships & Exhibits]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_exhibit]
-- [SOURCES: source.ams_rem_shopping_exhibit]
-- [GRAIN  : one row per exhibit definition in ams_rem_shopping_exhibit]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_exhibit
(
    exhibit_key,
    exhibit_code,
    exhibit_name,
    exhibit_type,
    exhibit_status
)
SELECT
    CAST(s1.ams_rem_shopping_exhibit_key_sk AS BIGINT) AS exhibit_key,   -- source: ams_rem_shopping_exhibit_key_sk
    CAST(s1.code AS VARCHAR(100))                      AS exhibit_code,  -- source: code
    CAST(s1.name AS VARCHAR(255))                      AS exhibit_name,  -- source: name
    CAST(s1.type AS VARCHAR(100))                      AS exhibit_type,  -- source: type
    CAST(s1.meta_record_status AS VARCHAR(50))         AS exhibit_status -- source: meta_record_status
FROM source.ams_rem_shopping_exhibit s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE
    AND s1.id IS NOT NULL;


-- Dimension: dim_exhibitor
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Sponsorships & Exhibits]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_exhibitor]
-- [SOURCES: source.ams_rem_purchase_exhibitorbooth]
-- [GRAIN  : one row per exhibitor-organization combination in ams_rem_purchase_exhibitorbooth]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_exhibitor
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
    CAST(s1.ams_rem_purchase_exhibitorbooth_key_sk AS BIGINT) AS exhibitor_key,          -- source: ams_rem_purchase_exhibitorbooth_key_sk
    CAST(s1.organizationid AS VARCHAR(100))                   AS exhibitor_id,           -- source: organizationid
    CAST(s1.displayname AS VARCHAR(255))                      AS exhibitor_name,         -- source: displayname
    NULL::VARCHAR(100)                                       AS exhibitor_type,         -- TODO: derive type from related attributes if available
    NULL::VARCHAR(255)                                       AS exhibitor_busines_area, -- TODO: derive from CRM or category
    CAST(s1.meta_record_status AS VARCHAR(50))               AS exhibitor_status,       -- source: meta_record_status
    NULL::BIGINT                                             AS exhibitor_geography_key -- TODO: map venue/location to geography
FROM source.ams_rem_purchase_exhibitorbooth s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE;


-- ===============================================================
-- Chapter Memberships Domain - DDL
-- ===============================================================

-- Dimension: dim_chapter
CREATE TABLE IF NOT EXISTS newmodel.dim_chapter
(
    chapter_key    BIGINT      NOT NULL,
    chapter_code   VARCHAR(100),
    chapter_name   VARCHAR(255),
    chapter_type   VARCHAR(100),
    chapter_status VARCHAR(50),
    PRIMARY KEY (chapter_key)
);

-- Fact: fact_chapter_memberships
CREATE TABLE IF NOT EXISTS newmodel.fact_chapter_memberships
(
    chapter_memberships_key    BIGINT      NOT NULL,
    chapter_membership_quantity NUMERIC(18,4),
    chapter_membership_amount   NUMERIC(18,4),
    chapter_membership_start_date DATE,
    chapter_membership_end_date   DATE,
    chapter_membership_grace_date DATE,
    chapter_key                  BIGINT,
    individual_key               BIGINT,
    company_key                  BIGINT,
    geography_key                BIGINT,
    PRIMARY KEY (chapter_memberships_key)
);


-- ===============================================================
-- Chapter Memberships Domain - ETL (Skeleton)
-- ===============================================================

-- Dimension: dim_chapter
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Chapter Memberships]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_chapter]
-- [SOURCES: TODO]
-- [GRAIN  : one row per chapter entity]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_chapter
(
    chapter_key,
    chapter_code,
    chapter_name,
    chapter_type,
    chapter_status
)
SELECT
    NULL::BIGINT       AS chapter_key,   -- TODO: no source mapping identified
    NULL::VARCHAR(100) AS chapter_code,  -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS chapter_name,  -- TODO: no source mapping identified
    NULL::VARCHAR(100) AS chapter_type,  -- TODO: no source mapping identified
    NULL::VARCHAR(50)  AS chapter_status -- TODO: no source mapping identified
WHERE 1 = 0;


-- Fact: fact_chapter_memberships
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Chapter Memberships]
-- [TYPE   : fact]
-- [TARGET : newmodel.fact_chapter_memberships]
-- [SOURCES: TODO]
-- [GRAIN  : one row per chapter membership transaction]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.fact_chapter_memberships
(
    chapter_memberships_key,
    chapter_membership_quantity,
    chapter_membership_amount,
    chapter_membership_start_date,
    chapter_membership_end_date,
    chapter_membership_grace_date,
    chapter_key,
    individual_key,
    company_key,
    geography_key
)
SELECT
    NULL::BIGINT       AS chapter_memberships_key,     -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS chapter_membership_quantity, -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS chapter_membership_amount,   -- TODO: no source mapping identified
    NULL::DATE          AS chapter_membership_start_date,-- TODO: no source mapping identified
    NULL::DATE          AS chapter_membership_end_date,  -- TODO: no source mapping identified
    NULL::DATE          AS chapter_membership_grace_date,-- TODO: no source mapping identified
    NULL::BIGINT        AS chapter_key,                 -- TODO: no source mapping identified
    NULL::BIGINT        AS individual_key,              -- TODO: no source mapping identified
    NULL::BIGINT        AS company_key,                 -- TODO: no source mapping identified
    NULL::BIGINT        AS geography_key                -- TODO: no source mapping identified
WHERE 1 = 0;


-- ===============================================================
-- Web Domain - DDL
-- ===============================================================

-- Dimension: dim_web_sessions
CREATE TABLE IF NOT EXISTS newmodel.dim_web_sessions
(
    web_sessions_key    BIGINT      NOT NULL,
    session_id          VARCHAR(100),
    traffic_source_type VARCHAR(100),
    traffic_medium      VARCHAR(100),
    traffic_source      VARCHAR(255),
    hostname            VARCHAR(255),
    PRIMARY KEY (web_sessions_key)
);

-- Dimension: dim_web_pageviews
CREATE TABLE IF NOT EXISTS newmodel.dim_web_pageviews
(
    web_pageviews_key BIGINT      NOT NULL,
    page_sequence     VARCHAR(100),
    PRIMARY KEY (web_pageviews_key)
);

-- Dimension: dim_web_page
CREATE TABLE IF NOT EXISTS newmodel.dim_web_page
(
    web_page_key    BIGINT      NOT NULL,
    page_title      VARCHAR(255),
    page_location_1 VARCHAR(255),
    page_location_2 VARCHAR(255),
    page_location_3 VARCHAR(255),
    page_location_4 VARCHAR(255),
    page_location_5 VARCHAR(255),
    page_referrer   VARCHAR(500),
    PRIMARY KEY (web_page_key)
);

-- Fact: fact_web_sessions
CREATE TABLE IF NOT EXISTS newmodel.fact_web_sessions
(
    web_sessions_key        BIGINT      NOT NULL,
    session_duration_seconds NUMERIC(18,4),
    session_date            DATE,
    session_time            VARCHAR(20),
    session_datetime        TIMESTAMP,
    geography_key           BIGINT,
    PRIMARY KEY (web_sessions_key)
);

-- Fact: fact_web_pageviews
CREATE TABLE IF NOT EXISTS newmodel.fact_web_pageviews
(
    web_pageviews_key       BIGINT      NOT NULL,
    pageview_duration_seconds NUMERIC(18,4),
    pageview_start_date     DATE,
    pageview_start_time     VARCHAR(20),
    pageview_start_datetime TIMESTAMP,
    geography_key           BIGINT,
    web_page_key            BIGINT,
    web_sessions_key        BIGINT,
    PRIMARY KEY (web_pageviews_key)
);


-- ===============================================================
-- Web Domain - ETL (Skeleton)
-- ===============================================================

-- Dimension: dim_web_sessions
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Web]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_web_sessions]
-- [SOURCES: TODO]
-- [GRAIN  : one row per web session id]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_web_sessions
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
    NULL::VARCHAR(100) AS session_id,          -- TODO: no source mapping identified
    NULL::VARCHAR(100) AS traffic_source_type, -- TODO: no source mapping identified
    NULL::VARCHAR(100) AS traffic_medium,      -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS traffic_source,      -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS hostname             -- TODO: no source mapping identified
WHERE 1 = 0;


-- Dimension: dim_web_pageviews
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Web]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_web_pageviews]
-- [SOURCES: TODO]
-- [GRAIN  : one row per pageview sequence]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_web_pageviews
(
    web_pageviews_key,
    page_sequence
)
SELECT
    NULL::BIGINT       AS web_pageviews_key, -- TODO: no source mapping identified
    NULL::VARCHAR(100) AS page_sequence      -- TODO: no source mapping identified
WHERE 1 = 0;


-- Dimension: dim_web_page
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Web]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_web_page]
-- [SOURCES: TODO]
-- [GRAIN  : one row per web page]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_web_page
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
    NULL::BIGINT       AS web_page_key,    -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS page_title,      -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS page_location_1, -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS page_location_2, -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS page_location_3, -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS page_location_4, -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS page_location_5, -- TODO: no source mapping identified
    NULL::VARCHAR(500) AS page_referrer    -- TODO: no source mapping identified
WHERE 1 = 0;


-- Fact: fact_web_sessions
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Web]
-- [TYPE   : fact]
-- [TARGET : newmodel.fact_web_sessions]
-- [SOURCES: TODO]
-- [GRAIN  : one row per web session]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.fact_web_sessions
(
    web_sessions_key,
    session_duration_seconds,
    session_date,
    session_time,
    session_datetime,
    geography_key
)
SELECT
    NULL::BIGINT       AS web_sessions_key,        -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS session_duration_seconds, -- TODO: no source mapping identified
    NULL::DATE          AS session_date,           -- TODO: no source mapping identified
    NULL::VARCHAR(20)   AS session_time,           -- TODO: no source mapping identified
    NULL::TIMESTAMP     AS session_datetime,       -- TODO: no source mapping identified
    NULL::BIGINT        AS geography_key           -- TODO: no source mapping identified
WHERE 1 = 0;


-- Fact: fact_web_pageviews
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Web]
-- [TYPE   : fact]
-- [TARGET : newmodel.fact_web_pageviews]
-- [SOURCES: TODO]
-- [GRAIN  : one row per pageview]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.fact_web_pageviews
(
    web_pageviews_key,
    pageview_duration_seconds,
    pageview_start_date,
    pageview_start_time,
    pageview_start_datetime,
    geography_key,
    web_page_key,
    web_sessions_key
)
SELECT
    NULL::BIGINT       AS web_pageviews_key,        -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS pageview_duration_seconds, -- TODO: no source mapping identified
    NULL::DATE          AS pageview_start_date,     -- TODO: no source mapping identified
    NULL::VARCHAR(20)   AS pageview_start_time,     -- TODO: no source mapping identified
    NULL::TIMESTAMP     AS pageview_start_datetime, -- TODO: no source mapping identified
    NULL::BIGINT        AS geography_key,           -- TODO: no source mapping identified
    NULL::BIGINT        AS web_page_key,            -- TODO: no source mapping identified
    NULL::BIGINT        AS web_sessions_key         -- TODO: no source mapping identified
WHERE 1 = 0;


-- ===============================================================
-- Activities Domain - DDL
-- ===============================================================

-- Dimension: dim_activities
CREATE TABLE IF NOT EXISTS newmodel.dim_activities
(
    activities_key   BIGINT      NOT NULL,
    activity_status  VARCHAR(50),
    activity_entity  VARCHAR(255),
    PRIMARY KEY (activities_key)
);

-- Dimension: dim_activity_level
CREATE TABLE IF NOT EXISTS newmodel.dim_activity_level
(
    activity_level_key BIGINT      NOT NULL,
    activity_level_1   VARCHAR(255),
    activity_level_2   VARCHAR(255),
    activity_level_3   VARCHAR(255),
    PRIMARY KEY (activity_level_key)
);

-- Fact: fact_activities
CREATE TABLE IF NOT EXISTS newmodel.fact_activities
(
    activities_key      BIGINT      NOT NULL,
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
    PRIMARY KEY (activities_key)
);


-- ===============================================================
-- Activities Domain - ETL (Skeleton)
-- ===============================================================

-- Dimension: dim_activities
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Activities]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_activities]
-- [SOURCES: TODO]
-- [GRAIN  : one row per activity type/entity]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_activities
(
    activities_key,
    activity_status,
    activity_entity
)
SELECT
    NULL::BIGINT      AS activities_key,  -- TODO: no source mapping identified
    NULL::VARCHAR(50) AS activity_status, -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS activity_entity -- TODO: no source mapping identified
WHERE 1 = 0;


-- Dimension: dim_activity_level
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Activities]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_activity_level]
-- [SOURCES: TODO]
-- [GRAIN  : one row per activity level combination]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_activity_level
(
    activity_level_key,
    activity_level_1,
    activity_level_2,
    activity_level_3
)
SELECT
    NULL::BIGINT       AS activity_level_key, -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS activity_level_1,   -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS activity_level_2,   -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS activity_level_3    -- TODO: no source mapping identified
WHERE 1 = 0;


-- Fact: fact_activities
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Activities]
-- [TYPE   : fact]
-- [TARGET : newmodel.fact_activities]
-- [SOURCES: TODO]
-- [GRAIN  : one row per activity event]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.fact_activities
(
    activities_key,
    activity_quantity,
    activity_amount,
    activity_duration,
    activity_date,
    activity_start_date,
    activity_end_date,
    activity_level_key,
    individual_key,
    company_key,
    geography_key
)
SELECT
    NULL::BIGINT       AS activities_key,      -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS activity_quantity,  -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS activity_amount,    -- TODO: no source mapping identified
    NULL::NUMERIC(18,4) AS activity_duration,  -- TODO: no source mapping identified
    NULL::DATE          AS activity_date,      -- TODO: no source mapping identified
    NULL::DATE          AS activity_start_date,-- TODO: no source mapping identified
    NULL::DATE          AS activity_end_date,  -- TODO: no source mapping identified
    NULL::BIGINT        AS activity_level_key, -- TODO: no source mapping identified
    NULL::BIGINT        AS individual_key,     -- TODO: no source mapping identified
    NULL::BIGINT        AS company_key,        -- TODO: no source mapping identified
    NULL::BIGINT        AS geography_key       -- TODO: no source mapping identified
WHERE 1 = 0;


-- ===============================================================
-- Core Shared Dimensions - DDL
-- ===============================================================

-- Dimension: dim_product
CREATE TABLE IF NOT EXISTS newmodel.dim_product
(
    product_key   BIGINT      NOT NULL,
    product_code  VARCHAR(100),
    product_name  VARCHAR(255),
    product_type  VARCHAR(100),
    product_status VARCHAR(50),
    PRIMARY KEY (product_key)
);

-- Dimension: dim_location
CREATE TABLE IF NOT EXISTS newmodel.dim_location
(
    location_key  BIGINT      NOT NULL,
    location_code VARCHAR(100),
    location_name VARCHAR(255),
    location_type VARCHAR(100),
    PRIMARY KEY (location_key)
);

-- Dimension: dim_geography
CREATE TABLE IF NOT EXISTS newmodel.dim_geography
(
    geography_key BIGINT      NOT NULL,
    geography_name VARCHAR(255),
    PRIMARY KEY (geography_key)
);

-- Dimension: dim_date
CREATE TABLE IF NOT EXISTS newmodel.dim_date
(
    date_key   BIGINT      NOT NULL,
    date_value DATE,
    PRIMARY KEY (date_key)
);

-- Dimension: dim_time
CREATE TABLE IF NOT EXISTS newmodel.dim_time
(
    time_key   BIGINT      NOT NULL,
    time_value TIME,
    PRIMARY KEY (time_key)
);

-- Dimension: dim_month
CREATE TABLE IF NOT EXISTS newmodel.dim_month
(
    month_key   BIGINT      NOT NULL,
    month_value VARCHAR(20),
    PRIMARY KEY (month_key)
);

-- Dimension: dim_year
CREATE TABLE IF NOT EXISTS newmodel.dim_year
(
    year_key   BIGINT      NOT NULL,
    year_value INTEGER,
    PRIMARY KEY (year_key)
);


-- ===============================================================
-- Core Shared Dimensions - ETL (Skeleton)
-- ===============================================================

-- Dimension: dim_product
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Shared]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_product]
-- [SOURCES: TODO]
-- [GRAIN  : one row per product]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_product
(
    product_key,
    product_code,
    product_name,
    product_type,
    product_status
)
SELECT
    NULL::BIGINT       AS product_key,    -- TODO: no source mapping identified
    NULL::VARCHAR(100) AS product_code,   -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS product_name,   -- TODO: no source mapping identified
    NULL::VARCHAR(100) AS product_type,   -- TODO: no source mapping identified
    NULL::VARCHAR(50)  AS product_status  -- TODO: no source mapping identified
WHERE 1 = 0;


-- Dimension: dim_location
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Shared]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_location]
-- [SOURCES: TODO]
-- [GRAIN  : one row per location]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_location
(
    location_key,
    location_code,
    location_name,
    location_type
)
SELECT
    NULL::BIGINT       AS location_key,   -- TODO: no source mapping identified
    NULL::VARCHAR(100) AS location_code,  -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS location_name,  -- TODO: no source mapping identified
    NULL::VARCHAR(100) AS location_type   -- TODO: no source mapping identified
WHERE 1 = 0;


-- Dimension: dim_geography
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Shared]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_geography]
-- [SOURCES: TODO]
-- [GRAIN  : one row per geography entity]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_geography
(
    geography_key,
    geography_name
)
SELECT
    NULL::BIGINT       AS geography_key,   -- TODO: no source mapping identified
    NULL::VARCHAR(255) AS geography_name   -- TODO: no source mapping identified
WHERE 1 = 0;


-- Dimension: dim_date
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Shared]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_date]
-- [SOURCES: TODO]
-- [GRAIN  : one row per calendar date]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_date
(
    date_key,
    date_value
)
SELECT
    NULL::BIGINT AS date_key,   -- TODO: no source mapping identified
    NULL::DATE   AS date_value  -- TODO: no source mapping identified
WHERE 1 = 0;


-- Dimension: dim_time
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Shared]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_time]
-- [SOURCES: TODO]
-- [GRAIN  : one row per time-of-day]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_time
(
    time_key,
    time_value
)
SELECT
    NULL::BIGINT AS time_key,   -- TODO: no source mapping identified
    NULL::TIME   AS time_value  -- TODO: no source mapping identified
WHERE 1 = 0;


-- Dimension: dim_month
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Shared]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_month]
-- [SOURCES: TODO]
-- [GRAIN  : one row per month]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_month
(
    month_key,
    month_value
)
SELECT
    NULL::BIGINT      AS month_key,   -- TODO: no source mapping identified
    NULL::VARCHAR(20) AS month_value  -- TODO: no source mapping identified
WHERE 1 = 0;


-- Dimension: dim_year
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Shared]
-- [TYPE   : dimension]
-- [TARGET : newmodel.dim_year]
-- [SOURCES: TODO]
-- [GRAIN  : one row per year]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.dim_year
(
    year_key,
    year_value
)
SELECT
    NULL::BIGINT AS year_key,   -- TODO: no source mapping identified
    NULL::INTEGER AS year_value -- TODO: no source mapping identified
WHERE 1 = 0;



-- Fact: fact_sponsorships
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Sponsorships & Exhibits]
-- [TYPE   : fact]
-- [TARGET : newmodel.fact_sponsorships]
-- [SOURCES: source.ams_rem_shopping_sponsorship]
-- [GRAIN  : one row per sponsorship product offering]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.fact_sponsorships
(
    sponsorships_key,
    sponsorship_quanity,
    sponsorship_amount,
    sponsorship_purchase_date,
    sponsorship_start_date,
    sponsorship_end_date,
    product_key,
    sponsor_key
)
SELECT
    CAST(s1.ams_rem_shopping_sponsorship_key_sk AS BIGINT)  AS sponsorships_key,        -- source: ams_rem_shopping_sponsorship_key_sk
    NULL::NUMERIC(18,4)                                     AS sponsorship_quanity,     -- TODO: derive from purchase/line item data
    NULL::NUMERIC(18,4)                                     AS sponsorship_amount,      -- TODO: join to accounting lineitemamount
    COALESCE(TO_DATE(s1.createdon, 'YYYY-MM-DD'), NULL)::DATE AS sponsorship_purchase_date, -- derived: createdon
    COALESCE(TO_DATE(s1.startdate, 'YYYY-MM-DD'), NULL)::DATE  AS sponsorship_start_date,   -- source: startdate
    COALESCE(TO_DATE(s1.enddate, 'YYYY-MM-DD'), NULL)::DATE    AS sponsorship_end_date,     -- source: enddate
    NULL::BIGINT                                            AS product_key,             -- TODO: map to dim_product
    NULL::BIGINT                                            AS sponsor_key              -- TODO: map to dim_sponsor
FROM source.ams_rem_shopping_sponsorship s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE
    AND s1.id IS NOT NULL;


-- Fact: fact_event_exhibits
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Sponsorships & Exhibits]
-- [TYPE   : fact]
-- [TARGET : newmodel.fact_event_exhibits]
-- [SOURCES: source.ams_rem_shopping_exhibitionbooth]
-- [GRAIN  : one row per event exhibit/booth configuration]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.fact_event_exhibits
(
    event_exhibits_key,
    event_exhibit_quantity,
    event_exhibit_amount,
    event_exhibit_date,
    event_start_date,
    event_key,
    product_key,
    event_exhibit_key,
    exhibitor_key
)
SELECT
    CAST(s1.ams_rem_shopping_exhibitionbooth_key_sk AS BIGINT) AS event_exhibits_key, -- source: ams_rem_shopping_exhibitionbooth_key_sk
    NULL::NUMERIC(18,4)                                       AS event_exhibit_quantity, -- TODO: derive from booth capacity or purchase count
    NULL::NUMERIC(18,4)                                       AS event_exhibit_amount,   -- TODO: join to accounting data
    COALESCE(TO_DATE(s1.startdate, 'YYYY-MM-DD'), NULL)::DATE AS event_exhibit_date,     -- source: startdate
    COALESCE(TO_DATE(s1.startdate, 'YYYY-MM-DD'), NULL)::DATE AS event_start_date,       -- derived proxy
    NULL::BIGINT                                              AS event_key,              -- TODO: join to dim_event
    NULL::BIGINT                                              AS product_key,            -- TODO: map to dim_product
    CAST(s1.ams_rem_shopping_exhibitionbooth_key_sk AS BIGINT) AS event_exhibit_key,    -- derived: use own surrogate as exhibit key
    NULL::BIGINT                                              AS exhibitor_key           -- TODO: join to dim_exhibitor via booth/exhibitor mapping
FROM source.ams_rem_shopping_exhibitionbooth s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE
    AND s1.id IS NOT NULL;


-- Fact: fact_event_exhibit_purchases
-- ────────────────────────────────────────────────────────
-- [DOMAIN : Sponsorships & Exhibits]
-- [TYPE   : fact]
-- [TARGET : newmodel.fact_event_exhibit_purchases]
-- [SOURCES: source.ams_rem_purchase_exhibitorbooth]
-- [GRAIN  : one row per exhibitor booth purchase/allocation]
-- ────────────────────────────────────────────────────────

INSERT INTO newmodel.fact_event_exhibit_purchases
(
    event_exhibit_purchases_key,
    event_exhibit_purchase_quantity,
    event_exhibit_purchase_amount,
    event_exhibit_purchase_date,
    event_start_date,
    event_key,
    product_key,
    event_exhibit_key,
    exhibitor_key,
    event_exhibits_key
)
SELECT
    CAST(s1.ams_rem_purchase_exhibitorbooth_key_sk AS BIGINT) AS event_exhibit_purchases_key, -- source: ams_rem_purchase_exhibitorbooth_key_sk
    1::NUMERIC(18,4)                                         AS event_exhibit_purchase_quantity, -- derived: assume one booth per row
    NULL::NUMERIC(18,4)                                      AS event_exhibit_purchase_amount,    -- TODO: join to accounting amounts
    COALESCE(TO_DATE(s1.createdon, 'YYYY-MM-DD'), NULL)::DATE AS event_exhibit_purchase_date,    -- source: createdon
    COALESCE(TO_DATE(s1.createdon, 'YYYY-MM-DD'), NULL)::DATE AS event_start_date,               -- derived proxy
    NULL::BIGINT                                             AS event_key,                       -- TODO: join to dim_event
    NULL::BIGINT                                             AS product_key,                     -- TODO: map to dim_product
    NULL::BIGINT                                             AS event_exhibit_key,               -- TODO: link to dim_exhibit/booth
    CAST(s1.ams_rem_purchase_exhibitorbooth_key_sk AS BIGINT) AS exhibitor_key,                 -- derived: use own key to link to dim_exhibitor
    NULL::BIGINT                                             AS event_exhibits_key               -- TODO: join to fact_event_exhibits
FROM source.ams_rem_purchase_exhibitorbooth s1
WHERE
    COALESCE(s1.isdeleted, FALSE) = FALSE;
