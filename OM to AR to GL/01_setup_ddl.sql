-- =============================================================================
-- BAMUL OM to AR Interface - Setup DDL
-- Run on: TARGET (New EBS) instance as APPS user
--
-- NOTE: The following mapping tables ALREADY EXIST in apps schema:
--   - apps.mapping_division_segment   (ORGANIZATION_ID, DIVISION_CODE)
--   - apps.mapping_account_segment    (OLD_ACCOUNT_FLEX_VALUE, NEW_ACCOUNT_FLEX_VALUE)
--   - apps.mapping_department_segment (OLD_DEPARTMENT_FLEX_VALUE, NEW_DEPARTMENT_FLEX_VALUE)
--   - apps.mapping_product_segment    (INVENTORY_ITEM_ID, PRODUCT_CODE)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Database Link to Legacy EBS
-- -----------------------------------------------------------------------------
CREATE DATABASE LINK legacy_instance
   CONNECT TO apps_readonly IDENTIFIED BY "<db_password>"
   USING '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=115.124.111.4)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=BMLPROD)))';

-- Verify DB link
SELECT * FROM dual@legacy_instance;

-- -----------------------------------------------------------------------------
-- 2. Order Type to AR Transaction Type Mapping
-- -----------------------------------------------------------------------------
CREATE TABLE apps.mapping_order_type (
    old_order_type_id      NUMBER        NOT NULL,
    old_order_type_name    VARCHAR2(240),
    new_cust_trx_type_name VARCHAR2(240) NOT NULL,
    new_batch_source_name  VARCHAR2(240) NOT NULL,
    description            VARCHAR2(240),
    enabled_flag           VARCHAR2(1)   DEFAULT 'Y',
    creation_date          DATE          DEFAULT SYSDATE,
    CONSTRAINT mapping_order_type_pk PRIMARY KEY (old_order_type_id)
);

-- Seed Order Type Mapping
INSERT INTO apps.mapping_order_type (old_order_type_id, old_order_type_name, new_cust_trx_type_name, new_batch_source_name, description)
VALUES (1009, 'BMD-ROUTE SALES', 'Bamul Route Sales', 'BAMUL_OM_IMPORT', 'Route Sales');

INSERT INTO apps.mapping_order_type (old_order_type_id, old_order_type_name, new_cust_trx_type_name, new_batch_source_name, description)
VALUES (1030, 'BMD-P&I SALES - DCS SALES', 'Bamul P&I Sales', 'BAMUL_OM_IMPORT', 'P&I DCS Sales');

COMMIT;

-- -----------------------------------------------------------------------------
-- 3. Interface Run Log
-- -----------------------------------------------------------------------------
CREATE TABLE apps.bamul_om_ar_interface_log (
    run_id            NUMBER        NOT NULL,
    run_date          DATE          DEFAULT SYSDATE,
    ship_date_from    DATE,
    ship_date_to      DATE,
    lines_extracted   NUMBER        DEFAULT 0,
    lines_inserted    NUMBER        DEFAULT 0,
    status            VARCHAR2(30),
    error_message     VARCHAR2(4000),
    created_by        NUMBER        DEFAULT -1,
    creation_date     DATE          DEFAULT SYSDATE,
    CONSTRAINT bamul_om_ar_log_pk PRIMARY KEY (run_id)
);

CREATE SEQUENCE apps.bamul_om_ar_log_s START WITH 1 INCREMENT BY 1 NOCACHE;

-- -----------------------------------------------------------------------------
-- 4. Reminder: Populate Account Mapping
-- The apps.mapping_account_segment table is currently EMPTY (0 rows).
-- It must be populated before running the interface.
-- Example:
-- INSERT INTO apps.mapping_account_segment (old_account_flex_value, new_account_flex_value)
-- VALUES (400001, 4001);
-- -----------------------------------------------------------------------------
