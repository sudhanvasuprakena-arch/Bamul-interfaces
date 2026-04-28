-- =============================================================================
-- BAMUL OM to AR Interface - Package Specification
-- Run on: TARGET (New EBS) instance as APPS user
-- =============================================================================
CREATE OR REPLACE PACKAGE apps.bamul_om_ar_interface_pkg AS

    -- Main procedure: extract shipped OM lines and insert into AR interface
    -- p_ship_date_from/to: defaults to previous day if NULL
    PROCEDURE run_interface (
        p_ship_date_from IN DATE DEFAULT NULL,
        p_ship_date_to   IN DATE DEFAULT NULL,
        p_submit_autoinv IN VARCHAR2 DEFAULT 'Y'
    );

    -- Submit AutoInvoice concurrent program
    PROCEDURE submit_autoinvoice (
        p_org_id           IN NUMBER,
        p_batch_source_name IN VARCHAR2
    );

END bamul_om_ar_interface_pkg;
/
