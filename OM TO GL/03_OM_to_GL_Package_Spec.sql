/* ============================================================================
   FILE:         03_OM_to_GL_Package_Spec.sql
   PACKAGE:      XXCUST_OM_GL_INTERFACE_PKG
   VERSION:      1.0 PRODUCTION
   
   DESCRIPTION:
   ------------
   Package specification for OM to GL COGS journal interface.
   Creates COGS journal entries when AR invoices are created from Sales Orders.
   
   ============================================================================ */

CREATE OR REPLACE PACKAGE apps.xxcust_om_gl_interface_pkg AS

    /*
     || IF-02: OM to GL Interface (COGS Journal Creation)
     ||
     || Creates COGS journal entries in GL_INTERFACE when AR invoices are created.
     || Uses XX_COGS_DETAILS for product cost and account information.
     ||
     || Journal Entry Pattern:
     ||   DR COGS Account (expense)     XXX
     ||      CR Inventory Account (asset)   XXX
     ||
     || Call Sequence:
     ||   1. OM to AR interface creates AR invoices
     ||   2. Call run_interface() to create COGS journals
     ||   3. Run Journal Import (Source: XXCUST_OM_COGS)
     */

    -- -------------------------------------------------------------------------
    -- Main interface procedure
    -- Processes AR invoice lines and creates COGS journal entries
    -- -------------------------------------------------------------------------
    PROCEDURE run_interface (
        p_errbuf              OUT VARCHAR2,
        p_retcode             OUT NUMBER,
        p_group_id            OUT NUMBER,
        p_ar_batch_source     IN  VARCHAR2 DEFAULT 'BAMUL_OM_IMPORT',
        p_invoice_date_from   IN  VARCHAR2 DEFAULT NULL,  -- DD-MON-YYYY
        p_invoice_date_to     IN  VARCHAR2 DEFAULT NULL,  -- DD-MON-YYYY
        p_debug_mode          IN  VARCHAR2 DEFAULT 'N'
    );

    -- -------------------------------------------------------------------------
    -- Purge old log records
    -- -------------------------------------------------------------------------
    PROCEDURE purge_log (
        p_days_to_keep IN NUMBER DEFAULT 90
    );

END xxcust_om_gl_interface_pkg;
/


