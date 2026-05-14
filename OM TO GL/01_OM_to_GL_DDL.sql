/* ============================================================================
   FILE:         01_OM_to_GL_DDL.sql
   INTERFACE:    IF-02: OM to GL (COGS Journal Creation)
   VERSION:      1.0 PRODUCTION
   PLATFORM:     Oracle EBS R12.2
   
   DESCRIPTION:
   ------------
   DDL for OM to GL COGS interface custom tables.
   Run this ONCE in the new instance as APPS user.
   
   TABLES CREATED:
   1. XX_COGS_DETAILS - Product cost and account master
   2. XX_OM_GL_INTERFACE_LOG - Interface audit log
   
   ============================================================================ */

-- ============================================================================
-- TABLE 1: XX_COGS_DETAILS (Product Cost & Account Master)
-- ============================================================================
CREATE TABLE apps.xx_cogs_details (
    cogs_detail_id       NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_code         VARCHAR2(50) NOT NULL UNIQUE,
    description          VARCHAR2(240),
    cost                 NUMBER NOT NULL,
    unit_of_measurement  VARCHAR2(3),
    material_account     VARCHAR2(20),  -- Segment 3 for CREDIT (Inventory OUT)
    cogs_account         VARCHAR2(20),  -- Segment 3 for DEBIT (Expense UP)
    enabled_flag         VARCHAR2(1) DEFAULT 'Y',
    last_updated_by      NUMBER DEFAULT -1,
    last_update_date     DATE DEFAULT SYSDATE,
    created_by           NUMBER DEFAULT -1,
    creation_date        DATE DEFAULT SYSDATE
);

COMMENT ON TABLE apps.xx_cogs_details IS 'Product-level COGS cost and GL account mapping for OM to GL interface';
COMMENT ON COLUMN apps.xx_cogs_details.product_code IS 'Unique product identifier (maps to item number)';
COMMENT ON COLUMN apps.xx_cogs_details.cost IS 'Unit cost for COGS calculation';
COMMENT ON COLUMN apps.xx_cogs_details.material_account IS 'Segment 3 value for CREDIT line (Inventory reduction)';
COMMENT ON COLUMN apps.xx_cogs_details.cogs_account IS 'Segment 3 value for DEBIT line (COGS expense)';

-- ============================================================================
-- TABLE 2: XX_OM_GL_INTERFACE_LOG (Interface Audit Log)
-- ============================================================================
CREATE TABLE apps.xx_om_gl_interface_log (
    log_id                   NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id                   VARCHAR2(50) NOT NULL,
    run_date                 DATE DEFAULT SYSDATE,
    
    -- AR Invoice reference
    ar_invoice_number        VARCHAR2(30),
    ar_invoice_line_id       NUMBER,
    ar_customer_trx_id       NUMBER,
    
    -- Product and quantity
    product_code             VARCHAR2(50),
    inventory_item_id        NUMBER,
    quantity                 NUMBER,
    unit_cost                NUMBER,
    total_cost               NUMBER,
    
    -- Accounting
    material_account         VARCHAR2(20),
    cogs_account             VARCHAR2(20),
    debit_ccid               NUMBER,
    credit_ccid              NUMBER,
    accounting_date          DATE,
    period_name              VARCHAR2(15),
    currency_code            VARCHAR2(15) DEFAULT 'INR',
    
    -- GL Interface tracking
    gl_interface_group_id    NUMBER,
    je_batch_name            VARCHAR2(100),
    
    -- Status
    interface_status         VARCHAR2(20),  -- PROCESSED, REJECTED, ERROR
    rejection_reason         VARCHAR2(2000),
    
    -- Audit
    creation_date            DATE DEFAULT SYSDATE,
    created_by               NUMBER DEFAULT -1,
    last_update_date         DATE DEFAULT SYSDATE,
    last_updated_by          NUMBER DEFAULT -1
);

COMMENT ON TABLE apps.xx_om_gl_interface_log IS 'Audit log for OM to GL COGS interface runs';

-- Indexes for performance
CREATE INDEX apps.xx_om_gl_log_n1 ON apps.xx_om_gl_interface_log (run_id);
CREATE INDEX apps.xx_om_gl_log_n2 ON apps.xx_om_gl_interface_log (ar_invoice_number);
CREATE INDEX apps.xx_om_gl_log_n3 ON apps.xx_om_gl_interface_log (interface_status);
CREATE INDEX apps.xx_om_gl_log_n4 ON apps.xx_om_gl_interface_log (product_code);
CREATE INDEX apps.xx_om_gl_log_n5 ON apps.xx_om_gl_interface_log (accounting_date);
