# BAMUL OM to GL COGS Interface - Setup & Execution Flow

## Overview

### Goal

Automatically create Cost of Goods Sold (COGS) journal entries in GL for shipped sales orders that have been imported into AR. This interface reads AR invoice lines, looks up product costs from a master table, and generates balanced journal entries (DR: COGS Account, CR: Material/Inventory Account) in GL_INTERFACE for Journal Import.

### Deliverables

- A PL/SQL package (`xx_om_gl_cogs_pkg`) that reads AR interface lines, calculates COGS based on product master data, and inserts balanced journal entries into GL_INTERFACE.
- A COGS details master table (`xx_cogs_details`) containing product codes, unit costs, and GL account mappings.
- An interface log table (`xx_om_gl_interface_log`) to track processed, rejected, and errored transactions.
- Validation logic to ensure GL periods are open, CCIDs are valid, and costs are positive before creating journal entries.

### Approach

1. **Read** – Query AR interface lines from `ra_interface_lines_all` joined with `ra_interface_distributions_all` to get product codes from segment5.
2. **Validate** – Check if product exists in COGS master, cost is positive, GL period is open, and CCIDs are derivable.
3. **Calculate** – Compute total COGS = quantity × unit_cost.
4. **Load** – Insert balanced journal entries (debit COGS account, credit material account) into `gl_interface`.
5. **Log** – Record all processed, rejected, and errored transactions in the interface log table.
6. **Import** – Run GL Journal Import to create actual journal entries from GL_INTERFACE.

---

## 1. Setup DDL

This section contains all the database objects (tables, package) that must be created on the Target instance before the interface can be executed.

> **Run as:** APPS user on TARGET instance.

### 1.1 COGS Details Master Table

This table stores the product master data including unit costs and GL account mappings for COGS and material accounts.

```sql
CREATE TABLE apps.xx_cogs_details (
    product_code         VARCHAR2(50)  NOT NULL,
    description          VARCHAR2(240),
    cost                 NUMBER(15,2)  NOT NULL,
    unit_of_measurement  VARCHAR2(3),
    material_account     VARCHAR2(20)  NOT NULL,  -- Segment3 for Material/Inventory, credit side
    cogs_account         VARCHAR2(20)  NOT NULL,  -- Segment3 for COGS, debit side
    enabled_flag         VARCHAR2(1)   DEFAULT 'Y',
    creation_date        DATE          DEFAULT SYSDATE,
    created_by           NUMBER        DEFAULT -1,
    last_update_date     DATE          DEFAULT SYSDATE,
    last_updated_by      NUMBER        DEFAULT -1,
    CONSTRAINT xx_cogs_details_pk PRIMARY KEY (product_code)
);
```

### 1.2 Interface Log Table

This table tracks each transaction processed by the interface — capturing product details, costs, CCIDs, status, and rejection reasons.

```sql
CREATE TABLE apps.xx_om_gl_interface_log (
    log_id                  NUMBER        NOT NULL,
    run_id                  VARCHAR2(50)  NOT NULL,
    run_date                DATE          DEFAULT SYSDATE,
    ar_invoice_number       VARCHAR2(30),
    ar_invoice_line_id      NUMBER,
    ar_customer_trx_id      NUMBER,
    product_code            VARCHAR2(50),
    inventory_item_id       NUMBER,
    quantity                NUMBER,
    unit_cost               NUMBER,
    total_cost              NUMBER,
    material_account        VARCHAR2(20),
    cogs_account            VARCHAR2(20),
    debit_ccid              NUMBER,
    credit_ccid             NUMBER,
    accounting_date         DATE,
    period_name             VARCHAR2(15),
    currency_code           VARCHAR2(15),
    gl_interface_group_id   NUMBER,
    je_batch_name           VARCHAR2(100),
    interface_status        VARCHAR2(20),  -- PROCESSED, REJECTED, ERROR
    rejection_reason        VARCHAR2(4000),
    creation_date           DATE          DEFAULT SYSDATE,
    created_by              NUMBER        DEFAULT -1,
    last_update_date        DATE          DEFAULT SYSDATE,
    last_updated_by         NUMBER        DEFAULT -1,
    CONSTRAINT xx_om_gl_log_pk PRIMARY KEY (log_id)
);

CREATE SEQUENCE apps.xx_om_gl_log_s START WITH 1 INCREMENT BY 1 NOCACHE;
```

### 1.3 Seed COGS Details Data

Populate the COGS master table with product codes, costs, and account mappings. This data must exist before running the interface.

```sql
-- Example seed data (adjust costs and accounts as needed)
INSERT INTO apps.xx_cogs_details (product_code, description, cost, unit_of_measurement, material_account, cogs_account)
VALUES ('MITK1000', 'Toned Milk 1000 ml', 44.95, 'PKT', '121001', '511002');

INSERT INTO apps.xx_cogs_details (product_code, description, cost, unit_of_measurement, material_account, cogs_account)
VALUES ('MITK0500', 'Toned Milk 500 ml', 45.10, 'EA', '121001', '511002');

INSERT INTO apps.xx_cogs_details (product_code, description, cost, unit_of_measurement, material_account, cogs_account)
VALUES ('MISH0500', 'Shubham Milk 500 ml', 49.22, 'EA', '121001', '511002');

INSERT INTO apps.xx_cogs_details (product_code, description, cost, unit_of_measurement, material_account, cogs_account)
VALUES ('CUKA0200', 'Curd 200g', 45.00, 'EA', '121001', '511002');

-- Add more products as needed
COMMIT;
```

---

## 2. Package Installation

Compile the package specification and body in the following order:

### 2.1 Package Specification

```sql
@03_OM_to_GL_Package_Spec.sql
```

### 2.2 Package Body

```sql
@04_OM_to_GL_Package_Body.sql
```

---

## 3. Execution Flow

This section provides the step-by-step sequence to execute the OM to GL COGS interface.

### 3.1 Prerequisites

1. **AR Interface must be run first** – The OM to AR interface must complete successfully and populate `ra_interface_lines_all` and `ra_interface_distributions_all`.
2. **GL Period must be open** – The accounting period for the transaction dates must be open or future-enterable.
3. **COGS master data must exist** – All product codes in AR interface lines must have corresponding entries in `xx_cogs_details`.

### 3.2 Execution

The OM to GL COGS interface is automatically called by the OM to AR interface package. It can also be run standalone:

```sql
SET SERVEROUTPUT ON SIZE UNLIMITED;

DECLARE
    l_retcode   NUMBER;
    l_group_id  NUMBER;
BEGIN
    apps.xx_om_gl_cogs_pkg.process_om_to_gl_cogs(
        p_ar_batch_source    => 'BAMUL_OM_IMPORT',
        p_invoice_date_from  => '29-APR-2026',
        p_invoice_date_to    => '29-APR-2026',
        p_retcode            => l_retcode,
        p_group_id           => l_group_id
    );
    
    DBMS_OUTPUT.PUT_LINE('Return Code: ' || l_retcode);
    DBMS_OUTPUT.PUT_LINE('Group ID: ' || l_group_id);
END;
/
```

### 3.3 Output

The interface will display:
- Run ID and Group ID for GL Journal Import
- Count of lines processed, rejected, and errored
- Instructions for next steps (run Journal Import)

Example output:
```sql
=================================================================
IF-02: OM to GL COGS Interface - Run ID: OMGL-20260514092345
Group ID (Journal Import): 824
Ledger (SOB ID)          : 2021
AR Batch Source          : BAMUL_OM_IMPORT
Invoice Date From        : 29-APR-2026
Invoice Date To          : 29-APR-2026
=================================================================
REJECTED: Order 11104779328 Line  - Product PN001000 not found
REJECTED: Order 11104779328 Line  - Product MINK0200 not found
=================================================================
IF-02 Complete - Run ID: OMGL-20260514092345
  Lines Processed: 24
  Lines Rejected : 6
  Errors         : 0
-----------------------------------------------------------------
Next Step: Run Journal Import in GL
  Source: XXCUST_OM_COGS
  Group ID: 824
=================================================================
```

### 3.4 Run GL Journal Import

After the interface completes successfully, run GL Journal Import to create actual journal entries:

**Navigation:** General Ledger > Journals > Import > Run

| Parameter | Value |
|-----------|-------|
| Source | XXCUST_OM_COGS |
| Group ID | (from interface output) |
| Ledger | Your Ledger Name |

---

## 4. Validation & Reconciliation Queries

Use these queries to verify the interface results and troubleshoot issues.

### 4.1 Check OM to GL Interface Log

View all transactions processed by the interface, including status and rejection reasons.

```sql
-- Check OM to GL Log
SELECT 
    run_id,
    ar_invoice_number,
    product_code,
    quantity,
    unit_cost,
    total_cost,
    interface_status,
    rejection_reason
FROM apps.xx_om_gl_interface_log
WHERE run_date >= TRUNC(SYSDATE)
ORDER BY creation_date DESC;
```

### 4.2 Check GL_INTERFACE

Verify that journal entries were created in GL_INTERFACE with status = 'NEW'.

```sql
-- Check GL_INTERFACE
SELECT 
    group_id,
    reference1 AS order_number,
    reference10 AS run_id,
    entered_dr,
    entered_cr,
    code_combination_id,
    status
FROM apps.gl_interface
WHERE user_je_source_name = 'XXCUST_OM_COGS'
AND creation_date >= TRUNC(SYSDATE)
ORDER BY group_id DESC, reference1;
```

### 4.3 Balance Check

Verify that debits equal credits for each group_id (journal batch).

```sql
-- Balance Check
SELECT 
    group_id,
    SUM(NVL(entered_dr, 0)) total_dr,
    SUM(NVL(entered_cr, 0)) total_cr,
    SUM(NVL(entered_dr, 0)) - SUM(NVL(entered_cr, 0)) variance,
    CASE 
        WHEN ABS(SUM(NVL(entered_dr, 0)) - SUM(NVL(entered_cr, 0))) < 0.01 
        THEN 'BALANCED'
        ELSE 'OUT OF BALANCE'
    END balance_status
FROM apps.gl_interface
WHERE user_je_source_name = 'XXCUST_OM_COGS'
GROUP BY group_id
ORDER BY group_id DESC;
```

### 4.4 Summary by Status

Get a count of processed, rejected, and errored lines by run.

```sql
-- Summary by Status
SELECT 
    run_id,
    interface_status,
    COUNT(*) as line_count,
    SUM(total_cost) as total_amount
FROM apps.xx_om_gl_interface_log
WHERE run_date >= TRUNC(SYSDATE)
GROUP BY run_id, interface_status
ORDER BY run_id DESC, interface_status;
```

### 4.5 Verify GL Journals Created

After successful Journal Import, verify the actual GL journals were created.

```sql
-- Verify GL Journals Created
SELECT 
    gjh.je_batch_id,
    gjh.je_header_id,
    gjh.name as journal_name,
    gjh.status,
    gjh.period_name,
    gjl.code_combination_id,
    gjl.entered_dr,
    gjl.entered_cr,
    gjl.reference_1
FROM apps.gl_je_headers gjh
JOIN apps.gl_je_lines gjl ON gjh.je_header_id = gjl.je_header_id
WHERE gjh.je_source = 'XXCUST_OM_COGS'
AND gjh.creation_date >= TRUNC(SYSDATE)
ORDER BY gjh.je_header_id, gjl.je_line_num;
```

---

## 5. Troubleshooting

### 5.1 Common Rejection Reasons

| Rejection Reason | Solution |
|------------------|----------|
| Product code not found in XX_COGS_DETAILS | Add the missing product to `xx_cogs_details` table |
| Product has zero or negative cost | Update the cost in `xx_cogs_details` to a positive value |
| No open GL period found | Open the GL period for the transaction date |
| CCID not found | Verify segment values exist in GL chart of accounts |

### 5.2 Re-running Failed Transactions

The interface automatically excludes already-processed lines using the `NOT EXISTS` clause in the cursor. To reprocess rejected lines:

1. Fix the root cause (add missing products, open periods, etc.)
2. Re-run the interface with the same date range
3. Only previously rejected lines will be picked up

### 5.3 Filtering Invalid Products

The interface automatically filters out lines with product code '00000000' (invalid/placeholder codes). These lines will not appear in the log table.

---

## 6. Integration with OM to AR Interface

The OM to GL COGS interface is automatically called by the OM to AR interface package after AR lines are successfully inserted. The integration flow is:

1. OM to AR interface runs and populates `ra_interface_lines_all`
2. OM to AR interface calls `xx_om_gl_cogs_pkg.process_om_to_gl_cogs`
3. OM to GL COGS interface reads AR lines and creates GL journal entries
4. Both interfaces log their results and return status codes

This ensures COGS entries are created immediately after revenue recognition, maintaining accounting consistency.

---
