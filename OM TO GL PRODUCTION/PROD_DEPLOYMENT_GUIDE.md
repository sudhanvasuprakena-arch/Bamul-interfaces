# OM to GL COGS Interface — Production Deployment Guide

## Overview

This interface creates Cost of Goods Sold (COGS) journal entries in GL when AR invoices are created from Sales Orders. It is integrated into the existing OM-to-AR process (`BAMUL_OM_AR_INTERFACE_PKG`) and automatically submits Journal Import after staging entries.

**Journal Entry Pattern:**
```
DR  COGS Account (541xxx)        — Expense UP
    CR  Inventory Account (121xxx)   — Asset DOWN
```

---

## Pre-requisites (Manual Setup — One Time)

### A. Register GL Journal Source
**Navigation:** General Ledger > Setup > Journals > Sources

| Field | Value |
|-------|-------|
| Source Name | XXCUST_OM_COGS |
| Source Key | XXCUST_OM_COGS |
| Description | OM to GL COGS Interface |

### B. Register GL Journal Category
**Navigation:** General Ledger > Setup > Journals > Categories

| Field | Value |
|-------|-------|
| Category | Cost of Goods Sold |
| Category Key | Cost of Goods Sold |
| Description | Inventory Sales Order Issues (COGS) |

> Note: This category likely already exists. Verify before creating.

### C. GL Period
Ensure the target accounting period is **Open** or **Future-Enterable** for journal import.

---

## SQL Scripts — Execution Order

Run as **APPS** user in the following order:

| Step | File | Purpose |
|------|------|---------|
| 1 | `01_OM_to_GL_DDL.sql` | Creates tables: `XX_COGS_DETAILS`, `XX_OM_GL_INTERFACE_LOG` |
| 2 | `02_Populate_COGS_Details.sql` | Populates `XX_COGS_DETAILS` with 757 products |
| 3 | `03_OM_to_GL_Package_Spec.sql` | Creates package spec: `XXCUST_OM_GL_INTERFACE_PKG` |
| 4 | `04_OM_to_GL_Package_Body.sql` | Creates package body: `XXCUST_OM_GL_INTERFACE_PKG` |
| 5 | `Integration_reference_only.sql.sql` | It is only for reference of integration. Don't run this one. |

---

## Dependencies (Must Already Exist)

| Object | Rows | Used For |
|--------|------|----------|
| `mapping_product_segment` | 768 | Product segment (segment5) derivation |
| `mapping_division_segment` | 3 | Division segment derivation |
| `mapping_department_segment` | 58 | Department segment derivation |
| `gl_je_sources` (XXCUST_OM_COGS) | 1 | Journal Import source |
| `gl_access_set_norm_assign` | — | GLLEZL argument2 (access_set_id) |
| `gl_journal_import_s` (sequence) | — | Interface run ID generation |

---

## How It Works (Runtime Flow)

1. **OM-to-AR** (`BAMUL_OM_AR_INTERFACE_PKG.run_interface`) inserts AR invoice lines
2. If `l_lines_inserted > 0`:
   - Calls `XXCUST_OM_GL_INTERFACE_PKG.run_interface` → stages COGS entries in `GL_INTERFACE`
   - Inserts control row into `GL_INTERFACE_CONTROL`
   - Submits **Journal Import** (`GLLEZL`) concurrent program automatically
3. Journal Import moves data from `GL_INTERFACE` → `GL_JE_LINES` (actual journal)
4. AutoInvoice runs as usual (independent of COGS)

---

## Concurrent Programs

| Program | Short Name | Triggered By |
|---------|-----------|--------------|
| Order Management To AR Invoice CP | Custom | Scheduled / Manual |
| Journal Import | GLLEZL | Auto-submitted by OM-to-AR after COGS staging |

### GLLEZL Parameters (auto-populated, no hardcoding):

| Argument | Value | Source |
|----------|-------|--------|
| 1 | Interface Run ID | `gl_journal_import_s.NEXTVAL` |
| 2 | Data Access Set ID | `gl_access_set_norm_assign.access_set_id` |
| 3 | Post Errors to Suspense | N |
| 4 | From Date | (empty) |
| 5 | To Date | (empty) |
| 6 | Create Summary Journals | N |
| 7 | Import DFF | N |
| 8 | Process | Y |

---

## Post-Deployment Validation

```sql
-- 1. Verify tables created and populated
SELECT COUNT(*) FROM apps.xx_cogs_details;          -- Expected: 757
SELECT COUNT(*) FROM apps.xx_om_gl_interface_log;   -- Expected: 0

-- 2. Verify packages are VALID
SELECT object_name, object_type, status 
FROM all_objects 
WHERE object_name IN ('XXCUST_OM_GL_INTERFACE_PKG', 'BAMUL_OM_AR_INTERFACE_PKG') 
AND object_type LIKE 'PACKAGE%'
ORDER BY object_name, object_type;
-- All should be VALID

-- 3. Verify GL source registered
SELECT je_source_name, user_je_source_name 
FROM apps.gl_je_sources 
WHERE user_je_source_name = 'XXCUST_OM_COGS';

-- 4. Verify dependency (OM-to-AR references GL package)
SELECT referenced_name 
FROM all_dependencies 
WHERE name = 'BAMUL_OM_AR_INTERFACE_PKG' 
AND referenced_name = 'XXCUST_OM_GL_INTERFACE_PKG';

-- 5. Verify mapping tables populated
SELECT 
    (SELECT COUNT(*) FROM apps.mapping_product_segment) product_mapping,
    (SELECT COUNT(*) FROM apps.mapping_division_segment) division_mapping,
    (SELECT COUNT(*) FROM apps.mapping_department_segment) department_mapping
FROM dual;

-- 6. Verify no gap between COGS and product mapping
SELECT mps.product_code 
FROM apps.mapping_product_segment mps
WHERE NOT EXISTS (
    SELECT 1 FROM apps.xx_cogs_details xcd 
    WHERE xcd.product_code = mps.product_code
);
-- Expected: 0 rows
```

---

## Post-Run Validation (After First Live Run)

```sql
-- Check GL_INTERFACE staging (should be 0 after successful import)
SELECT COUNT(*) FROM apps.gl_interface 
WHERE user_je_source_name = 'XXCUST_OM_COGS';

-- Check journals created
SELECT jh.je_header_id, jh.name, jh.status, jh.period_name,
       (SELECT COUNT(*) FROM apps.gl_je_lines jl WHERE jl.je_header_id = jh.je_header_id) line_count
FROM apps.gl_je_headers jh
WHERE jh.je_source = (SELECT je_source_name FROM apps.gl_je_sources WHERE user_je_source_name = 'XXCUST_OM_COGS')
ORDER BY jh.creation_date DESC
FETCH FIRST 5 ROWS ONLY;

-- Check COGS log
SELECT run_id, interface_status, COUNT(*) 
FROM apps.xx_om_gl_interface_log 
GROUP BY run_id, interface_status 
ORDER BY run_id DESC;

-- Verify DR = CR balance
SELECT jh.name, 
       SUM(NVL(jl.entered_dr,0)) total_dr, 
       SUM(NVL(jl.entered_cr,0)) total_cr,
       SUM(NVL(jl.entered_dr,0)) - SUM(NVL(jl.entered_cr,0)) difference
FROM apps.gl_je_lines jl
JOIN apps.gl_je_headers jh ON jh.je_header_id = jl.je_header_id
WHERE jh.je_source = (SELECT je_source_name FROM apps.gl_je_sources WHERE user_je_source_name = 'XXCUST_OM_COGS')
GROUP BY jh.name
ORDER BY jh.name DESC
FETCH FIRST 5 ROWS ONLY;
-- Difference should always be 0
```

---

## Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| GLLEZL fails with ORA-01403 | Missing `GL_INTERFACE_CONTROL` row | Check if package compiled correctly — control row insert is in the code |
| GLLEZL Signal 11 (segfault) | Invalid arguments passed | Verify GLLEZL arguments match the pattern above |
| Products rejected | Product not in `XX_COGS_DETAILS` | Add missing product with cost and account info |
| CCID not found | Product segment not in `GL_CODE_COMBINATIONS` | Package auto-creates via `FND_FLEX_EXT.GET_CCID` |
| Period closed | GL period not Open/Future | Open the period in GL |

---

## Key Tables

| Table | Purpose |
|-------|---------|
| `XX_COGS_DETAILS` | Product cost master (cost, UOM, accounts) |
| `XX_OM_GL_INTERFACE_LOG` | Audit log per line processed/rejected |
| `GL_INTERFACE` | Staging table for journal lines (temporary) |
| `GL_INTERFACE_CONTROL` | Control row for GLLEZL to find data |
| `GL_JE_HEADERS` / `GL_JE_LINES` | Final journal entries after import |
