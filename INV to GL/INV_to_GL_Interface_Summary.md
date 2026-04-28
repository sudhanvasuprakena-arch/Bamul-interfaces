# IF-03: Legacy Oracle INV → New Oracle GL Interface
## Technical Summary Document

**Interface ID:** IF-03  
**Script File:** `INV_to_GL_Interface_v3.sql`  
**Platform:** Oracle EBS R12.2 (Legacy) → Oracle EBS R12.2 (New Instance)  
**Package:** `XXCUST_INV_GL_INTERFACE_PKG`  
**GL Source:** `XXCUST_INV_ACCT`  
**Ledger:** BAMUL_Primary_Ledger (set_of_books_id = 2021, chart_of_accounts_id = 50428)

---

## 1. What This Interface Does

Extracts inventory accounting entries from the **legacy Oracle EBS instance** and loads them into the **new Oracle GL** via the standard GL_INTERFACE open interface table. After the interface runs, the standard Oracle Journal Import program creates the actual GL journals.

In simple terms:
```
Legacy MTL Transactions → Interface Package → GL_INTERFACE → Journal Import → GL Journals (Posted)
```

---

## 2. Data Flow — Step by Step

```
LEGACY INSTANCE (via DB Link: LEGACY_INSTANCE)
        │
        │  READ
        ▼
MTL_MATERIAL_TRANSACTIONS  ──┐
MTL_TRANSACTION_ACCOUNTS   ──┤  Cursor c_inv_accounting
MTL_SYSTEM_ITEMS_B         ──┤  (one row per accounting line)
GL_CODE_COMBINATIONS       ──┤
HR_ORGANIZATION_UNITS      ──┤
WIP_ENTITIES               ──┘
        │
        │  For each accounting line:
        │  Step 1: Derive journal category
        │  Step 2: Derive/override accounting date
        │  Step 3: Derive new CCID (segment mapping)
        │  Step 4: Validate (BR-01 to BR-08)
        │  Step 5: Get source document reference
        │  Step 6: Load to GL_INTERFACE + log
        ▼
NEW INSTANCE
        │
        ├──► GL.GL_INTERFACE          (staging table, status = NEW)
        │         │
        │         │  Oracle Journal Import (Source: XXCUST_INV_ACCT)
        │         ▼
        │    GL.GL_JE_BATCHES
        │    GL.GL_JE_HEADERS
        │    GL.GL_JE_LINES           (final GL journals)
        │
        └──► APPS.XXCUST_INV_GL_INTERFACE_LOG  (audit/control log)
```

---

## 3. Source Tables (Legacy Instance — via DB Link)

| Table | Purpose | Key Columns Used |
|-------|---------|-----------------|
| `MTL_MATERIAL_TRANSACTIONS` | Transaction header | `transaction_id`, `transaction_type_id`, `transaction_date`, `organization_id`, `inventory_item_id`, `transaction_source_id` |
| `MTL_TRANSACTION_ACCOUNTS` | Accounting lines (one per DR/CR) | `transaction_id`, `reference_account` (legacy CCID), `base_transaction_value` |
| `MTL_SYSTEM_ITEMS_B` | Item description | `description` |
| `GL_CODE_COMBINATIONS` | Legacy account segments | `segment2` (cost center), `segment4` (account) |
| `HR_ORGANIZATION_UNITS` | Organisation name | `name` |
| `WIP_ENTITIES` | WIP job number | `wip_entity_name` |
| `PO_HEADERS_ALL` | PO number for receipts | `segment1` |
| `OE_ORDER_HEADERS_ALL` | Sales order number | `order_number` |
| `JAI_TAX_LINES_ALL` | GST tax code for TXN type segment | `tax_rate_code` |

---

## 4. Target Tables (New Instance)

| Table | Purpose |
|-------|---------|
| `GL.GL_INTERFACE` | Staging table — interface loads rows here with status=NEW. Journal Import reads and clears them. |
| `GL.GL_JE_BATCHES` | Journal batch created by Journal Import |
| `GL.GL_JE_HEADERS` | Journal header — one per journal category per accounting date |
| `GL.GL_JE_LINES` | Journal lines — one per GL_INTERFACE row |
| `APPS.XXCUST_INV_GL_INTERFACE_LOG` | Custom audit log — one row per accounting line processed |

---

## 5. Mapping Tables (New Instance — APPS Schema)

These tables are used by the helper functions to derive the new 8-segment COA CCID:

| Table | Purpose |
|-------|---------|
| `APPS.MAPPING_DIVISION_SEGMENT` | Maps `organization_id` → new Division segment (seg2) |
| `APPS.MAPPING_ACCOUNT_SEGMENT` | Maps legacy `segment4` (account) → new Account segment (seg3) |
| `APPS.MAPPING_DEPARTMENT_SEGMENT` | Maps legacy `segment2` (cost center) → new Department segment (seg4) |
| `APPS.MAPPING_PRODUCT_SEGMENT` | Maps `inventory_item_id` → Product segment (seg5) |
| `APPS.MAPPING_PRODUCT_PREFIX_ACCOUNT` | Fallback: maps product prefix → account segment |
| `APPS.MAPPING_ITEM_KEYWORD_ACCOUNT` | Fallback: maps item description keywords → account segment |

---

## 6. New COA Structure (8 Segments)

The new instance uses an 8-segment Chart of Accounts. The interface builds the new CCID by deriving each segment:

```
Segment 1 — ENTITY       : Fixed = '01'
Segment 2 — DIVISION     : From MAPPING_DIVISION_SEGMENT (by org_id)
Segment 3 — ACCOUNT      : From MAPPING_ACCOUNT_SEGMENT (by legacy segment4)
Segment 4 — DEPARTMENT   : From MAPPING_DEPARTMENT_SEGMENT (by legacy segment2)
Segment 5 — PRODUCT      : From MAPPING_PRODUCT_SEGMENT (by inventory_item_id)
Segment 6 — TXN_TYPE     : From JAI_TAX_LINES_ALL (0=default, 1=local/CGST/SGST, 2=interstate/IGST)
Segment 7 — FUTURE1      : Fixed = '000'
Segment 8 — FUTURE2      : Fixed = '000'
```

The derived segments are looked up in `GL_CODE_COMBINATIONS` to find the matching CCID. If not found, returns -1 and the transaction is rejected (BR-05).

---

## 7. In-Scope Transaction Types

| Type ID | Name | GL Journal Category |
|---------|------|-------------------|
| 1 | PO Receipt into Inspection | Receiving |
| 17 | PO Deliver to Subinventory / RMA Receipt | Receiving |
| 27 | Return PO Receipt into Inspection | Receiving |
| 2 | Subinventory Transfer | Inventory |
| 3 | Direct Inter-Org Transfer | Inventory |
| 21 | Intransit Shipment | Inventory |
| 22 | Intransit Receipt | Inventory |
| 33 | Sales Order Issue (COGS) | Cost of Goods Sold |
| 34 | Account Alias Issue | Inventory Accounting |
| 4 | Cycle Count / Physical Inv / Misc Adjustment | Inventory Accounting |
| 5 | Miscellaneous Issue / Receipt | Inventory Accounting |
| 24 | Standard Cost Update | Inventory Accounting |
| 35 | WIP Component Issue | WIP |
| 44 | WIP Assembly Completion | WIP |

---

## 8. Business Rules (Validation)

Each accounting line is validated before loading. If any rule fails, the transaction is rejected and logged.

| Rule | Check | Action if Failed |
|------|-------|-----------------|
| BR-01 | Transaction type must be in approved in-scope list | Reject |
| BR-02 | `base_transaction_value` must be non-zero | Exclude silently |
| BR-03 | SUM of all accounting lines for a transaction must = 0 (balanced) | Reject entire transaction |
| BR-04 | Transaction not already processed (duplicate check on log table, split by DR/CR side) | Reject as duplicate |
| BR-06 | GL period must be Open or Future-Enterable in new instance | Reject (or override — see Section 10) |
| BR-08 | New CCID must be enabled and non-summary in `GL_CODE_COMBINATIONS` | Reject |

---

## 9. Package Functions and Procedures

| Name | Type | Purpose |
|------|------|---------|
| `log_msg` | Procedure | Writes timestamped message to DBMS_OUTPUT |
| `get_division_segment` | Function | Looks up Division segment from `MAPPING_DIVISION_SEGMENT` by org_id |
| `get_account_segment` | Function | Looks up Account segment from `MAPPING_ACCOUNT_SEGMENT` by legacy account value |
| `get_account_segment_fallback` | Function | 3-step fallback: product prefix → keyword match → default 513001 |
| `get_department_segment` | Function | Looks up Department segment from `MAPPING_DEPARTMENT_SEGMENT` |
| `get_product_segment` | Function | Looks up Product segment from `MAPPING_PRODUCT_SEGMENT` by item_id |
| `get_txn_type_segment` | Function | Derives TXN type segment from GST tax code on legacy JAI_TAX_LINES_ALL |
| `derive_new_ccid` | Function | Master function — calls all segment helpers, builds 8-segment CCID, looks up in GL_CODE_COMBINATIONS |
| `get_period_name` | Function | Finds open/future-enterable GL period for a given date from GL_PERIOD_STATUSES |
| `get_sob_id` | Function | Returns primary ledger set_of_books_id (filters out -1 rows) |
| `get_je_category` | Function | Maps transaction_type_id → GL journal category using g_category_map |
| `get_source_doc_ref` | Function | Derives source document reference (PO#, SO#, JOB#, INV ADJ) |
| `validate_transaction` | Function | Applies BR-01 to BR-08, returns TRUE/FALSE |
| `load_gl_line` | Procedure | Inserts one row into GL_INTERFACE and one row into XXCUST_INV_GL_INTERFACE_LOG |
| `run_interface` | Procedure | Main entry point — runs the full E-V-T-L processing loop |
| `purge_log` | Procedure | Deletes PROCESSED log records older than N days (default 90) |

---

## 10. Key Design Decisions and Customisations

### 10.1 Accounting Date Override for Closed/Missing GL Periods
**Problem:** Legacy transactions are dated Dec 2023. The new GL calendar only starts from APR-2025 and the earliest open period is MAR-2026. Dec 2023 period does not exist in the new instance.

**Solution implemented:** If `get_period_name` returns NULL for the original transaction date, the interface automatically overrides the accounting date to `TRUNC(SYSDATE, 'MM')` (first day of current month) and re-derives the period. This allows historical legacy transactions to be posted to the currently open period.

```sql
-- In run_interface Step 2:
l_period_name := get_period_name(r.accounting_date);
IF l_period_name IS NULL THEN
    l_accounting_date := TRUNC(SYSDATE, 'MM');   -- e.g. 01-APR-2026
    l_period_name     := get_period_name(l_accounting_date);
    log_msg('Accounting date overridden from ' || original_date || ' to ' || new_date);
ELSE
    l_accounting_date := r.accounting_date;
END IF;
```

The original transaction date is preserved in the journal description for audit purposes.

---

### 10.2 Manual CCID Insert for Missing Account Combinations
**Problem:** The derived segment combination `01.01.513001.00000.00000000.1.000.000` did not exist in `GL_CODE_COMBINATIONS` in the new instance.

**Solution:** Manually inserted the missing CCID using:
```sql
INSERT INTO apps.gl_code_combinations (
    code_combination_id, chart_of_accounts_id,
    segment1, segment2, segment3, segment4,
    segment5, segment6, segment7, segment8,
    enabled_flag, summary_flag, account_type,
    detail_posting_allowed_flag, detail_budgeting_allowed_flag,
    last_update_date, last_updated_by
)
SELECT apps.gl_code_combinations_s.nextval, 50428,
       '01','01','513001','00000','00000000','1','000','000',
       'Y','N','E','Y','Y', SYSDATE, -1
FROM dual;
```
This created CCID **7030** which is used for all WIP transactions in the test run.

> **Note for production:** All required account combinations should be pre-created in the new COA before running the interface. Use the pre-run check query (Section 12) to identify missing CCIDs.

---

### 10.3 BR-04 Duplicate Check — DR/CR Side Aware
**Problem:** Each MTL transaction has two accounting lines with the same CCID — one DR (+13) and one CR (-13). The original BR-04 check on `transaction_id + CCID` was blocking the second line as a duplicate.

**Solution:** Added DR/CR side awareness to the duplicate check:
```sql
AND (   (p_base_txn_value > 0 AND entered_dr IS NOT NULL)
     OR (p_base_txn_value < 0 AND entered_cr IS NOT NULL) )
```
This ensures the DR line and CR line of the same transaction are treated as separate entries.

---

### 10.4 Currency Code Default
**Problem:** Legacy MTL transactions had NULL `currency_code`, causing `ORA-01400: cannot insert NULL into GL_INTERFACE.CURRENCY_CODE`.

**Solution:** Added NVL default in the cursor:
```sql
NVL(mmt.currency_code, 'INR')   currency_code
```
The functional currency of BAMUL_Primary_Ledger is **INR**.

---

### 10.5 GL_PERIOD_STATUSES Instead of GL_PERIODS + GL_SETS_OF_BOOKS
**Problem:** Original `get_period_name` used `GL_SETS_OF_BOOKS` which does not exist in this instance. Also used `p_accounting_date BETWEEN` which caused `ORA-00984: column not allowed here`.

**Solution:** Rewrote to use `GL_PERIOD_STATUSES` directly with explicit `<=` and `>=` comparisons:
```sql
FROM   apps.gl_period_statuses gps
JOIN   apps.ar_system_parameters_all asp ON asp.set_of_books_id = gps.set_of_books_id
WHERE  gps.start_date <= p_accounting_date
AND    gps.end_date   >= p_accounting_date
AND    (gps.closing_status = 'O' OR gps.closing_status = 'F')
AND    asp.set_of_books_id > 0   -- excludes -1 rows in AR_SYSTEM_PARAMETERS_ALL
```

---

### 10.6 get_sob_id Fix — AR_SYSTEM_PARAMETERS_ALL Has -1 Rows
**Problem:** `AR_SYSTEM_PARAMETERS_ALL` contains rows with `set_of_books_id = -1` (system rows). `MIN()` was returning -1 instead of 2021.

**Solution:**
```sql
SELECT MIN(set_of_books_id) INTO l_sob_id
FROM AR_SYSTEM_PARAMETERS_ALL
WHERE set_of_books_id > 0;
```

---

### 10.7 WIP_ENTITIES Instead of WIP_DISCRETE_JOBS for Job Name
**Problem:** `WIP_DISCRETE_JOBS` does not have a `WIP_ENTITY_NAME` column. The column exists on `WIP_ENTITIES`.

**Solution:** Changed the WIP job lookup to join `WIP_ENTITIES@LEGACY_INSTANCE` instead.

---

### 10.8 MTL_MATERIAL_TRANSACTIONS Uses TRANSACTION_SOURCE_ID Not SOURCE_HEADER_ID
**Problem:** `MTL_MATERIAL_TRANSACTIONS` does not have a `SOURCE_HEADER_ID` column. The correct column is `TRANSACTION_SOURCE_ID`.

**Solution:** Cursor uses `mmt.transaction_source_id` aliased as `source_header_id`.

---

## 11. How to Run the Interface

```sql
DECLARE
    l_errbuf   VARCHAR2(2000);
    l_retcode  NUMBER;
    l_group_id NUMBER;
BEGIN
    XXCUST_INV_GL_INTERFACE_PKG.run_interface(
        p_errbuf          => l_errbuf,
        p_retcode         => l_retcode,
        p_group_id        => l_group_id,
        p_organization_id => NULL,            -- NULL = all orgs
        p_txn_date_from   => '01-DEC-2023',   -- DD-MON-YYYY
        p_txn_date_to     => '31-DEC-2023',
        p_txn_type_ids    => NULL,            -- NULL = all in-scope types
        p_debug_mode      => 'Y'              -- Y for detailed logging
    );
    DBMS_OUTPUT.PUT_LINE('Return Code : ' || l_retcode);
    DBMS_OUTPUT.PUT_LINE('Message     : ' || l_errbuf);
    DBMS_OUTPUT.PUT_LINE('Group ID    : ' || l_group_id);
END;
/
```

After the interface runs, execute Journal Import in EBS UI:
> **GL → Journals → Import → Run**
> - Source: `XXCUST_INV_ACCT`
> - Group ID: `<value from l_group_id above>`
> - Post Errors to Suspense: No

---

## 12. Pre-Run Checks

Run these before executing the interface:

**Check 1 — Open GL periods:**
```sql
SELECT DISTINCT period_name, start_date, end_date, closing_status
FROM apps.gl_period_statuses
WHERE set_of_books_id = 2021
AND closing_status IN ('O','F')
AND adjustment_period_flag = 'N'
ORDER BY start_date;
```

**Check 2 — Missing CCIDs (segment combinations not in GL_CODE_COMBINATIONS):**
```sql
-- Run derive_new_ccid logic manually for each legacy CCID to confirm
-- the derived combination exists in GL_CODE_COMBINATIONS
SELECT code_combination_id, segment1, segment2, segment3, segment4,
       segment5, segment6, segment7, segment8, enabled_flag
FROM apps.gl_code_combinations
WHERE chart_of_accounts_id = 50428
AND enabled_flag = 'Y'
ORDER BY segment1, segment2, segment3;
```

**Check 3 — Mapping table coverage:**
```sql
SELECT 'MAPPING_DIVISION_SEGMENT'   tbl, COUNT(*) cnt FROM APPS.MAPPING_DIVISION_SEGMENT    UNION ALL
SELECT 'MAPPING_ACCOUNT_SEGMENT',        COUNT(*)     FROM APPS.MAPPING_ACCOUNT_SEGMENT     UNION ALL
SELECT 'MAPPING_DEPARTMENT_SEGMENT',     COUNT(*)     FROM APPS.MAPPING_DEPARTMENT_SEGMENT  UNION ALL
SELECT 'MAPPING_PRODUCT_SEGMENT',        COUNT(*)     FROM APPS.MAPPING_PRODUCT_SEGMENT     UNION ALL
SELECT 'MAPPING_PRODUCT_PREFIX_ACCOUNT', COUNT(*)     FROM APPS.MAPPING_PRODUCT_PREFIX_ACCOUNT UNION ALL
SELECT 'MAPPING_ITEM_KEYWORD_ACCOUNT',   COUNT(*)     FROM APPS.MAPPING_ITEM_KEYWORD_ACCOUNT;
```

---

## 13. Test Run Results (DEV Instance — 27-APR-2026)

| Item | Value |
|------|-------|
| Run ID | INV-20260427105235 |
| Group ID | 126 |
| Transactions processed | 3 (TxnIDs: 275809253, 278297615, 278334580) |
| Accounting lines processed | 6 (2 per transaction — DR and CR) |
| Rejections | 0 |
| Errors | 0 |
| Legacy transaction date | Dec 2023 |
| GL accounting date (overridden) | 01-APR-2026 |
| GL period | APR-26 |
| Journal category | WIP |
| New CCID used | 7030 (01.01.513001.00000.00000000.1.000.000) |
| Total DR | 39 INR |
| Total CR | 39 INR |
| Balance | BALANCED (variance = 0) |
| Journal batches created | 6016, 6017, 6018 |
| Batch status | P (Posted) |

---

## 14. Known Gaps / Pending Items for Production

| Item | Detail |
|------|--------|
| Org 212 not in MAPPING_DIVISION_SEGMENT | Defaults to division `01`. Add org 212 with correct division code. |
| Account 103000 not in MAPPING_ACCOUNT_SEGMENT | Falls back to `513001` (General Consumables). Add correct mapping. |
| Department old_flex_value=2 not mapped | Defaults to `00000`. Add correct new department value. |
| Item 43006 not in MAPPING_PRODUCT_SEGMENT | Defaults to `00000000`. Add product code mapping. |
| CCID 7030 manually inserted | Created for test. In production, all required CCIDs should exist in GL_CODE_COMBINATIONS before running. |
| Accounting date override | Currently overrides to first day of current month when original period is closed. For production, confirm with GL accountant whether historical periods should be opened or override approach is acceptable. |
| Currency code default | Legacy transactions had NULL currency_code, defaulted to INR. Confirm this is correct for all orgs. |

---

## 15. File Structure of INV_to_GL_Interface_v3.sql

| Section | Content |
|---------|---------|
| Section 1 | DDL — `CREATE TABLE XXCUST_INV_GL_INTERFACE_LOG` + 7 indexes |
| Section 2 | Package Specification — `XXCUST_INV_GL_INTERFACE_PKG` |
| Section 3 | Package Body — all functions, procedures, main loop |
| Section 4 | Reconciliation Queries (4A–4F) |
| Section 5 | Execution Guide |
