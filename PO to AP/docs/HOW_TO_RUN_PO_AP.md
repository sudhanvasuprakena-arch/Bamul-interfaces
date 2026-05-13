# How to Run the PO to AP Interface

This guide covers everything from initial setup and prerequisites to running and verifying the PO-to-AP interface.

---

## Prerequisites — One-Time Setup

These steps must be completed **before** the interface can run. Each step only needs to be done once (or when new data is added).

### Step 1: Create the Database Link

The interface reads receipt and PO data from the legacy EBS instance via a database link. Create it on the **new** EBS instance:

```sql
-- Run as APPS on the NEW instance
CREATE DATABASE LINK LEGACY_INSTANCE
    CONNECT TO apps IDENTIFIED BY <password>
    USING '<legacy_tns_alias>';
```

**Verify it works:**
```sql
SELECT COUNT(*) FROM apps.po_headers_all@LEGACY_INSTANCE;
```

**Required grants on legacy instance** (run as DBA on legacy):
```sql
-- The DB link user must have SELECT on these legacy tables:
GRANT SELECT ON apps.rcv_transactions          TO <db_link_user>;
GRANT SELECT ON apps.rcv_shipment_headers      TO <db_link_user>;
GRANT SELECT ON apps.rcv_shipment_lines        TO <db_link_user>;
GRANT SELECT ON apps.po_headers_all            TO <db_link_user>;
GRANT SELECT ON apps.po_lines_all              TO <db_link_user>;
GRANT SELECT ON apps.po_line_locations_all     TO <db_link_user>;
GRANT SELECT ON apps.po_distributions_all      TO <db_link_user>;
GRANT SELECT ON apps.ap_suppliers              TO <db_link_user>;
GRANT SELECT ON apps.ap_supplier_sites_all     TO <db_link_user>;
GRANT SELECT ON apps.hr_all_organization_units TO <db_link_user>;
GRANT SELECT ON apps.gl_code_combinations      TO <db_link_user>;
GRANT SELECT ON apps.jai_tax_lines_all         TO <db_link_user>;
GRANT SELECT ON apps.mtl_system_items_b        TO <db_link_user>;
```

---

### Step 2: Migrate Suppliers

The interface validates that each supplier exists in the **new** AP instance. Invoices for suppliers that don't exist will be **rejected**.

Run the supplier migration script:
```sql
-- Run on the NEW instance (connected as APPS)
@migrate_supplier.sql
```

This migrates from legacy to new:
- `AP_SUPPLIERS` — Vendor master records
- `AP_SUPPLIER_SITES_ALL` — Vendor sites (addresses, payment details)
- `AP_SUPPLIER_CONTACTS` — Contact information
- `IBY_EXT_BANK_ACCOUNTS` — Supplier bank accounts
- `IBY_PMT_INSTR_USES_ALL` — Payment instrument assignments
- `IBY_EXTERNAL_PAYEES_ALL` — External payee records

**Verify suppliers migrated:**
```sql
-- Count suppliers in new instance
SELECT COUNT(*) FROM ap_suppliers;

-- Check a specific supplier
SELECT vendor_id, vendor_name, enabled_flag
FROM ap_suppliers
WHERE vendor_id = <vendor_id>;

-- Check supplier site has payment method = CHECK
SELECT vendor_site_code, payment_method_lookup_code
FROM ap_supplier_sites_all
WHERE vendor_id = <vendor_id>;
```

> **Important:** `PAYMENT_METHOD_LOOKUP_CODE` should be `'CHECK'` on the supplier site. This is required for the AP invoice to be paid.

---

### Step 3: Create Interface Tables

Run the DDL from Section 1 of `po_to_ap_v2.sql` on the **new** instance:

```sql
-- Run on the NEW instance
-- Creates: XXCUST_PO_AP_INTERFACE_LOG (interface log/control table)
-- Creates: XXCUST_COA_MAPPING (legacy-to-new account mapping — optional)
-- Creates: XXCUST_TRANSPORT_INVOICE_STG (transport billing staging — Process C only)
```

**Verify:**
```sql
SELECT table_name FROM user_tables WHERE table_name LIKE 'XXCUST%';
-- Expected: XXCUST_PO_AP_INTERFACE_LOG, XXCUST_COA_MAPPING, XXCUST_TRANSPORT_INVOICE_STG
```

If upgrading from v1.0, run the ALTER TABLE statements instead (see comments in the SQL file).

---

### Step 4: Create COA Segment Mapping Tables

The interface maps legacy Chart of Accounts (COA) segments to new COA segments. Four mapping tables are required.

Run the segment mapping script:
```sql
-- Run on the NEW instance
@Segment_mapping.sql
```

This creates and populates:

| Table | Maps | Example |
|-------|------|---------|
| `MAPPING_DIVISION_SEGMENT` | `organization_id` → Division code | 171 → `'01'`, 169 → `'02'`, 170 → `'03'` |
| `MAPPING_PRODUCT_SEGMENT` | `inventory_item_id` → Product code | 1001 → `'MITK1000'` |
| `MAPPING_DEPARTMENT_SEGMENT` | Old department flex value → New department code | `'45001'` → `'BPR02'` |
| `MAPPING_ACCOUNT_SEGMENT` | Old account flex value → New account flex value | 500000 → 510000 |

**Verify all tables are populated:**
```sql
SELECT 'MAPPING_DIVISION_SEGMENT'   AS table_name, COUNT(*) AS rows FROM mapping_division_segment
UNION ALL
SELECT 'MAPPING_PRODUCT_SEGMENT',                   COUNT(*)        FROM mapping_product_segment
UNION ALL
SELECT 'MAPPING_DEPARTMENT_SEGMENT',                COUNT(*)        FROM mapping_department_segment
UNION ALL
SELECT 'MAPPING_ACCOUNT_SEGMENT',                   COUNT(*)        FROM mapping_account_segment;
```

> **Note:** If a product or account is missing from these tables, the interface uses fallback logic (product prefix matching → item keyword matching → default account `513001`). The invoice is still created but the CCID may need manual review.

---

### Step 5: Compile the Interface Package

Deploy the PL/SQL package on the **new** instance:

```sql
-- Run on the NEW instance (connected as APPS)
@po_to_ap_v2.sql
```

**Verify the package is valid:**
```sql
SELECT object_name, object_type, status
FROM user_objects
WHERE object_name = 'XXCUST_PO_AP_INTERFACE_PKG';
-- Expected: 2 rows (PACKAGE and PACKAGE BODY), both with STATUS = 'VALID'
```

If the package shows `INVALID`, check for missing dependencies:
```sql
SELECT * FROM user_errors WHERE name = 'XXCUST_PO_AP_INTERFACE_PKG';
```

---

### Step 6: Register the AP Invoice Source

The interface uses a custom source name `XXCUST_PO_RECEIPT` when loading invoices into the AP Open Interface. This source must be registered in Oracle AP.

1. Log in to **New EBS** as a Payables administrator
2. Navigate to **Payables Manager → Setup → Invoice → Source** (or **Lookups → Payables**)
3. Add a new source:

| Field | Value |
|-------|-------|
| Source Name | `XXCUST_PO_RECEIPT` |
| Description | PO Receipt-to-AP Interface |

**Or via SQL** (if you have access to the lookup tables):
```sql
-- Check if source already exists
SELECT lookup_code, meaning
FROM fnd_lookup_values
WHERE lookup_type = 'SOURCE'
  AND lookup_code = 'XXCUST_PO_RECEIPT';
```

> **Why this matters:** When you run AP Open Interface Import, you select `Source = XXCUST_PO_RECEIPT` to pick up only the invoices created by this interface. Without the source registered, AP Import won't find the records.

---

### Step 7: Verify AP Sequences Exist

The interface uses Oracle AP sequences to generate unique IDs:

```sql
-- These must exist in the new instance
SELECT sequence_name FROM user_sequences
WHERE sequence_name IN ('AP_INVOICES_INTERFACE_S', 'AP_INVOICE_LINES_INTERFACE_S');
-- Expected: 2 rows
```

These are standard Oracle AP sequences and should already exist. If missing, your AP module may not be fully installed.

---

### Prerequisites Checklist

| # | Item | Script/Action | Verified? |
|---|------|--------------|-----------|
| 1 | DB link `LEGACY_INSTANCE` created and working | `CREATE DATABASE LINK ...` | ☐ |
| 2 | SELECT grants on legacy tables via DB link | `GRANT SELECT ON ...` | ☐ |
| 3 | Suppliers migrated to new instance | `@migrate_supplier.sql` | ☐ |
| 4 | Supplier sites have `PAYMENT_METHOD_LOOKUP_CODE = 'CHECK'` | Verify in AP | ☐ |
| 5 | Interface tables created (`XXCUST_PO_AP_INTERFACE_LOG`, etc.) | `@po_to_ap_v2.sql` (Section 1) | ☐ |
| 6 | COA segment mapping tables populated | `@Segment_mapping.sql` | ☐ |
| 7 | `XXCUST_PO_AP_INTERFACE_PKG` compiled and VALID | `@po_to_ap_v2.sql` | ☐ |
| 8 | AP source `XXCUST_PO_RECEIPT` registered | Payables Setup → Source | ☐ |
| 9 | AP sequences exist (`AP_INVOICES_INTERFACE_S`, etc.) | `SELECT ... FROM user_sequences` | ☐ |

---

## Quick Reference

| Method | When to Use |
|--------|------------|
| **Node.js Scheduler** (recommended) | Daily automated runs, cron-scheduled |
| **SQL\*Plus / SQL Developer** | One-off manual runs, testing a single PO |
| **Concurrent Manager** | If registered as an EBS concurrent program |

---

## Method 1: Node.js Scheduler (Recommended)

This is the standard way to run the interface. A Node.js script connects to the new EBS database (optionally via SSH tunnel), runs Process A, then Process B, then purges old logs.

### 1.1 First-Time Setup

**Prerequisites:**
- Node.js 18+ installed on the server
- Oracle Instant Client installed (e.g. `/opt/oracle/instantclient_21_14`)
- Network access to the new EBS database (direct or via SSH)

**Steps:**

```bash
# 1. Go to the script directory
cd /home/sachin/dev/Ebs/script

# 2. Install dependencies
npm install

# 3. Create your configuration file
cp config.json.example config.json

# 4. Edit config.json with your database credentials
#    (see Configuration section below)
```

### 1.2 Configuration (config.json)

Open `config.json` and fill in these sections:

#### Connection Settings

Choose one of two connection modes:

**Option A — Direct connection (no SSH):**
```json
{
  "activeConnection": "directEbs",
  "connections": {
    "directEbs": {
      "ssh": { "enabled": false },
      "connection": {
        "user": "APPS",
        "password": "your_password",
        "connectString": "your-db-host:1521/SERVICE_NAME",
        "oracleClientPath": "/opt/oracle/instantclient_21_14"
      }
    }
  }
}
```

**Option B — Via SSH tunnel:**
```json
{
  "activeConnection": "sshEbs",
  "connections": {
    "sshEbs": {
      "ssh": {
        "enabled": true,
        "host": "your-server-ip",
        "port": 22,
        "username": "opc",
        "privateKeyPath": "./your-ssh-key.key"
      },
      "connection": {
        "user": "APPS",
        "password": "your_password",
        "connectString": "localhost:1521/SERVICE_NAME",
        "oracleClientPath": "/opt/oracle/instantclient_21_14"
      }
    }
  }
}
```

#### Process Settings

```json
{
  "processA": {
    "enabled": true,
    "operatingUnit": null,
    "dateFrom": null,
    "dateTo": null,
    "poNumber": null,
    "debugMode": "Y"
  },
  "processB": {
    "enabled": true,
    "operatingUnit": null,
    "dateFrom": null,
    "dateTo": null,
    "poNumber": null,
    "debugMode": "Y"
  },
  "purge": {
    "enabled": true,
    "daysToKeep": 90
  },
  "logging": {
    "logDir": "./logs",
    "retentionDays": 30
  }
}
```

| Setting | What It Means |
|---------|--------------|
| `enabled` | `true` to run this process, `false` to skip it |
| `operatingUnit` | Filter by operating unit name. `null` = process all |
| `dateFrom` / `dateTo` | Date range filter in `DD-MON-YYYY` format (e.g. `"01-JAN-2025"`). `null` = no filter |
| `poNumber` | Process a single PO only. `null` = process all eligible POs |
| `debugMode` | `"Y"` for detailed logging, `"N"` for minimal output |
| `daysToKeep` | Purge processed log records older than this many days |

### 1.3 Run Manually

```bash
cd /home/sachin/dev/Ebs/script
node run_po_ap_interface.js
```

The script will:
1. Connect to Oracle (via SSH tunnel if configured)
2. Run **Process A** — Receipt-to-Invoice
3. Run **Process B** — RTV-to-Credit-Memo (skipped if Process A fails fatally)
4. **Purge** old log records
5. Print results to console and write to `logs/po_ap_interface_YYYYMMDDHHMMSS.log`

**Exit codes:**
| Code | Meaning |
|------|---------|
| 0 | Success — all records processed |
| 1 | Completed with warnings — some records rejected (check log) |
| 2 | Fatal error — check log for details |

### 1.4 Run for a Specific PO

Edit `config.json` to set a specific PO number:

```json
{
  "processA": {
    "enabled": true,
    "poNumber": "1495474",
    "debugMode": "Y"
  },
  "processB": {
    "enabled": true,
    "poNumber": "1495474",
    "debugMode": "Y"
  }
}
```

Then run:
```bash
node run_po_ap_interface.js
```

> **Remember** to set `poNumber` back to `null` after testing.

### 1.5 Run for a Specific Date Range

```json
{
  "processA": {
    "enabled": true,
    "dateFrom": "01-APR-2026",
    "dateTo": "30-APR-2026",
    "poNumber": null
  }
}
```

### 1.6 Set Up Automated Daily Schedule (Cron)

```bash
# Open crontab editor
crontab -e

# Add this line (runs daily at 2:00 AM):
0 2 * * * cd /home/sachin/dev/Ebs/script && /usr/bin/node run_po_ap_interface.js >> /home/sachin/dev/Ebs/script/logs/cron.log 2>&1
```

To verify:
```bash
crontab -l
```

To remove:
```bash
crontab -l | grep -v po_ap_interface | crontab -
```

### 1.7 Check the Logs

Log files are saved in `script/logs/`:

```bash
# View latest log
ls -lt /home/sachin/dev/Ebs/script/logs/ | head -5

# Read a specific log
cat /home/sachin/dev/Ebs/script/logs/po_ap_interface_20260513020001.log
```

Look for these key lines in the output:

```
PROCESS A Complete - Run ID: INV-20260513020001
  Processed : 35
  Rejected  : 0
  Skipped   : 2  (net qty = 0 after RTV netting)
  Errors    : 0
Next: Run Payables Open Interface Import | Source: XXCUST_PO_RECEIPT | Group ID: 8005
```

> **Note the Group ID** — you'll need it for the AP Import step.

---

## Method 2: SQL*Plus / SQL Developer (Manual)

Use this for one-off runs or testing directly on the database.

### 2.1 Run Process A (Receipt-to-Invoice)

```sql
SET SERVEROUTPUT ON SIZE 1000000

DECLARE
    l_errbuf   VARCHAR2(2000);
    l_retcode  NUMBER;
BEGIN
    XXCUST_PO_AP_INTERFACE_PKG.run_receipt_interface(
        p_errbuf            => l_errbuf,
        p_retcode           => l_retcode,
        p_operating_unit    => NULL,            -- NULL = all OUs
        p_receipt_date_from => '01-JAN-2025',   -- adjust dates as needed
        p_receipt_date_to   => '31-DEC-2025',
        p_po_number         => NULL,            -- NULL = all POs, or specify e.g. '1495474'
        p_debug_mode        => 'Y'
    );
    DBMS_OUTPUT.PUT_LINE('Return Code: ' || l_retcode);
    DBMS_OUTPUT.PUT_LINE('Message    : ' || l_errbuf);
END;
/
```

### 2.2 Run Process B (RTV-to-Credit-Memo)

Run this **after** Process A completes.

```sql
SET SERVEROUTPUT ON SIZE 1000000

DECLARE
    l_errbuf   VARCHAR2(2000);
    l_retcode  NUMBER;
BEGIN
    XXCUST_PO_AP_INTERFACE_PKG.run_rtv_interface(
        p_errbuf         => l_errbuf,
        p_retcode        => l_retcode,
        p_operating_unit => NULL,
        p_rtv_date_from  => '01-JAN-2025',
        p_rtv_date_to    => '31-DEC-2025',
        p_po_number      => NULL,
        p_debug_mode     => 'Y'
    );
    DBMS_OUTPUT.PUT_LINE('Return Code: ' || l_retcode);
    DBMS_OUTPUT.PUT_LINE('Message    : ' || l_errbuf);
END;
/
```

### 2.3 Run for a Single PO

Change the `p_po_number` parameter:

```sql
-- Process A for PO 1495474 only
XXCUST_PO_AP_INTERFACE_PKG.run_receipt_interface(
    p_errbuf            => l_errbuf,
    p_retcode           => l_retcode,
    p_po_number         => '1495474',
    p_debug_mode        => 'Y'
);
```

### 2.4 Purge Old Log Records

```sql
BEGIN
    XXCUST_PO_AP_INTERFACE_PKG.purge_log(p_days_to_keep => 90);
END;
/
```

---

## Method 3: EBS Concurrent Manager

If the concurrent program has been registered (see `CONCURRENT_PROGRAM_SETUP.md`), submit it from the EBS UI.

### 3.1 Submit the Request

1. Log in to **New EBS** as a Payables user
2. Navigate to **Payables Manager → View → Requests → Submit a New Request**
3. Select: **PO to AP Receipt Interface**
4. Fill in parameters:

| Parameter | Value | Notes |
|-----------|-------|-------|
| Operating Unit | _(leave blank for all)_ | Or select a specific OU |
| Date From | `01-JAN-2025` | DD-MON-YYYY format |
| Date To | `31-DEC-2025` | DD-MON-YYYY format |
| PO Number | _(leave blank for all)_ | Or enter a specific PO number |
| Debug Mode | `Y` or `N` | Y = detailed log output |

5. Click **Submit**

### 3.2 Check the Output

1. Go to **View → Requests → Find**
2. Find your request by ID or name
3. Click **View Output** to see the processing log
4. Click **View Log** to see any errors

---

## After Running: AP Open Interface Import

**This is a required step.** The interface loads data into staging tables (`AP_INVOICES_INTERFACE` / `AP_INVOICE_LINES_INTERFACE`). You must run the AP Import to move them into actual AP invoices.

### Step-by-Step

1. Log in to **New EBS** as a Payables user
2. Navigate to **Payables Manager → Invoices → Import**
3. Fill in:

| Field | Value |
|-------|-------|
| Source | `XXCUST_PO_RECEIPT` |
| Group ID | _(from the interface run output — see log)_ |
| Batch Name | _(optional — leave blank)_ |
| Hold Unmatched Invoices | No |
| Create One Distribution Per Line | Yes |

4. Click **Import**
5. Wait for the concurrent request to complete

### Verify AP Import Results

```sql
-- Check if invoices were created in AP
SELECT invoice_id, invoice_num, invoice_type_lookup_code, invoice_amount,
       vendor_id, approval_status
FROM ap_invoices_all
WHERE source = 'XXCUST_PO_RECEIPT'
  AND creation_date >= TRUNC(SYSDATE);
```

If any invoices remain in the interface tables (not imported), check:
```sql
-- Rows that failed AP Import
SELECT invoice_num, status, reject_lookup_code
FROM ap_invoices_interface
WHERE source = 'XXCUST_PO_RECEIPT'
  AND status IS NOT NULL;
```

---

## After Running: Reconciliation

Run these checks to confirm everything processed correctly.

### Quick Count Check

```sql
-- How many invoices and credit memos were created in the latest run?
SELECT transaction_class, interface_status, COUNT(*)
FROM xxcust_po_ap_interface_log
WHERE run_id = (SELECT MAX(run_id) FROM xxcust_po_ap_interface_log
                WHERE transaction_class = 'INVOICE')
GROUP BY transaction_class, interface_status
ORDER BY transaction_class, interface_status;
```

### Check for Rejections

```sql
-- What was rejected and why?
SELECT legacy_po_number, invoice_num, interface_status, rejection_reason
FROM xxcust_po_ap_interface_log
WHERE interface_status IN ('REJECTED', 'ERROR')
  AND run_id LIKE 'INV-%'   -- or 'RTV-%' for Process B
ORDER BY creation_date DESC;
```

### Full Reconciliation (after AP Import)

```sql
-- Match interface log to actual AP invoices
SELECT log.invoice_num, log.invoice_amount,
       ai.invoice_id, ai.invoice_amount ap_amount,
       CASE WHEN ai.invoice_id IS NOT NULL THEN 'OK'
            ELSE 'NOT IN AP - Re-run Import' END status
FROM xxcust_po_ap_interface_log log
LEFT JOIN ap_invoices_all ai ON ai.invoice_num = log.invoice_num
WHERE log.interface_status = 'PROCESSED'
  AND log.run_id = :run_id
ORDER BY log.legacy_po_number;
```

---

## Complete Run Sequence (Cheat Sheet)

```
┌─────────────────────────────────────────────────┐
│  1. Run PO-AP Interface                         │
│     node run_po_ap_interface.js                  │
│     (or run via SQL*Plus / Concurrent Manager)   │
│                                                  │
│     → Creates invoices in AP_INVOICES_INTERFACE  │
│     → Note the Group ID from output              │
├─────────────────────────────────────────────────┤
│  2. Run AP Open Interface Import                 │
│     Payables Manager → Invoices → Import         │
│     Source: XXCUST_PO_RECEIPT                    │
│     Group ID: <from step 1>                      │
│                                                  │
│     → Moves invoices to AP_INVOICES_ALL          │
├─────────────────────────────────────────────────┤
│  3. Reconcile                                    │
│     Run the count check / reconciliation SQL     │
│     Verify all records show OK                   │
├─────────────────────────────────────────────────┤
│  4. Done!                                        │
│     Invoices are in AP and ready for payment     │
└─────────────────────────────────────────────────┘
```

---

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| `config.json not found` | Missing configuration file | Run `cp config.json.example config.json` and fill in credentials |
| `ORA-02019: connection description for remote database not found` | DB link `LEGACY_INSTANCE` not configured | Create the DB link on the new EBS instance |
| `ORA-12541: TNS:no listener` | Database not reachable | Check network, firewall, or SSH tunnel configuration |
| `Supplier ID not found in new AP` | Vendor not migrated to new instance | Set up the supplier in new EBS first, then re-run |
| `No active COA mapping for legacy CCID` | Account mapping missing | Add the mapping to `XXCUST_COA_MAPPING` or segment mapping tables |
| `already processed as invoice` | Duplicate — interface was already run for this receipt | This is expected. The duplicate check is working correctly |
| 0 records processed | No eligible receipts in the date range / PO filter | Check date range, PO number, and verify receipts exist in legacy |
| Process B: all skipped (Scenario A) | Returns happened before Process A ran | Expected — the net qty logic already handled the returns |
| AP Import has rejections | Data validation failure in AP | Check `AP_INVOICES_INTERFACE.status` and `reject_lookup_code` for details |
| Exit code 2 | Fatal error | Check the log file for the full error message and stack trace |

---

## Parameters Reference

| Parameter | Process A | Process B | Format | Default |
|-----------|-----------|-----------|--------|---------|
| Operating Unit | `p_operating_unit` | `p_operating_unit` | OU name (text) | NULL (all) |
| Date From | `p_receipt_date_from` | `p_rtv_date_from` | DD-MON-YYYY | NULL (no filter) |
| Date To | `p_receipt_date_to` | `p_rtv_date_to` | DD-MON-YYYY | NULL (no filter) |
| PO Number | `p_po_number` | `p_po_number` | PO segment1 | NULL (all) |
| Debug Mode | `p_debug_mode` | `p_debug_mode` | Y / N | N |
