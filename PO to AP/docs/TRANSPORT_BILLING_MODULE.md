# Transportation Billing Module — Technical Documentation

## 1. Overview

The Transportation Billing Module automates the calculation of transporter payments and creation of AP (Accounts Payable) invoices in **two Oracle EBS instances** simultaneously:

| Instance | Host | org_id | Role |
|---|---|---|---|
| **Legacy EBS (BMLDEV)** | 115.124.111.49:1521/BMLDEV | 148 | Trip data, billing calculation, AP invoice creation |
| **New EBS (DEV)** | 140.245.248.159:11522/DEV | 81 | Receives replicated AP invoices via DB link |

The module runs as an Oracle Concurrent Program **XXBML_TRANSPORT_BILLING** on the legacy instance, backed by the PL/SQL package `XXBML_TPT_PROCESS_PKG`.

---

## 2. DB Link Architecture

Two database links connect the instances:

| DB Link | Direction | Purpose |
|---|---|---|
| `NEW_EBS_LINK` | Legacy → New EBS | Push invoice headers/lines + trigger AP Import on new EBS |
| `LEGACY_INSTANCE` | New EBS → Legacy | Used by PO receipt interface (Process A/B); **not used** in transport billing |

The transport billing uses a **push model**: Legacy EBS calls remote procedures on New EBS via `@NEW_EBS_LINK` within the same distributed transaction.

**Key constraint**: No `COMMIT` or `ROLLBACK` in the remote procedures (causes ORA-02064). All transaction control is managed by the legacy caller.

---

## 3. Entry Point — `transport_billing_process`

This is the top-level concurrent program procedure. It accepts parameters:

| Parameter | Description |
|---|---|
| `p_from_date` / `p_to_date` | Trip date range |
| `p_reporting_month` / `p_reporting_period` | Period labels stored on invoice |
| `p_route_type` | Route type code (DTC, TCD, PTC, BMC, etc.) |
| `p_route_shift` | Optional shift filter (E/M) |
| `p_route_number` | Optional route filter |
| `p_vendor_id` | Optional vendor filter |
| `p_generate_invoice` | Y/N — whether to create AP invoices |

It dispatches to a billing sub-procedure based on `p_route_type`:

| Route Types | Billing Procedure |
|---|---|
| DTC, ADH | `billing_dtc` |
| TCD, RCD, CFR | `billing_tcd` |
| PTC, MVR, EV, OTR | `billing_ptc` |
| BMC, EMR, DCS | `billing_rate_based` |
| BMC_ITB | `billing_bmc_itb` |

---

## 4. Invoice Creation — `ap_invoices_interface_insert`

Called when `p_generate_invoice = 'Y'`. This procedure performs **three parallel operations** for each invoice:

### 4.1 Invoice Grouping

Trips are grouped into invoices by:
- **TCD/PTC route types**: `transporter_name` + `item_desc` + `item_id` + `dist_code_combination_id` + `parent_route_number`
- **DTC/other route types**: `transporter_name` + `item_desc` + `item_id` + `dist_code_combination_id` + `route_number` + `route_shift`

Invoice number format: `{route_type}-{invoice_id}` (e.g. `DTC-5467274`)

### 4.2 Legacy AP Interface Insert

Inserts into legacy `AP_INVOICES_INTERFACE` and `AP_INVOICE_LINES_INTERFACE`:

| Field | Value |
|---|---|
| `vendor_id` | Looked up from `ap_suppliers` by `transporter_name` |
| `vendor_site_id` | Matched by `PAY_GROUP_LOOKUP_CODE LIKE route_type%` |
| `source` | `TRANSPORT` |
| `org_id` | 148 |
| `terms_id` | 10003 |
| `payment_method_code` | NEFT |
| `invoice_type_lookup_code` | STANDARD |

### 4.3 New EBS Interface Insert via DB Link (Process C)

Immediately after each legacy insert, the procedure calls remote procedures on New EBS:

#### Header Push

```sql
xxcust_po_ap_interface_pkg.insert_transport_invoice@NEW_EBS_LINK(
    p_invoice_id, p_invoice_num, p_vendor_id, p_vendor_site_id,
    p_invoice_amount, p_vendor_number, p_vendor_site_code,
    ... → x_new_invoice_id, x_return_status, x_return_msg
);
```

**What `insert_transport_invoice` does on New EBS:**
1. Maps vendor by `segment1` (vendor_number) → finds `vendor_id` in new AP
2. Maps vendor site by `vendor_site_code` in `org_id = 81`
3. Generates new `invoice_id` from `ap_invoices_interface_s`
4. Inserts into `AP_INVOICES_INTERFACE` with `invoice_num = 'TRAN-' || original_num`
5. Logs to `XXCUST_PO_AP_INTERFACE_LOG` with `transaction_class = 'TRANSPORT'`
6. Returns `x_new_invoice_id` to the legacy caller

#### Line Push

```sql
-- First: query legacy segments locally (avoids DB link loop)
SELECT gcc.segment2, gcc.segment4
  INTO l_legacy_seg2, l_legacy_seg4
  FROM apps.gl_code_combinations gcc
 WHERE gcc.code_combination_id = inv_hdr_rec.dist_code_combination_id;

-- Then: push line to new EBS
xxcust_po_ap_interface_pkg.insert_transport_inv_line@NEW_EBS_LINK(
    p_new_invoice_id, p_line_number, p_amount,
    p_legacy_segment2, p_legacy_segment4,
    ... → x_return_status, x_return_msg
);
```

**What `insert_transport_inv_line` does on New EBS:**
1. Receives legacy `segment2` (costcenter) and `segment4` (account) as parameters
2. Maps account: `get_account_segment(segment4)` → new account code (fallback: `821130` for transport)
3. Maps department: `get_department_segment(segment2)` → new department code
4. Maps product: `get_product_segment(item_id)` → product code
5. Builds new 8-segment CCID: `01.01.{account}.{department}.{product}.0.000.000`
6. If CCID doesn't exist, creates it via `fnd_flex_ext.get_ccid`
7. Inserts into `AP_INVOICE_LINES_INTERFACE` (without `inventory_item_id` — legacy item doesn't exist in new EBS)

### 4.4 Submit AP Import on New EBS

After all invoices and lines are inserted:

```sql
xxcust_po_ap_interface_pkg.run_ap_import@NEW_EBS_LINK(
    p_source => 'TRANSPORT',
    x_request_id, x_return_status, x_return_msg
);
```

This submits the **APXIIMPT** concurrent program on New EBS (`org_id = 81`, source = `TRANSPORT`) which validates and creates actual `AP_INVOICES_ALL` records.

### 4.5 Submit AP Import on Legacy

Then calls `call_api` which submits APXIIMPT on the legacy instance (`org_id = 148`, source = `TRANSPORT`) via `fnd_request.submit_request`.

### 4.6 Update Trip Status

```sql
UPDATE tr_trip_hdr SET calc_status = 'PROCESSED'
 WHERE calc_request_id = p_request_id;
```

---

## 5. End-to-End Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                    LEGACY EBS (BMLDEV, org_id=148)                  │
│                                                                     │
│  Concurrent Program: XXBML_TRANSPORT_BILLING                        │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │ transport_billing_process                                     │  │
│  │   ├─ billing_dtc / billing_tcd / billing_ptc / ...            │  │
│  │   │   ├─ Mark trips (calc_request_id, ebs_ap_invoice_id=-9)   │  │
│  │   │   ├─ Calculate payment per trip                           │  │
│  │   │   └─ Update payment_amount, calc_status='CALCULATED'      │  │
│  │   │                                                           │  │
│  │   └─ ap_invoices_interface_insert                             │  │
│  │       ├─ INSERT ap_invoices_interface (legacy)                │  │
│  │       ├─ CALL insert_transport_invoice@NEW_EBS_LINK  ─────────┼──┤
│  │       ├─ INSERT ap_invoice_lines_interface (legacy)           │  │
│  │       ├─ CALL insert_transport_inv_line@NEW_EBS_LINK  ────────┼──┤
│  │       │                                                       │  │
│  │       ├─ CALL run_ap_import@NEW_EBS_LINK  ────────────────────┼──┤
│  │       ├─ call_api (submit APXIIMPT locally)                   │  │
│  │       └─ UPDATE calc_status='PROCESSED'                       │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                              │ DB Link: NEW_EBS_LINK
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     NEW EBS (DEV, org_id=81)                        │
│                                                                     │
│  Package: XXCUST_PO_AP_INTERFACE_PKG                                │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │ insert_transport_invoice                                      │  │
│  │   ├─ Map vendor_id by segment1 (vendor_number)                │  │
│  │   ├─ Map vendor_site_id by vendor_site_code in org_id=81      │  │
│  │   ├─ INSERT ap_invoices_interface (invoice_num='TRAN-...')    │  │
│  │   └─ LOG to xxcust_po_ap_interface_log                        │  │
│  │                                                               │  │
│  │ insert_transport_inv_line                                     │  │
│  │   ├─ Map account: legacy seg4 → get_account_segment           │  │
│  │   ├─ Map department: legacy seg2 → get_department_segment     │  │
│  │   ├─ Map product: item_id → get_product_segment               │  │
│  │   ├─ Derive new CCID (01.01.acct.dept.prod.0.000.000)         │  │
│  │   └─ INSERT ap_invoice_lines_interface                        │  │
│  │                                                               │  │
│  │ run_ap_import                                                 │  │
│  │   ├─ apps_initialize (SYSADMIN, Payables resp)                │  │
│  │   ├─ fnd_request.submit_request('SQLAP','APXIIMPT')           │  │
│  │   └─ fnd_concurrent.wait_for_request → creates AP invoices    │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

---
