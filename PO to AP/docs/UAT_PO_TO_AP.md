# UAT Test Plan — PO to AP Interface (v2.0)

**Module:** XXCUST_PO_AP_INTERFACE_PKG  
**Interfaces:** Process A (Receipt-to-Invoice), Process B (RTV-to-Credit-Memo), Process C (Transportation Invoice)  
**Target Instance:** New Oracle EBS R12.2 (org_id = 81)  
**Source Instance:** Legacy Oracle EBS R12.2 (via DB link `LEGACY_INSTANCE`)  
**Prepared Date:** 13-May-2026

---

## 1. Scope

| Process | Description | Invoice Type |
|---------|-------------|--------------|
| **A** | Receipt-driven AP Invoice creation with RTV netting | STANDARD |
| **B** | Post-invoice RTV → AP Credit Memo | CREDIT |
| **C** | Transportation invoice (no receipt, from staging table) | STANDARD / DEBIT |

---

## 2. Pre-Requisites Checklist

| # | Item | Verified? |
|---|------|-----------|
| 2.1 | `XXCUST_PO_AP_INTERFACE_PKG` compiled and valid in new instance | ☐ |
| 2.2 | `XXCUST_PO_AP_INTERFACE_LOG` table exists with v2.0 columns | ☐ |
| 2.3 | `XXCUST_COA_MAPPING` table exists | ☐ |
| 2.4 | `XXCUST_TRANSPORT_INVOICE_STG` table exists | ☐ |
| 2.5 | DB link `LEGACY_INSTANCE` active and SELECT grants in place | ☐ |
| 2.6 | Segment mapping tables populated (`MAPPING_DIVISION_SEGMENT`, `MAPPING_DEPARTMENT_SEGMENT`, `MAPPING_ACCOUNT_SEGMENT`, `MAPPING_PRODUCT_SEGMENT`) | ☐ |
| 2.7 | Test vendors exist in new instance `AP_SUPPLIERS` / `AP_SUPPLIER_SITES_ALL` | ☐ |
| 2.8 | `AP_INVOICES_INTERFACE_S` and `AP_INVOICE_LINES_INTERFACE_S` sequences exist | ☐ |
| 2.9 | Concurrent program `XXCUST_PO_AP_CONC_PROG` registered (if applicable) | ☐ |
| 2.10 | Payables Open Interface Import program available (Source: `XXCUST_PO_RECEIPT`) | ☐ |

---

## 3. Test Data Requirements

### 3.1 Process A & B — Receipt-Based POs

| # | Scenario | PO Type | PO Number | Requirements |
|---|----------|---------|-----------|--------------|
| TD-1 | Standard PO with receipts, no RTV | STANDARD | ___________ | At least 1 RECEIVE transaction, 0 RTVs |
| TD-2 | Standard PO with partial RTV before invoice | STANDARD | ___________ | RECEIVE + RTV on same line, net qty > 0 |
| TD-3 | Standard PO fully returned before invoice | STANDARD | ___________ | RECEIVE + RTV on same line, net qty = 0 |
| TD-4 | Blanket PO with multiple releases | BLANKET | ___________ | Multiple releases across lines |
| TD-5 | Service PO (TEMP LABOR / FIXED PRICE) | STANDARD | ___________ | `purchase_basis = 'TEMP LABOR'` or `'FIXED PRICE'` |
| TD-6 | PO with RTV after invoice already created | STANDARD | ___________ | Run Process A first, then RTV occurs in legacy |
| TD-7 | Multi-currency PO | STANDARD | ___________ | `currency_code != 'INR'` with conversion rate |
| TD-8 | PO with GST tax lines (CGST+SGST) | STANDARD | ___________ | Tax lines in `JAI_TAX_LINES_ALL` |
| TD-9 | PO with IGST tax | STANDARD | ___________ | Interstate transaction |

### 3.2 Process C — Transportation POs

| # | Scenario | Invoice Type | Requirements |
|---|----------|--------------|--------------|
| TD-10 | Standard transport invoice | STANDARD | Row in staging with `invoice_type = 'STANDARD'` |
| TD-11 | Debit memo (deductions/fines) | DEBIT | Row in staging with `invoice_type = 'DEBIT'` |
| TD-12 | Transport invoice with GST (RCM) | STANDARD | `gst_applicable_flag = 'YES'`, `gst_tax_rate = 'RCM'` |
| TD-13 | Transport invoice with TDS | STANDARD | `tds_section = '194 C -1 %'` |

---

## 4. Test Cases — Process A (Receipt-to-Invoice)

### TC-A01: Basic Receipt-to-Invoice — Standard PO (No RTV)

**Objective:** Verify a standard PO receipt creates a STANDARD AP invoice with correct amounts.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Confirm RECEIVE transaction exists in legacy for test PO | Transaction found with qty > 0 | ☐ |
| 2 | Run `run_receipt_interface` with `p_po_number = '<PO#>'`, `p_debug_mode = 'Y'` | Completes with `retcode = 0` | ☐ |
| 3 | Check `AP_INVOICES_INTERFACE` for new invoice | Invoice exists with `invoice_type_lookup_code = 'STANDARD'` | ☐ |
| 4 | Verify invoice number format | `RCPT-{receipt_num}-L{line_num}` | ☐ |
| 5 | Verify `invoice_amount` = qty × unit_price + tax | Amounts match | ☐ |
| 6 | Verify `org_id = 81` on header | Correct | ☐ |
| 7 | Verify `source = 'XXCUST_PO_RECEIPT'` | Correct | ☐ |
| 8 | Check `AP_INVOICE_LINES_INTERFACE` for ITEM line | `line_type_lookup_code = 'ITEM'`, qty and amount correct | ☐ |
| 9 | Verify `dist_code_combination_id` is derived (not -1 or 0) | Valid CCID from segment mapping | ☐ |
| 10 | Check `XXCUST_PO_AP_INTERFACE_LOG` | 1 record: `transaction_class = 'INVOICE'`, `interface_status = 'PROCESSED'` | ☐ |

**Verification SQL:**
```sql
-- Invoice header
SELECT invoice_num, invoice_type_lookup_code, invoice_amount, org_id, source, vendor_id
FROM ap_invoices_interface
WHERE invoice_num = 'RCPT-<receipt_num>-L<line_num>';

-- Invoice line
SELECT line_number, line_type_lookup_code, amount, quantity_invoiced, 
       dist_code_combination_id, description
FROM ap_invoice_lines_interface
WHERE invoice_id = (SELECT invoice_id FROM ap_invoices_interface 
                    WHERE invoice_num = 'RCPT-<receipt_num>-L<line_num>');

-- Log
SELECT interface_status, transaction_class, invoice_num, invoice_amount,
       receipt_quantity, net_quantity, rejection_reason
FROM xxcust_po_ap_interface_log
WHERE legacy_po_number = '<PO#>' AND transaction_class = 'INVOICE';
```

---

### TC-A02: RTV Netting — Partial Return Before Invoice (Scenario A)

**Objective:** Verify net quantity logic: invoice qty = gross received qty − RTV qty.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Confirm RECEIVE transaction (e.g. qty = 100) and RTV (e.g. qty = 20) exist for same PO line location in legacy | Both transactions found | ☐ |
| 2 | Run `run_receipt_interface` for the PO | Completes successfully | ☐ |
| 3 | Verify invoice quantity = 100 − 20 = 80 | `quantity_invoiced = 80` | ☐ |
| 4 | Verify invoice base amount = 80 × unit_price | Amount correct | ☐ |
| 5 | Verify invoice description contains "Net of RTV" | Description indicates netting applied | ☐ |
| 6 | Check log: `receipt_quantity`, `rtv_quantity`, `net_quantity` | 100, 20, 80 respectively | ☐ |

---

### TC-A03: Full Return Before Invoice — Net Qty = 0 (Scenario A Skip)

**Objective:** Verify no invoice is created when all goods are returned before invoicing.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Confirm RECEIVE qty = RTV qty for a PO line location | Net qty = 0 | ☐ |
| 2 | Run `run_receipt_interface` for the PO | Completes, record excluded at cursor level | ☐ |
| 3 | Verify NO invoice created for this line | No row in `AP_INVOICES_INTERFACE` | ☐ |
| 4 | Check log | Record shows `interface_status = 'SKIPPED'` or not present (cursor excluded it) | ☐ |

---

### TC-A04: Blanket PO with Multiple Releases

**Objective:** Verify blanket POs with multiple lines/releases generate correct individual invoices.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Identify blanket PO with multiple received releases | PO found | ☐ |
| 2 | Run `run_receipt_interface` for the PO | Multiple invoices created (one per receipt per line) | ☐ |
| 3 | Verify each invoice has unique invoice_num | No duplicates | ☐ |
| 4 | Verify each invoice amount matches its receipt's net qty × unit_price + tax | Amounts correct | ☐ |
| 5 | Check total invoice count matches expected receipt count | Counts match | ☐ |

---

### TC-A05: Service PO (Amount-Based)

**Objective:** Verify service POs use `amount_received` instead of qty × unit_price.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Identify a PO with `purchase_basis = 'TEMP LABOR'` or `'FIXED PRICE'` | PO found | ☐ |
| 2 | Run `run_receipt_interface` | Invoice created | ☐ |
| 3 | Verify `invoice_amount` = `pll.amount_received` (not qty × price) | Amount correct | ☐ |

---

### TC-A06: Tax Lines — CGST + SGST (Local)

**Objective:** Verify GST tax lines from legacy `JAI_TAX_LINES_ALL` are interfaced correctly.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Run Process A for a PO with CGST + SGST tax in legacy | Invoice created | ☐ |
| 2 | Check `AP_INVOICE_LINES_INTERFACE` | ITEM line + 2 tax lines (CGST, SGST) | ☐ |
| 3 | Verify tax line amounts match legacy tax amounts | Amounts match | ☐ |
| 4 | Verify invoice header amount = base + CGST + SGST | Total correct | ☐ |
| 5 | Verify `txn_type` segment = `'1'` (local) in CCID derivation | Segment correct | ☐ |

---

### TC-A07: Tax Lines — IGST (Interstate)

**Objective:** Verify IGST tax handling and correct transaction type segment.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Run Process A for a PO with IGST tax | Invoice created | ☐ |
| 2 | Verify single IGST tax line in interface | 1 tax line | ☐ |
| 3 | Verify `txn_type` segment = `'2'` (interstate) in CCID derivation | Segment correct | ☐ |

---

### TC-A08: COA Segment Mapping — All Segments Derived

**Objective:** Verify Chart of Accounts mapping produces valid CCID from all segment mappings.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Run Process A for a PO with known mapping data | Invoice created | ☐ |
| 2 | Check debug output for derived segments | All 8 segments logged | ☐ |
| 3 | Verify Entity segment = `'01'` (fixed) | Correct | ☐ |
| 4 | Verify Division segment from `MAPPING_DIVISION_SEGMENT` | Matches expected value | ☐ |
| 5 | Verify Account segment from `MAPPING_ACCOUNT_SEGMENT` | Matches expected value | ☐ |
| 6 | Verify Department segment from `MAPPING_DEPARTMENT_SEGMENT` | Matches expected value | ☐ |
| 7 | Verify Product segment from `MAPPING_PRODUCT_SEGMENT` | Matches expected value | ☐ |
| 8 | Verify Transaction Type segment from `JAI_TAX_LINES_ALL` | `0`, `1`, or `2` | ☐ |
| 9 | Verify Future segments = `'000'` (fixed) | Correct | ☐ |
| 10 | Verify `dist_code_combination_id` on invoice line is valid | CCID exists in `GL_CODE_COMBINATIONS` | ☐ |

---

### TC-A09: COA Fallback — Unmapped Account

**Objective:** Verify account segment fallback logic when old account has no direct mapping.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Identify a PO line where old account segment has no row in `MAPPING_ACCOUNT_SEGMENT` | Found | ☐ |
| 2 | Run Process A | Invoice created | ☐ |
| 3 | Check debug log for "Account fallback" messages | Fallback triggered | ☐ |
| 4 | Verify fallback resolved via product prefix or keyword match or default `513001` | Account segment populated | ☐ |

---

### TC-A10: CCID Dynamic Creation (FND_FLEX_EXT)

**Objective:** Verify new CCID is created via `FND_FLEX_EXT.GET_CCID` when segment combination doesn't exist.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Identify a PO line that produces a segment combination not yet in `GL_CODE_COMBINATIONS` | Found | ☐ |
| 2 | Run Process A | Invoice created | ☐ |
| 3 | Check debug log for "attempting dynamic creation via FND_FLEX_EXT.GET_CCID" | Message present | ☐ |
| 4 | Verify new CCID created and assigned to invoice line | Valid `dist_code_combination_id` > 0 | ☐ |

---

### TC-A11: Supplier Validation Failure

**Objective:** Verify invoice is rejected when supplier doesn't exist in new instance.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Identify a PO whose vendor_id does NOT exist in new `AP_SUPPLIERS` | Found | ☐ |
| 2 | Run Process A | Record rejected | ☐ |
| 3 | Check log | `interface_status = 'REJECTED'`, reason: "Supplier ID not found in new AP" | ☐ |
| 4 | Verify no invoice created for this PO | No row in `AP_INVOICES_INTERFACE` | ☐ |

---

### TC-A12: Operating Unit Filter

**Objective:** Verify `p_operating_unit` parameter filters correctly.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Run with `p_operating_unit = '<valid OU name>'` | Only receipts from that OU processed | ☐ |
| 2 | Run with `p_operating_unit = '<non-existent OU>'` | 0 records processed | ☐ |

---

### TC-A13: Date Range Filter

**Objective:** Verify `p_receipt_date_from` and `p_receipt_date_to` filters.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Run with a narrow date range containing known receipts | Only receipts within range processed | ☐ |
| 2 | Run with a date range containing no receipts | 0 records processed | ☐ |

---

## 5. Test Cases — Process B (RTV-to-Credit-Memo)

### TC-B01: Credit Memo for Post-Invoice RTV (Scenario B — Full Return)

**Objective:** Verify a Credit Memo is created when all goods are returned after invoice was created.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Run Process A for test PO — invoice created | Invoice exists in log with `PROCESSED` | ☐ |
| 2 | Confirm RTV transaction exists in legacy for same PO line (after receipt date) | RTV found | ☐ |
| 3 | Run `run_rtv_interface` for the PO | Credit Memo created | ☐ |
| 4 | Check `AP_INVOICES_INTERFACE` | `invoice_type_lookup_code = 'CREDIT'` | ☐ |
| 5 | Verify credit memo number = `CM-{original_invoice_num}` | Format correct | ☐ |
| 6 | Verify header `invoice_amount` is **negative** | Negative value | ☐ |
| 7 | Verify ITEM line amount is **negative** (−qty × unit_price) | Correct | ☐ |
| 8 | Verify tax lines are present (if applicable) | Tax amounts negative | ☐ |
| 9 | Check log | `transaction_class = 'CREDIT_MEMO'`, `interface_status = 'PROCESSED'` | ☐ |

---

### TC-B02: Partial RTV Credit Memo (Scenario C)

**Objective:** Verify partial return generates a correctly sized Credit Memo.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Process A created invoice for receipt qty = 100 | Invoice exists | ☐ |
| 2 | Legacy RTV exists for qty = 30 (partial) | RTV found | ☐ |
| 3 | Run Process B | Credit Memo created for qty 30 only | ☐ |
| 4 | Verify credit memo amount = −(30 × unit_price + tax) | Amount correct | ☐ |
| 5 | Verify original invoice is NOT modified | Original invoice amount unchanged | ☐ |

---

### TC-B03: Scenario A Skip — RTV with No Prior Invoice

**Objective:** Verify RTV is skipped when no AP Invoice was previously created for the PO line.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Identify PO line with RTV but Process A was never run (or net qty was 0) | Found | ☐ |
| 2 | Run Process B | Record skipped | ☐ |
| 3 | Check log | `interface_status = 'SKIPPED'`, reason: "No processed AP Invoice found…Scenario A" | ☐ |
| 4 | Verify no credit memo created | No row in `AP_INVOICES_INTERFACE` | ☐ |

---

### TC-B04: Over-Credit Prevention

**Objective:** Verify credit memo is rejected when RTV amount exceeds original invoice amount.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Identify scenario where RTV amount > original invoice amount | Found (or simulate) | ☐ |
| 2 | Run Process B | Record rejected | ☐ |
| 3 | Check log | `interface_status = 'REJECTED'`, reason: "RTV amount exceeds original invoice amount" | ☐ |

---

### TC-B05: Credit Memo CCID Derivation

**Objective:** Verify Credit Memo uses correctly derived CCID (same mapping logic as Process A).

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Run Process B for a valid RTV | Credit Memo created | ☐ |
| 2 | Verify `dist_code_combination_id` on credit memo line | Valid CCID, derived from same segment mapping | ☐ |

---

## 6. Test Cases — Process C (Transportation Invoice)

### TC-C01: Standard Transport Invoice

**Objective:** Verify staging table row with `invoice_type = 'STANDARD'` creates a STANDARD AP invoice.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Insert test row into `XXCUST_TRANSPORT_INVOICE_STG` with `process_status = 'NEW'`, `invoice_type = 'STANDARD'` | Row inserted | ☐ |
| 2 | Run `run_transport_interface` with `p_batch_id` or NULL | Completes successfully | ☐ |
| 3 | Check `AP_INVOICES_INTERFACE` | STANDARD invoice created with correct amount | ☐ |
| 4 | Verify vendor resolved from `vendor_code` | Correct `vendor_id` | ☐ |
| 5 | Verify vendor site resolved from `vendor_site_code` | Correct `vendor_site_id` | ☐ |
| 6 | Verify `invoice_date` = `invoice_date_to` from staging | Date correct | ☐ |
| 7 | Check staging table | `process_status = 'PROCESSED'`, `run_id` populated | ☐ |

---

### TC-C02: Debit Memo (Deduction/Fine)

**Objective:** Verify staging row with `invoice_type = 'DEBIT'` creates a DEBIT memo.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Insert test row with `invoice_type = 'DEBIT'` | Row inserted | ☐ |
| 2 | Run `run_transport_interface` | Completes successfully | ☐ |
| 3 | Check `AP_INVOICES_INTERFACE` | `invoice_type_lookup_code = 'DEBIT'` | ☐ |
| 4 | Verify amount is positive (AP handles sign for DEBIT) | Amount > 0 | ☐ |

---

### TC-C03: Transport Invoice Vendor Validation

**Objective:** Verify rejection when vendor_code or vendor_site_code is invalid.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Insert staging row with non-existent `vendor_code` | Row inserted | ☐ |
| 2 | Run Process C | Record rejected | ☐ |
| 3 | Check staging table | `process_status = 'REJECTED'`, `process_message` describes error | ☐ |

---

### TC-C04: Batch ID Filter

**Objective:** Verify `p_batch_id` parameter processes only matching rows.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Insert rows with `batch_id = 'BATCH-001'` and `batch_id = 'BATCH-002'` | Rows inserted | ☐ |
| 2 | Run with `p_batch_id = 'BATCH-001'` | Only BATCH-001 rows processed | ☐ |
| 3 | Verify BATCH-002 rows remain `process_status = 'NEW'` | Unprocessed | ☐ |

---

## 7. Test Cases — Cross-Process & Integration

### TC-X01: Duplicate Prevention — Process A Re-Run

**Objective:** Verify re-running Process A does not create duplicate invoices.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Run Process A for a PO — invoices created | N invoices processed | ☐ |
| 2 | Run Process A again for same PO | 0 new invoices | ☐ |
| 3 | Check log | All records show "already processed as invoice" | ☐ |

---

### TC-X02: Duplicate Prevention — Process B Re-Run

**Objective:** Verify re-running Process B does not create duplicate credit memos.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Run Process B — credit memos created | N records processed | ☐ |
| 2 | Run Process B again | 0 new credit memos | ☐ |
| 3 | Check log | All records show "already processed as Credit Memo" | ☐ |

---

### TC-X03: Process A then B Sequential Run

**Objective:** Verify Process B correctly references invoices created by Process A.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Run Process A for PO with a post-receipt RTV | Invoices created | ☐ |
| 2 | Run Process B for same PO | Credit Memo references the Process A invoice | ☐ |
| 3 | Verify credit memo `invoice_num` = `CM-{Process A invoice_num}` | Correct reference | ☐ |

---

### TC-X04: AP Open Interface Import — Invoices

**Objective:** Verify AP Import successfully picks up interfaced invoices.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | After Process A, run Payables Open Interface Import (Source: `XXCUST_PO_RECEIPT`, Group ID from log) | Import completes | ☐ |
| 2 | Check `AP_INVOICES_ALL` for created invoices | Invoices exist with correct amounts | ☐ |
| 3 | Verify invoice approval status | As expected per AP setup | ☐ |
| 4 | Run Reconciliation Query 4A with Run ID | All records show `OK` | ☐ |

---

### TC-X05: AP Open Interface Import — Credit Memos

**Objective:** Verify AP Import successfully picks up interfaced credit memos.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | After Process B, run Payables Open Interface Import | Import completes | ☐ |
| 2 | Check `AP_INVOICES_ALL` for CREDIT type invoices | Credit Memos exist | ☐ |
| 3 | Run Reconciliation Query 4B with Run ID | All records show `OK` | ☐ |

---

### TC-X06: Net AP Balance Reconciliation

**Objective:** Confirm net AP exposure = (gross received − RTV) × unit_price for each PO line.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Run Reconciliation Query 4C (master net balance) | Query returns results | ☐ |
| 2 | Verify all PO lines show `balance_status = 'BALANCED'` | No variances | ☐ |
| 3 | If any `VARIANCE - REVIEW`, investigate and document | Explained or defect raised | ☐ |

---

### TC-X07: Purge Log

**Objective:** Verify `purge_log` removes only old PROCESSED records.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Ensure log has PROCESSED records older than 90 days (or use `p_days_to_keep = 0` for test) | Records exist | ☐ |
| 2 | Run `XXCUST_PO_AP_INTERFACE_PKG.purge_log(p_days_to_keep => 0)` | Delete count logged | ☐ |
| 3 | Verify REJECTED/ERROR records are NOT deleted | Still present | ☐ |

---

## 8. Test Cases — Error Handling & Edge Cases

### TC-E01: DB Link Down

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Simulate DB link failure (invalid credentials or network) | Process fails gracefully | ☐ |
| 2 | Verify `retcode = 2` and `errbuf` contains meaningful error | Fatal error captured | ☐ |
| 3 | Verify ROLLBACK executed (no partial data in interface tables) | No orphaned records | ☐ |

---

### TC-E02: Invalid Legacy CCID

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Identify receipt where `pod.code_combination_id` doesn't exist in `GL_CODE_COMBINATIONS` | Found | ☐ |
| 2 | Run Process A | Invoice created with fallback CCID `00000` | ☐ |
| 3 | Verify debug log shows CCID derivation failure | `-1` returned, then fallback applied | ☐ |

---

### TC-E03: Large Volume Run

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Run Process A with no PO filter, broad date range | Processes all eligible receipts | ☐ |
| 2 | Verify no ORA errors (memory, temp space, etc.) | Completes without fatal error | ☐ |
| 3 | Note execution time for baseline | Time: _________ | ☐ |
| 4 | Verify COMMIT at end (not per record) | Single commit | ☐ |

---

## 9. Reconciliation Queries

Run these after AP Open Interface Import to validate end-to-end:

### 9.1 Query 4A — Receipt-to-Invoice Reconciliation
```sql
SELECT log.run_id, log.legacy_po_number, log.vendor_name, log.receipt_number,
       log.receipt_quantity, log.rtv_quantity, log.net_quantity,
       log.invoice_num, log.invoice_amount,
       ai.invoice_id, ai.invoice_amount ap_amount, ai.approval_status,
       CASE
           WHEN log.interface_status = 'PROCESSED' AND ai.invoice_id IS NULL THEN 'WARNING: Re-run AP Import'
           WHEN log.interface_status = 'REJECTED' THEN 'REJECTED: ' || log.rejection_reason
           WHEN log.interface_status = 'PROCESSED' AND ai.invoice_id IS NOT NULL THEN 'OK'
           ELSE log.interface_status
       END reconciliation_status
FROM xxcust_po_ap_interface_log log
LEFT JOIN ap_invoices_all ai ON ai.invoice_num = log.invoice_num AND ai.vendor_id = log.legacy_vendor_id
WHERE log.run_id = :p_run_id AND log.transaction_class = 'INVOICE'
ORDER BY log.interface_status, log.legacy_po_number;
```

### 9.2 Query 4B — Credit Memo Reconciliation
```sql
SELECT log.run_id, log.legacy_po_number, log.vendor_name,
       log.rtv_quantity, log.invoice_num credit_memo_num, log.invoice_amount,
       ai.invoice_id, ai.invoice_amount ap_amount,
       CASE
           WHEN log.interface_status = 'PROCESSED' AND ai.invoice_id IS NULL THEN 'WARNING: Re-run AP Import'
           WHEN log.interface_status = 'PROCESSED' AND ai.invoice_id IS NOT NULL THEN 'OK'
           ELSE log.interface_status || ': ' || log.rejection_reason
       END reconciliation_status
FROM xxcust_po_ap_interface_log log
LEFT JOIN ap_invoices_all ai ON ai.invoice_num = log.invoice_num
     AND ai.vendor_id = log.legacy_vendor_id AND ai.invoice_type_lookup_code = 'CREDIT'
WHERE log.run_id = :p_run_id AND log.transaction_class = 'CREDIT_MEMO'
ORDER BY log.interface_status;
```

### 9.3 Query 4C — Net AP Balance Check
```sql
SELECT inv.legacy_po_number, inv.legacy_po_line_num, inv.vendor_name,
       inv.receipt_quantity gross_qty, NVL(rtv.rtv_quantity, 0) rtv_qty,
       inv.invoice_amount, NVL(rtv.invoice_amount, 0) credit_amount,
       (inv.invoice_amount - NVL(rtv.invoice_amount, 0)) net_ap_balance,
       CASE WHEN ABS((inv.invoice_amount - NVL(rtv.invoice_amount, 0))
            - ((inv.receipt_quantity - NVL(rtv.rtv_quantity, 0))
               * inv.invoice_amount / NULLIF(inv.receipt_quantity, 0))) < 0.01
            THEN 'BALANCED' ELSE 'VARIANCE' END balance_status
FROM (SELECT legacy_po_number, legacy_po_line_num, vendor_name,
             SUM(receipt_quantity) receipt_quantity, SUM(invoice_amount) invoice_amount
      FROM xxcust_po_ap_interface_log
      WHERE transaction_class = 'INVOICE' AND interface_status = 'PROCESSED'
      GROUP BY legacy_po_number, legacy_po_line_num, vendor_name) inv
LEFT JOIN (SELECT legacy_po_number, legacy_po_line_num,
                  SUM(rtv_quantity) rtv_quantity, SUM(invoice_amount) invoice_amount
           FROM xxcust_po_ap_interface_log
           WHERE transaction_class = 'CREDIT_MEMO' AND interface_status = 'PROCESSED'
           GROUP BY legacy_po_number, legacy_po_line_num) rtv
ON rtv.legacy_po_number = inv.legacy_po_number AND rtv.legacy_po_line_num = inv.legacy_po_line_num
ORDER BY balance_status DESC, inv.legacy_po_number;
```

---

## 10. UAT Sign-Off

| Process | Tester | Test Date | Status | Comments |
|---------|--------|-----------|--------|----------|
| Process A — Receipt-to-Invoice | | | ☐ Pass / ☐ Fail | |
| Process B — RTV-to-Credit-Memo | | | ☐ Pass / ☐ Fail | |
| Process C — Transportation Invoice | | | ☐ Pass / ☐ Fail | |
| AP Open Interface Import | | | ☐ Pass / ☐ Fail | |
| Reconciliation Queries | | | ☐ Pass / ☐ Fail | |
| Duplicate Prevention | | | ☐ Pass / ☐ Fail | |
| Error Handling | | | ☐ Pass / ☐ Fail | |

**UAT Approved By:** ___________________________  
**Date:** ___________________________  
**Notes:** ___________________________
