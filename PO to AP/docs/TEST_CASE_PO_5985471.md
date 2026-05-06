# Test Case: PO-AP Interface — PO 5985471

**PO Number:** 5985471  
**PO Type:** BLANKET  
**Vendor ID:** 13001 | **Vendor Site ID:** 14023  
**Currency:** INR  
**Legacy PO Header ID:** 6443887  
**Test Date:** 29-Apr-2026  
**Run IDs:** INV-20260429122004 (Process A), RTV-20260429122055 (Process B)

---

## 1. Pre-Conditions

| # | Condition | Expected | Status |
|---|-----------|----------|--------|
| 1.1 | PO exists in legacy with `cancel_flag = 'N'` | Yes | |
| 1.2 | Vendor 13001 exists in new AP (`AP_SUPPLIERS`) | Yes | |
| 1.3 | PO has 5,558 total releases across 104 lines | Confirmed | |
| 1.4 | 43 releases have `quantity_billed < quantity_ordered` | Confirmed | |
| 1.5 | 35 of those 43 have at least 1 RECEIVE transaction | Confirmed | |
| 1.6 | 1 RETURN TO VENDOR exists (Line 75, qty=10, 02-Apr-26) | Confirmed | |

---

## 2. Test Case: Process A — Receipt-to-Invoice

### TC-A1: Correct number of invoices created

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Run Process A for PO 5985471 | 35 STANDARD invoices created in `AP_INVOICES_INTERFACE` | |
| 2 | Check `XXCUST_PO_AP_INTERFACE_LOG` | 35 records with `transaction_class = 'INVOICE'`, `interface_status = 'PROCESSED'` | |
| 3 | Verify 0 rejected, 0 skipped, 0 errors | Counters match | |

**Verification SQL:**
```sql
SELECT COUNT(*) FROM apps.ap_invoices_interface
WHERE attribute5 = '5985471' AND invoice_type_lookup_code = 'STANDARD';
-- Expected: 35

SELECT interface_status, COUNT(*) FROM apps.xxcust_po_ap_interface_log
WHERE legacy_po_number = '5985471' AND transaction_class = 'INVOICE'
GROUP BY interface_status;
-- Expected: PROCESSED = 35
```

### TC-A2: Fully billed releases are excluded

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Count total RECEIVE transactions for this PO in legacy | ~5,550+ | |
| 2 | Verify only 35 invoices created (unbilled releases only) | 5,515 fully billed releases excluded | |

**Rationale:** The cursor filter `nvl(pod.quantity_billed, 0) < rt.quantity` correctly excludes releases where legacy AP already billed the full quantity.

### TC-A3: RTV Net Quantity — Line 75 (KODUBALE 30 GM)

This is the key Scenario A netting test.

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Check legacy RECEIVE for `line_location_id = 14796453` | Qty = 1,500 on 01-Apr-26 | |
| 2 | Check legacy RTV for same line location | Qty = 10 on 02-Apr-26 | |
| 3 | Verify invoice uses **net qty** (1500 - 10 = 1490) | Invoice `RCPT-6797774-L075` qty = 1,490 | |
| 4 | Verify invoice base amount = 1490 × 7.11 = 10,593.90 | `ITEM` line amount = 10,593.90 | |
| 5 | Verify tax: CGST 2.5% = 266.63, SGST 2.5% = 266.63 | 2 tax lines, total = 533.26 | |
| 6 | Verify total invoice amount = 10,593.90 + 533.26 = 11,127.16 | Header `invoice_amount = 11,127.16` | |
| 7 | Invoice description contains "Net of RTV: 10" | Confirmed | |

**Verification SQL:**
```sql
SELECT invoice_num, invoice_amount FROM apps.ap_invoices_interface
WHERE invoice_num = 'RCPT-6797774-L075';
-- Expected: 11127.16

SELECT line_number, line_type_lookup_code, amount, quantity_invoiced
FROM apps.ap_invoice_lines_interface
WHERE invoice_id = (SELECT invoice_id FROM apps.ap_invoices_interface WHERE invoice_num = 'RCPT-6797774-L075');
-- Expected:
--   Line 75:   ITEM, 10593.9, qty=1490
--   Line 7501: ITEM, 266.63   (CGST-2.5)
--   Line 7502: ITEM, 266.63   (SGST-2.5)
```

### TC-A4: Standard invoice (no RTV) — Line 4 (BADAM BURFEE)

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Check invoice `RCPT-6915616-L004` | Type = STANDARD | |
| 2 | Verify amount = qty × price + tax | Base = 5,937.76, tax = 296.88, total = 6,234.64 | |
| 3 | Verify CCID = 6028 (from segment derivation) | `dist_code_combination_id = 6028` | |

**Verification SQL:**
```sql
SELECT il.dist_code_combination_id
FROM apps.ap_invoice_lines_interface il
JOIN apps.ap_invoices_interface i ON i.invoice_id = il.invoice_id
WHERE i.invoice_num = 'RCPT-6915616-L019' AND il.line_type_lookup_code = 'ITEM' AND il.line_number = 19;
-- Expected: 0 (invalid — known issue)
```

---

## 3. Test Case: Process B — RTV-to-Credit-Memo

### TC-B1: Credit Memo created for post-invoice RTV

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Run Process B for PO 5985471 after Process A | 1 Credit Memo created | |
| 2 | Check `AP_INVOICES_INTERFACE` for CREDIT type | `CM-RCPT-6797774-L075` exists | |
| 3 | Verify Credit Memo references original invoice | Description: "Original Invoice: RCPT-6797774-L075" | |
| 4 | Verify `transaction_class = 'CREDIT_MEMO'` in log | 1 record, `interface_status = 'PROCESSED'` | |

### TC-B2: Credit Memo amounts are correct and negative

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Header amount = -(10 × 7.11 + tax) = -74.66 | `invoice_amount = -74.66` | |
| 2 | ITEM line: amount = -(10 × 7.11) = -71.10 | `amount = -71.10`, `quantity_invoiced = 10` | |
| 3 | CGST tax line: -1.78 | Line 7501 amount = -1.78 | |
| 4 | SGST tax line: -1.78 | Line 7502 amount = -1.78 | |
| 5 | All lines use CCID 5028 | `dist_code_combination_id = 5028` | |

**Verification SQL:**
```sql
SELECT invoice_num, invoice_type_lookup_code, invoice_amount
FROM apps.ap_invoices_interface
WHERE invoice_num = 'CM-RCPT-6797774-L075';
-- Expected: CREDIT, -74.66

SELECT line_number, amount, quantity_invoiced, dist_code_combination_id
FROM apps.ap_invoice_lines_interface
WHERE invoice_id = (SELECT invoice_id FROM apps.ap_invoices_interface WHERE invoice_num = 'CM-RCPT-6797774-L075')
ORDER BY line_number;
-- Expected:
--   75:   -71.10, qty=10, ccid=5028
--   7501: -1.78,  null,   ccid=5028
--   7502: -1.78,  null,   ccid=5028
```

### TC-B3: No Scenario A skips (all RTVs handled)

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Check Process B skipped count | 0 skipped | |
| 2 | The single RTV on Line 75 has a matching invoice from Process A | Scenario B/C applies (not A) | |

---

## 4. Test Case: Duplicate Prevention

### TC-D1: Re-running Process A does not create duplicate invoices

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Run Process A for PO 5985471 a second time | 0 new invoices processed | |
| 2 | Log shows all 35 receipts as REJECTED/SKIPPED (duplicate) | Rejection reason: "already processed as invoice" | |

### TC-D2: Re-running Process B does not create duplicate credit memos

| Step | Action | Expected Result | Pass/Fail |
|------|--------|-----------------|-----------|
| 1 | Run Process B for PO 5985471 a second time | 0 new credit memos | |
| 2 | Log shows RTV as REJECTED (duplicate) | Rejection reason: "already processed as Credit Memo" | |

---


## 5. Summary Totals

| Metric | Expected | Actual | Match? |
|--------|----------|--------|--------|
| Process A: Invoices processed | 35 | 35 | |
| Process A: Total invoice amount | 1,608,217.37 | 1,608,217.37 | |
| Process A: Rejected | 0 | 0 | |
| Process A: Skipped | 0 | 0 | |
| Process B: Credit Memos processed | 1 | 1 | |
| Process B: Credit Memo amount | 74.66 | 74.66 | |
| Process B: Skipped (Scenario A) | 0 | 0 | |
| **Net AP Balance** | **1,608,142.71** | 1,608,217.37 - 74.66 = **1,608,142.71** | |

