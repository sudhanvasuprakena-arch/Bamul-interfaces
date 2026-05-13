# Transport (DTC) Billing - Complete Workflow

## Overview

Bamul has a **custom Transport Portal** (schema: `BMLCUSTM2`) that manages daily milk collection/distribution truck routes. Invoices are auto-generated monthly for transporters — **no PO matching or receipts** are involved in AP.

---

## Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ The web portal is the first point of entry to create the route.             | 
| STEP 1: Route Setup (Portal)                                                │
│  Table: BMLCUSTM2.TR_ROUTE                                                  │
│  → Define route, assign vehicle/driver/transporter, set base rate           │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 2: Create Blanket Purchase Agreement (BPA)                             │
│  Table: PO.PO_HEADERS_ALL (type_lookup_code = 'BLANKET')                     │
│         PO.PO_LINES_ALL (item_description='TPT_DTC', UOM='Trip')             │
│  → Links vendor (transporter) to route via PO number                         │
│  → Multiple lines for different contract periods with different rates        │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 3: Trip Auto-Scheduling                                               │
│  Table: BMLCUSTM2.TR_TRIP_HDR                                               │
│  Source: BMLCUSTM2.TR_ROUTE_SCH (schedule: MON/TUE/WED... flags)            │
│  → One row per route per day (based on schedule)                            │
│  → Initial status: calc_status='NEW', route_status='SCHEDULED'              │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 4: Dispatch Confirmation (Gate Security Portal)                        │
│  Table: BMLCUSTM2.TR_DISPSHIP_HDR                                            │
│  → Gate security confirms truck dispatched: dispatch_status = 'Y'            │
│  → Gate security confirms truck shipped: ship_status = 'Y'                   │
│  → Links to trip via: route_no + report_date                                 │
│  → Records: dispatched_by, dispatched_time, shipped_by, shipped_time         │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 5: Billing Calculation (Concurrent Program)                            │
│  Program: XXBML_TRANSPORT_BILLING                                            │
│  Package: XXBML_TPT_PROCESS_PKG.TRANSPORT_BILLING_PROCESS                    │
│                                                                              │
│  Calculation:                                                                │
│    payment_amount = base_route_rate + fuel_adjustment                        │
│    fuel_adjustment = (route_distance_km / vehicle_mileage)                   │
│                      × (current_fuel_rate - base_fuel_rate)                  │
│                                                                              │
│  Example (KDTC013):                                                          │
│    base_rate = ₹4,000 (from BPA PO line for current contract period)         │
│    distance = 120 km, mileage = 4.5 km/l → 26.67 litres                      │
│    fuel_rate = ₹90.99, base_fuel = ~₹89.01 → delta = ₹1.98/l                 │
│    fuel_adjustment = 26.67 × 1.98 = ₹52.80                                   │
│    payment_amount = ₹4,000 + ₹52.80 = ₹4,052.80                              │
│                                                                              │
│  Updates TR_TRIP_HDR: calc_status = 'NEW' → 'PROCESSED'                      │
│  Fuel rates from: BMLCUSTM2.TR_FUEL_RATE_CHART (daily diesel prices)         │
│  Vehicle info from: BMLCUSTM2.TR_VEHICLE (fuel_type, mileage)                │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 6: Invoice Creation (Same Concurrent Program)                          │
│  Table: AP.AP_INVOICES_ALL                                                   │
│         AP.AP_INVOICE_DISTRIBUTIONS_ALL                                      │
│                                                                              │
│  Grouping: All PROCESSED trips for same PO + same month                      │
│                                                                              │
│  Invoice Header:                                                             │
│    invoice_num    = 'DTC-' || sequence  (e.g. DTC-7968733)                   │
│    source         = 'TRANSPORT'                                              │
│    type           = 'STANDARD'                                               │
│    vendor         = from BPA PO header                                       │
│    amount         = SUM(payment_amount) for all trips in month               │
│    pay_group      = 'DTC_TRANSPORT'                                          │
│    terms_id       = 10003                                                    │
│    description    = 'Transport Billing created automatically'                │
│    attribute2     = reporting period (e.g. '2026-FEB 30')                    │
│    gl_date        = program run date                                         │
│                                                                              │
│  Invoice Distribution (single line):                                         │
│    account        = 10.00002.0000.821130.0.0                                 │
│    line_type      = 'ACCRUAL'                                                │
│    description    = 'TPT_DTC'                                                │
│                                                                              │
│  Updates TR_TRIP_HDR:                                                        │
│    ebs_ap_invoice_num  = invoice_num (e.g. 'DTC-7968733')                    │
│    ebs_ap_invoice_id   = numeric sequence (NOT actual invoice_id)            │
│    ap_invoice_reporting_period = '2026-FEB 30'                               │
│    calc_request_id     = concurrent request_id                               │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 7: Payment                                                             │
│  → Invoice validated & paid through standard AP payment batch                │
│  → Pay group 'DTC_TRANSPORT' used to batch transport payments together       │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Key Tables

| # | Table | Schema | Purpose |
|---|-------|--------|---------|
| 1 | `TR_ROUTE` | BMLCUSTM2 | Route master — route_number, vehicle, transporter, base payment_amount, distance, day schedule |
| 2 | `TR_ROUTE_SCH` | BMLCUSTM2 | Route schedule — which days a route runs (MON-SUN flags) |
| 3 | `TR_VEHICLE` | BMLCUSTM2 | Vehicle master — fuel_type (Diesel), mileage (km/l), capacity |
| 4 | `TR_FUEL_RATE_CHART` | BMLCUSTM2 | Daily fuel prices — effective_date, fuel_type, value (₹/litre) |
| 5 | `PO_HEADERS_ALL` | PO | Blanket PA — links route to vendor (transporter), stores PO number |
| 6 | `PO_LINES_ALL` | PO | BPA line — unit_price (base rate per trip), contract period in attribute1/attribute2 |
| 7 | `TR_TRIP_HDR` | BMLCUSTM2 | Daily trip record — payment_amount, calc_status, invoice linkage |
| 8 | `TR_DISPSHIP_HDR` | BMLCUSTM2 | Dispatch confirmation — gate security marks dispatch_status='Y', ship_status='Y' |
| 9 | `TR_TRIP_POINTS` | BMLCUSTM2 | Stop points per trip — delivery quantities at each collection/drop point |
| 10 | `AP_INVOICES_ALL` | AP | Generated invoice — source='TRANSPORT', one per PO per month |
| 11 | `AP_INVOICE_DISTRIBUTIONS_ALL` | AP | Single accrual distribution — account 821130 |

---

## Key Relationships

```
TR_ROUTE.route_number ──────────── TR_TRIP_HDR.route_number
TR_ROUTE.ebs_bpa_po_number ─────── PO_HEADERS_ALL.segment1
TR_ROUTE.vehicle_id ────────────── TR_VEHICLE.vehicle_id
TR_TRIP_HDR.route_number + 
  schedule_date ────────────────── TR_DISPSHIP_HDR.route_no + report_date
TR_TRIP_HDR.ebs_ap_invoice_num ─── AP_INVOICES_ALL.invoice_num
PO_HEADERS_ALL.vendor_id ──────── AP_INVOICES_ALL.vendor_id
```

---

## Data Volume (Legacy)

| Metric | Count |
|--------|-------|
| Active Trip POs (BPA) | 297 |
| Total Trip POs | 655 |
| TR_TRIP_HDR rows (processed) | ~295,000 |
| DTC Invoices in AP | ~12,454 |
| Dispatch confirmations | ~760,000 |

---

## Important Notes

1. **No PO matching** — AP invoices are NOT matched to PO. The BPA is only a reference for vendor/rate.
2. **No receipts** — No `rcv_transactions` or `rcv_shipment_headers` for transport POs.
3. **Monthly consolidation** — One invoice per route-PO per month regardless of trip count.
4. **Fuel is variable** — Rate changes daily based on `TR_FUEL_RATE_CHART`; each trip gets the fuel rate of its schedule_date.
5. **Gate confirmation required** — Dispatch must be confirmed (`dispatch_status='Y'`) before billing picks up the trip.
6. **BPA has multiple rate periods** — `po_lines_all.attribute1`/`attribute2` store contract start/end; `unit_price` is the base trip rate for that period.


### INSERT OF CREATING INVOICE IN OLD