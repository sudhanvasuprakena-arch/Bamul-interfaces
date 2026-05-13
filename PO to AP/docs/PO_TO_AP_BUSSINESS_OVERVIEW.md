# PO to AP Interface — Business Overview

## What Is This?

When we purchase goods from a supplier, the process flows through two separate Oracle systems:

| System | What It Does |
|--------|-------------|
| **Legacy EBS** (old system) | Where Purchase Orders are created, goods are received at the warehouse, and returns are processed |
| **New EBS** (new system) | Where supplier invoices are created and payments are made |

This interface **automatically creates invoices in the new system** based on what was physically received in the old system. No manual invoice entry is needed.

**The golden rule: No Receipt = No Invoice.** If the warehouse hasn't confirmed receiving the goods, no invoice is created and no payment goes out.

---

## The Big Picture

```
Supplier ships goods
        │
        ▼
Warehouse receives goods (Legacy System)
        │
        ▼
Interface reads the receipt  ──────────►  Invoice created (New System)
        │                                         │
        ▼                                         ▼
If goods returned to supplier  ──────►  Credit Memo created (New System)
                                                  │
                                                  ▼
                                          Net payment to supplier
```

---

## How It Works — Step by Step

### Step 1: Purchase Order (PO)

A Purchase Order is raised in the legacy system when we need to buy something — raw materials, packaging, supplies, etc. The PO tells the supplier:
- What items we want
- How many
- At what price
- Where to deliver

### Step 2: Goods Receipt

When the supplier delivers the goods, the warehouse team confirms receipt in the legacy system. This is called a **Receiving Transaction**. It records:
- What was received
- How much was received
- When it was received
- Against which PO

### Step 3: Invoice Creation (Automatic)

The interface runs automatically (typically daily) and does the following:

1. **Looks at all confirmed receipts** in the legacy system
2. **Checks if an invoice was already created** for each receipt (to avoid duplicates)
3. **Creates an invoice** in the new system for each new receipt

The invoice amount is calculated as:

> **Invoice Amount = Quantity Received × PO Unit Price + Applicable Taxes (GST)**

#### What about taxes?

The interface also picks up tax information (CGST, SGST, or IGST) from the legacy system and adds them as separate lines on the invoice. This ensures the invoice total matches what was agreed with the supplier including taxes.

### Step 4: Payment

Once the invoice is created and approved in the new system, the finance team can process the supplier payment through the normal AP payment cycle.

---

## What Happens When Goods Are Returned?

Sometimes goods need to be sent back to the supplier — they may be damaged, wrong items, poor quality, etc. This is called a **Return to Vendor (RTV)**.

The interface handles returns in two different ways depending on **when** the return happens:

### Scenario A: Return BEFORE Invoice Is Created

If goods are returned **before** the interface creates an invoice, the system simply reduces the invoice quantity.

**Example:**
- Warehouse receives 100 units on Monday
- 20 units are returned on Tuesday (defective)
- Interface runs on Wednesday
- Invoice is created for **80 units only** (100 received − 20 returned)

No separate credit memo is needed. The invoice already reflects the correct net amount.

**Special case:** If ALL goods are returned before invoicing (100 received, 100 returned), **no invoice is created at all**.

### Scenario B: Full Return AFTER Invoice Is Created

If goods are returned **after** the invoice was already created, the interface creates a **Credit Memo** to offset the original invoice.

**Example:**
- Warehouse receives 100 units → Invoice created for 100 units (₹10,000)
- Later, all 100 units returned to supplier
- Interface creates a Credit Memo for ₹10,000
- **Net payable to supplier = ₹0**

### Scenario C: Partial Return AFTER Invoice Is Created

Same as Scenario B, but only some goods are returned.

**Example:**
- Warehouse receives 100 units → Invoice created for 100 units (₹10,000)
- Later, 30 units returned to supplier
- Interface creates a Credit Memo for ₹3,000
- **Net payable to supplier = ₹7,000**

---

## Summary of Scenarios

| Scenario | When Return Happens | What the Interface Does | Result |
|----------|--------------------|-----------------------|--------|
| No return | — | Creates invoice for full received qty | Full payment to supplier |
| **A** | Before invoice | Creates invoice for net qty (received − returned) | Reduced payment |
| **A** (full return) | Before invoice, all returned | No invoice created | No payment |
| **B** | After invoice, full return | Creates Credit Memo for full amount | Cancels payment |
| **C** | After invoice, partial return | Creates Credit Memo for returned portion | Partial payment |

---

## Safety Features

| Feature | What It Prevents |
|---------|-----------------|
| **No Receipt = No Invoice** | Invoices can't be created for goods that were never received |
| **Duplicate Prevention** | Running the interface twice won't create duplicate invoices or credit memos |
| **Supplier Check** | If the supplier doesn't exist in the new system, the invoice is rejected (not silently ignored) |
| **Over-Credit Protection** | A credit memo can't exceed the original invoice amount |
| **Full Audit Trail** | Every processed, rejected, or skipped record is logged with the reason |

---

## How to Check the Results

After the interface runs, there are three simple checks:

### Check 1: Were all receipts invoiced?

The reconciliation report shows each receipt and whether an invoice was successfully created. Look for:
- **OK** — Invoice created and confirmed in AP
- **REJECTED** — Something went wrong (reason is shown)
- **SKIPPED** — Net quantity was zero (all goods returned before invoicing)

### Check 2: Were all returns credited?

A similar report shows each return and whether a credit memo was created:
- **OK** — Credit memo created
- **SKIPPED** — Return was already handled by reducing the original invoice (Scenario A)

### Check 3: Does the net balance make sense?

The master reconciliation shows, for each PO line:

| What | Meaning |
|------|---------|
| Gross Received Qty | Total quantity the warehouse received |
| Total Returned Qty | Total quantity sent back to supplier |
| Net Qty | Gross − Returns = what we actually kept |
| Invoice Amount | What we owe for the goods |
| Credit Memo Amount | What the supplier owes us back |
| Net AP Balance | Invoice − Credit Memo = actual payment due |
| Balance Status | **BALANCED** means everything adds up correctly |

---

## Frequently Asked Questions

**Q: How often does the interface run?**
A: Typically once daily. It can also be run on demand for a specific PO or date range.

**Q: What if a supplier is missing from the new system?**
A: The receipt will be flagged as REJECTED. The supplier needs to be set up in the new system first, then the interface can be re-run.

**Q: Can I run the interface for just one PO?**
A: Yes. The interface accepts a PO number filter so you can process a single PO.

**Q: What if the interface creates a wrong invoice?**
A: The invoice can be cancelled in the new AP system. The log records exactly what was created and why, making it easy to trace back.

**Q: Does this handle all PO types?**
A: Yes — Standard POs, Blanket POs (with releases), Planned POs, and Service POs are all supported.

**Q: What about taxes?**
A: GST taxes (CGST + SGST for local purchases, IGST for interstate) are automatically picked up from the legacy system and added to the invoice.

**Q: Who can I contact if something looks wrong?**
A: Check the interface log first — it usually explains why a record was rejected or skipped. For unresolved issues, contact the EBS technical team.
