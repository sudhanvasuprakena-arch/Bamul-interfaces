/* ============================================================================
   INTERFACE:    Legacy Oracle PO Receipt --> New Oracle AP Invoice Interface
                 + RTV --> New Oracle AP Credit Memo Interface
   VERSION:      2.0
   PLATFORM:     Oracle EBS R12.2 (Legacy) --> Oracle EBS R12.2 (New Instance)
   DESCRIPTION:  Receipt-Driven AP Invoice Interface. Only PO receipts that
                 have been confirmed in the legacy Receiving module will trigger
                 the creation of AP Invoices in the new Oracle AP instance.
                 Covers all PO types: Standard, Blanket, Planned, and Services.

                 v2.0 Enhancements:
                 (1) Net quantity logic: RTV quantities are netted against RECEIVE
                     quantities before invoicing. If net qty = 0, no invoice created.
                 (2) RTV-to-Credit-Memo interface: RETURN TO VENDOR transactions
                     that occur AFTER an AP Invoice has been created generate a
                     Credit Memo (CREDIT type) in the new AP instance.

   BUSINESS RULES:
     - NO RECEIPT = NO AP INVOICE
     - NET RECEIVED QUANTITY drives invoice amounts (gross receipt minus RTV)
     - POST-INVOICE RTV = CREDIT MEMO in new AP
     - NO DUPLICATE PROCESSING of any receipt or RTV transaction

   AUTHOR:       Interface Design
   DATE:         2025
   ============================================================================

   INTERFACE FLOW (v2.0):
   -----------------------
   PROCESS A - Receipt-to-Invoice (enhanced with RTV netting):
   1. EXTRACT   - Query RECEIVE transactions; net off any RTV qty per PO line location
   2. VALIDATE  - Apply business rules and data quality checks
   3. TRANSFORM - Map net receipt data to AP_INVOICES_INTERFACE columns
   4. LOAD      - Insert into AP_INVOICES_INTERFACE / AP_INVOICE_LINES_INTERFACE
   5. LOG       - Record all processed/rejected records in XXCUST_PO_AP_INTERFACE_LOG

   PROCESS B - RTV-to-Credit-Memo (new in v2.0):
   1. EXTRACT   - Query RETURN TO VENDOR transactions from legacy RCV_TRANSACTIONS
   2. VALIDATE  - Check original receipt was interfaced; apply RTV business rules
   3. TRANSFORM - Map RTV data to AP_INVOICES_INTERFACE as CREDIT type invoice
   4. LOAD      - Insert into AP_INVOICES_INTERFACE / AP_INVOICE_LINES_INTERFACE
   5. LOG       - Record all Credit Memos in XXCUST_PO_AP_INTERFACE_LOG

   TABLES REFERENCED (LEGACY SOURCE):
   ------------------------------------
   RCV_TRANSACTIONS          - Receiving transaction details (RECEIVE + RETURN TO VENDOR)
   RCV_SHIPMENT_HEADERS      - Receipt header information
   RCV_SHIPMENT_LINES        - Receipt line details
   PO_HEADERS_ALL            - Purchase Order headers (all types)
   PO_LINES_ALL              - Purchase Order lines
   PO_LINE_LOCATIONS_ALL     - PO shipment schedules
   PO_DISTRIBUTIONS_ALL      - PO distributions / account coding
   AP_SUPPLIERS              - Supplier master
   AP_SUPPLIER_SITES_ALL     - Supplier site details
   HR_OPERATING_UNITS        - Operating unit reference

   TABLES REFERENCED (NEW TARGET):
   ---------------------------------
   AP_INVOICES_INTERFACE         - AP Invoice / Credit Memo header open interface
   AP_INVOICE_LINES_INTERFACE    - AP Invoice / Credit Memo lines open interface
   XXCUST_PO_AP_INTERFACE_LOG    - Custom interface control/log table
   XXCUST_COA_MAPPING            - Custom COA cross-reference mapping table

   CHANGE LOG:
   -----------
   v1.0  Initial release - Receipt-to-Invoice interface
   v2.0  Added RTV net quantity logic; added RTV-to-Credit-Memo interface
   ============================================================================ */


/* ============================================================================
   SECTION 1: DDL - Create / Alter supporting objects in NEW instance
   ============================================================================ */

-- Custom Interface Control and Log Table
-- (Run CREATE only on first deployment; ALTER statements add v2.0 columns)
CREATE TABLE XXCUST_PO_AP_INTERFACE_LOG (
    log_id                      NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id                      VARCHAR2(50)    NOT NULL,
    run_date                    DATE            DEFAULT SYSDATE,
    transaction_class           VARCHAR2(20),   -- INVOICE or CREDIT_MEMO (v2.0)
    legacy_rcv_transaction_id   NUMBER,         -- RECEIVE or RETURN TO VENDOR transaction_id
    legacy_orig_rcv_txn_id      NUMBER,         -- for RTVs: the original RECEIVE transaction_id
    legacy_po_header_id         NUMBER,
    legacy_po_number            VARCHAR2(20),
    legacy_po_line_num          NUMBER,
    legacy_vendor_id            NUMBER,
    vendor_name                 VARCHAR2(240),
    receipt_number              VARCHAR2(30),
    receipt_date                DATE,
    receipt_quantity            NUMBER,         -- gross received qty (RECEIVE transactions)
    rtv_quantity                NUMBER,         -- returned qty (RETURN TO VENDOR transactions)
    net_quantity                NUMBER,         -- receipt_quantity - rtv_quantity
    receipt_amount              NUMBER,         -- net invoiceable amount
    invoice_num                 VARCHAR2(50),
    invoice_amount              NUMBER,
    interface_status            VARCHAR2(20),   -- PROCESSED, REJECTED, SKIPPED, ERROR
    rejection_reason            VARCHAR2(2000),
    ap_invoice_id               NUMBER,         -- populated after AP Import run
    created_by                  NUMBER          DEFAULT -1,
    creation_date               DATE            DEFAULT SYSDATE,
    last_updated_by             NUMBER          DEFAULT -1,
    last_update_date            DATE            DEFAULT SYSDATE
);

CREATE INDEX XXCUST_PO_AP_IFACE_LOG_N1 ON XXCUST_PO_AP_INTERFACE_LOG (run_id);
CREATE INDEX XXCUST_PO_AP_IFACE_LOG_N2 ON XXCUST_PO_AP_INTERFACE_LOG (legacy_rcv_transaction_id);
CREATE INDEX XXCUST_PO_AP_IFACE_LOG_N3 ON XXCUST_PO_AP_INTERFACE_LOG (interface_status);
CREATE INDEX XXCUST_PO_AP_IFACE_LOG_N4 ON XXCUST_PO_AP_INTERFACE_LOG (transaction_class);
CREATE INDEX XXCUST_PO_AP_IFACE_LOG_N5 ON XXCUST_PO_AP_INTERFACE_LOG (legacy_orig_rcv_txn_id);

-- NOTE: If upgrading from v1.0, run these ALTER statements instead of the CREATE above:
/*
ALTER TABLE XXCUST_PO_AP_INTERFACE_LOG ADD (
    transaction_class       VARCHAR2(20),
    legacy_orig_rcv_txn_id  NUMBER,
    rtv_quantity            NUMBER,
    net_quantity            NUMBER
);
CREATE INDEX XXCUST_PO_AP_IFACE_LOG_N4 ON XXCUST_PO_AP_INTERFACE_LOG (transaction_class);
CREATE INDEX XXCUST_PO_AP_IFACE_LOG_N5 ON XXCUST_PO_AP_INTERFACE_LOG (legacy_orig_rcv_txn_id);
*/

-- Custom COA Mapping Table (unchanged from v1.0)
CREATE TABLE XXCUST_COA_MAPPING (
    mapping_id              NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    legacy_ccid             NUMBER          NOT NULL,
    legacy_account_num      VARCHAR2(240),
    new_account_segment1    VARCHAR2(25),
    new_account_segment2    VARCHAR2(25),
    new_account_segment3    VARCHAR2(25),
    new_account_segment4    VARCHAR2(25),
    new_account_segment5    VARCHAR2(25),
    new_ccid                NUMBER,
    mapping_status          VARCHAR2(20)    DEFAULT 'ACTIVE',
    notes                   VARCHAR2(500),
    created_by              NUMBER          DEFAULT -1,
    creation_date           DATE            DEFAULT SYSDATE
);

CREATE UNIQUE INDEX XXCUST_COA_MAPPING_U1 ON XXCUST_COA_MAPPING (legacy_ccid);


/* ============================================================================
   SECTION 2: MAIN INTERFACE PACKAGE SPECIFICATION
   ============================================================================ */

CREATE OR REPLACE PACKAGE XXCUST_PO_AP_INTERFACE_PKG AS

    -- -------------------------------------------------------------------------
    -- PROCESS A: Receipt-to-Invoice (with RTV netting)
    -- Entry point for standard receipt-driven AP Invoice creation.
    -- Net quantity logic is applied internally: gross received qty minus any
    -- RETURN TO VENDOR qty on the same PO line location is computed before
    -- deciding whether to create an AP Invoice and for what amount.
    -- -------------------------------------------------------------------------
    PROCEDURE run_receipt_interface (
        p_errbuf            OUT     VARCHAR2,
        p_retcode           OUT     NUMBER,
        p_operating_unit    IN      VARCHAR2    DEFAULT NULL,   -- NULL = all OUs
        p_receipt_date_from IN      VARCHAR2    DEFAULT NULL,   -- DD-MON-YYYY
        p_receipt_date_to   IN      VARCHAR2    DEFAULT NULL,   -- DD-MON-YYYY
        p_po_number         IN      VARCHAR2    DEFAULT NULL,   -- NULL = all POs
        p_debug_mode        IN      VARCHAR2    DEFAULT 'N'
    );

    -- -------------------------------------------------------------------------
    -- PROCESS B: RTV-to-Credit-Memo
    -- Entry point for generating AP Credit Memos from RETURN TO VENDOR
    -- transactions that occurred AFTER the original AP Invoice was created.
    -- RTVs with no matching processed invoice in the log are skipped (they
    -- are already handled by net quantity logic in Process A).
    -- -------------------------------------------------------------------------
    PROCEDURE run_rtv_interface (
        p_errbuf            OUT     VARCHAR2,
        p_retcode           OUT     NUMBER,
        p_operating_unit    IN      VARCHAR2    DEFAULT NULL,   -- NULL = all OUs
        p_rtv_date_from     IN      VARCHAR2    DEFAULT NULL,   -- DD-MON-YYYY
        p_rtv_date_to       IN      VARCHAR2    DEFAULT NULL,   -- DD-MON-YYYY
        p_po_number         IN      VARCHAR2    DEFAULT NULL,   -- NULL = all POs
        p_debug_mode        IN      VARCHAR2    DEFAULT 'N'
    );

    -- Purge successfully processed log records older than N days
    PROCEDURE purge_log (
        p_days_to_keep      IN      NUMBER      DEFAULT 90
    );

END XXCUST_PO_AP_INTERFACE_PKG;
/


/* ============================================================================
   SECTION 3: MAIN INTERFACE PACKAGE BODY
   ============================================================================ */

CREATE OR REPLACE PACKAGE BODY XXCUST_PO_AP_INTERFACE_PKG AS

    -- =========================================================================
    -- Package-level constants
    -- =========================================================================
    c_source_name       CONSTANT VARCHAR2(30)   := 'XXCUST_PO_RECEIPT';
    c_created_by        CONSTANT NUMBER         := -1;         -- replace with FND_GLOBAL.USER_ID
    c_invoice_type      CONSTANT VARCHAR2(25)   := 'STANDARD';
    c_credit_type       CONSTANT VARCHAR2(25)   := 'CREDIT';
    c_pay_group         CONSTANT VARCHAR2(25)   := 'STANDARD';
    -- DB link name pointing to legacy Oracle instance:
    c_db_link           CONSTANT VARCHAR2(30)   := 'LEGACY_INSTANCE';

    -- Transaction class constants (for log table)
    c_class_invoice     CONSTANT VARCHAR2(20)   := 'INVOICE';
    c_class_credit_memo CONSTANT VARCHAR2(20)   := 'CREDIT_MEMO';

    -- Status constants
    c_status_processed  CONSTANT VARCHAR2(20)   := 'PROCESSED';
    c_status_rejected   CONSTANT VARCHAR2(20)   := 'REJECTED';
    c_status_skipped    CONSTANT VARCHAR2(20)   := 'SKIPPED';
    c_status_error      CONSTANT VARCHAR2(20)   := 'ERROR';


    -- =========================================================================
    -- Private: Write message to output log
    -- =========================================================================
    PROCEDURE log_message (p_message IN VARCHAR2) IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS') || ' | ' || p_message);
        -- In full implementation: FND_FILE.PUT_LINE(FND_FILE.LOG, p_message);
    END log_message;


    -- =========================================================================
    -- Private: Lookup new COA CCID from legacy CCID. Returns -1 if not found.
    -- =========================================================================
    FUNCTION get_new_ccid (p_legacy_ccid IN NUMBER) RETURN NUMBER IS
        l_new_ccid  NUMBER;
    BEGIN
        SELECT new_ccid
        INTO   l_new_ccid
        FROM   XXCUST_COA_MAPPING
        WHERE  legacy_ccid    = p_legacy_ccid
        AND    mapping_status = 'ACTIVE';
        RETURN NVL(l_new_ccid, -1);
    EXCEPTION
        WHEN OTHERS THEN RETURN -1;
    END get_new_ccid;


    -- =========================================================================
    -- Private: Get net received quantity for a PO line location.
    -- Sums all RECEIVE transactions and subtracts all RETURN TO VENDOR
    -- transactions for the same po_line_location_id in the legacy instance.
    -- This is the core RTV Scenario A netting function.
    -- =========================================================================
    FUNCTION get_net_received_qty (
        p_po_line_location_id   IN  NUMBER
    ) RETURN NUMBER IS
        l_net_qty   NUMBER;
    BEGIN
        SELECT NVL(SUM(
                    CASE
                        WHEN rt.transaction_type = 'RECEIVE'          THEN  rt.quantity
                        WHEN rt.transaction_type = 'RETURN TO VENDOR' THEN -rt.quantity
                        ELSE 0
                    END
               ), 0)
        INTO   l_net_qty
        FROM   rcv_transactions@LEGACY_INSTANCE rt
        WHERE  rt.po_line_location_id   = p_po_line_location_id
        AND    rt.transaction_type      IN ('RECEIVE', 'RETURN TO VENDOR')
        AND    rt.source_document_code  = 'PO';

        RETURN l_net_qty;
    EXCEPTION
        WHEN OTHERS THEN RETURN 0;
    END get_net_received_qty;


    -- =========================================================================
    -- Private: Get total RTV quantity for a PO line location.
    -- Used by RTV interface to compute Credit Memo amounts.
    -- =========================================================================
    FUNCTION get_rtv_qty_for_line (
        p_po_line_location_id   IN  NUMBER
    ) RETURN NUMBER IS
        l_rtv_qty   NUMBER;
    BEGIN
        SELECT NVL(SUM(rt.quantity), 0)
        INTO   l_rtv_qty
        FROM   rcv_transactions@LEGACY_INSTANCE rt
        WHERE  rt.po_line_location_id   = p_po_line_location_id
        AND    rt.transaction_type      = 'RETURN TO VENDOR'
        AND    rt.source_document_code  = 'PO';

        RETURN l_rtv_qty;
    EXCEPTION
        WHEN OTHERS THEN RETURN 0;
    END get_rtv_qty_for_line;


    -- =========================================================================
    -- Private: Check whether the original RECEIVE transaction for a given
    -- PO line location has already been processed as an AP Invoice.
    -- Returns the original invoice_num if found, NULL otherwise.
    -- Used by RTV interface to determine Scenario A vs Scenario B/C.
    -- =========================================================================
    FUNCTION get_original_invoice_num (
        p_po_line_location_id   IN  NUMBER,
        p_po_header_id          IN  NUMBER
    ) RETURN VARCHAR2 IS
        l_invoice_num   VARCHAR2(50);
    BEGIN
        -- Find the most recently processed invoice for this PO line location
        SELECT invoice_num
        INTO   l_invoice_num
        FROM (
            SELECT log.invoice_num
            FROM   XXCUST_PO_AP_INTERFACE_LOG   log
            JOIN   rcv_transactions@LEGACY_INSTANCE rt
                   ON rt.transaction_id = log.legacy_rcv_transaction_id
            WHERE  rt.po_line_location_id   = p_po_line_location_id
            AND    log.legacy_po_header_id  = p_po_header_id
            AND    log.interface_status     = c_status_processed
            AND    log.transaction_class    = c_class_invoice
            ORDER BY log.creation_date DESC
        )
        WHERE ROWNUM = 1;

        RETURN l_invoice_num;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN RETURN NULL;
        WHEN OTHERS        THEN RETURN NULL;
    END get_original_invoice_num;


    -- =========================================================================
    -- Private: Validate a receipt record before loading as AP Invoice.
    -- Returns TRUE if valid. Populates p_rejection_reason on failure.
    -- =========================================================================
    FUNCTION validate_receipt (
        p_rcv_transaction_id    IN  NUMBER,
        p_vendor_id             IN  NUMBER,
        p_net_quantity          IN  NUMBER,
        p_net_amount            IN  NUMBER,
        p_legacy_ccid           IN  NUMBER,
        p_rejection_reason      OUT VARCHAR2
    ) RETURN BOOLEAN IS
        l_vendor_count          NUMBER;
        l_already_processed     NUMBER;
        l_new_ccid              NUMBER;
    BEGIN
        p_rejection_reason := NULL;

        -- BR-01: Net quantity or net amount must be positive after RTV netting
        IF NVL(p_net_quantity, 0) <= 0 AND NVL(p_net_amount, 0) <= 0 THEN
            p_rejection_reason := 'Net received quantity/amount is zero or negative after RTV netting. No invoice required.';
            RETURN FALSE;
        END IF;

        -- BR-02: Supplier must exist in new AP instance
        SELECT COUNT(*) INTO l_vendor_count
        FROM   AP_SUPPLIERS
        WHERE  vendor_id = p_vendor_id;

        IF l_vendor_count = 0 THEN
            p_rejection_reason := 'Supplier ID ' || p_vendor_id || ' not found in new AP. Migrate supplier first.';
            RETURN FALSE;
        END IF;

        -- BR-03: Duplicate check - receipt not already processed as an invoice
        SELECT COUNT(*) INTO l_already_processed
        FROM   XXCUST_PO_AP_INTERFACE_LOG
        WHERE  legacy_rcv_transaction_id    = p_rcv_transaction_id
        AND    interface_status             = c_status_processed
        AND    transaction_class            = c_class_invoice;

        IF l_already_processed > 0 THEN
            p_rejection_reason := 'Receipt TXN ID ' || p_rcv_transaction_id || ' already processed as invoice. Duplicate skipped.';
            RETURN FALSE;
        END IF;

        -- BR-04: COA mapping must exist for the distribution account
        l_new_ccid := get_new_ccid(p_legacy_ccid);
        IF l_new_ccid = -1 THEN
            p_rejection_reason := 'No active COA mapping for legacy CCID ' || p_legacy_ccid || '. Update XXCUST_COA_MAPPING.';
            RETURN FALSE;
        END IF;

        RETURN TRUE;
    EXCEPTION
        WHEN OTHERS THEN
            p_rejection_reason := 'Validation error: ' || SQLERRM;
            RETURN FALSE;
    END validate_receipt;


    -- =========================================================================
    -- Private: Validate an RTV record before loading as AP Credit Memo.
    -- Returns TRUE if valid. Populates p_rejection_reason on failure.
    -- =========================================================================
    FUNCTION validate_rtv (
        p_rtv_transaction_id    IN  NUMBER,
        p_po_line_location_id   IN  NUMBER,
        p_po_header_id          IN  NUMBER,
        p_rtv_quantity          IN  NUMBER,
        p_rtv_amount            IN  NUMBER,
        p_original_invoice_num  IN  VARCHAR2,
        p_original_inv_amount   IN  NUMBER,
        p_legacy_ccid           IN  NUMBER,
        p_rejection_reason      OUT VARCHAR2
    ) RETURN BOOLEAN IS
        l_vendor_count          NUMBER;
        l_already_processed     NUMBER;
        l_new_ccid              NUMBER;
    BEGIN
        p_rejection_reason := NULL;

        -- RTV-BR-01: RTV quantity must be positive
        IF NVL(p_rtv_quantity, 0) <= 0 AND NVL(p_rtv_amount, 0) <= 0 THEN
            p_rejection_reason := 'RTV quantity/amount is zero or negative. Cannot create Credit Memo.';
            RETURN FALSE;
        END IF;

        -- RTV-BR-02: Original receipt must have been interfaced as an AP Invoice
        -- (Scenario A RTVs - where no invoice was ever created - are excluded upstream)
        IF p_original_invoice_num IS NULL THEN
            p_rejection_reason := 'No processed AP Invoice found for this PO line location. RTV handled by net quantity logic (Scenario A). Skipping.';
            RETURN FALSE;
        END IF;

        -- RTV-BR-03: Duplicate check - RTV not already processed as Credit Memo
        SELECT COUNT(*) INTO l_already_processed
        FROM   XXCUST_PO_AP_INTERFACE_LOG
        WHERE  legacy_rcv_transaction_id    = p_rtv_transaction_id
        AND    interface_status             = c_status_processed
        AND    transaction_class            = c_class_credit_memo;

        IF l_already_processed > 0 THEN
            p_rejection_reason := 'RTV TXN ID ' || p_rtv_transaction_id || ' already processed as Credit Memo. Duplicate skipped.';
            RETURN FALSE;
        END IF;

        -- RTV-BR-04: Credit amount must not exceed original invoice amount
        IF NVL(p_rtv_amount, 0) > NVL(p_original_inv_amount, 0) THEN
            p_rejection_reason := 'RTV amount (' || p_rtv_amount || ') exceeds original invoice amount (' ||
                                  p_original_inv_amount || '). Manual review required.';
            RETURN FALSE;
        END IF;

        -- RTV-BR-05: COA mapping must exist for the distribution account
        l_new_ccid := get_new_ccid(p_legacy_ccid);
        IF l_new_ccid = -1 THEN
            p_rejection_reason := 'No active COA mapping for legacy CCID ' || p_legacy_ccid || '. Update XXCUST_COA_MAPPING.';
            RETURN FALSE;
        END IF;

        RETURN TRUE;
    EXCEPTION
        WHEN OTHERS THEN
            p_rejection_reason := 'RTV validation error: ' || SQLERRM;
            RETURN FALSE;
    END validate_rtv;


    /* =========================================================================
       PROCESS A: run_receipt_interface
       Receipt-to-Invoice with RTV Net Quantity Logic
       ========================================================================= */
    PROCEDURE run_receipt_interface (
        p_errbuf            OUT     VARCHAR2,
        p_retcode           OUT     NUMBER,
        p_operating_unit    IN      VARCHAR2    DEFAULT NULL,
        p_receipt_date_from IN      VARCHAR2    DEFAULT NULL,
        p_receipt_date_to   IN      VARCHAR2    DEFAULT NULL,
        p_po_number         IN      VARCHAR2    DEFAULT NULL,
        p_debug_mode        IN      VARCHAR2    DEFAULT 'N'
    ) IS

        -- -----------------------------------------------------------------------
        -- Receipt extraction cursor with RTV netting subquery.
        --
        -- KEY CHANGE vs v1.0:
        -- The WHERE clause now includes a net quantity subquery that computes
        -- SUM(RECEIVE qty) - SUM(RETURN TO VENDOR qty) per po_line_location_id.
        -- Records where net quantity <= 0 are excluded at cursor level (Scenario A).
        -- The net quantity and net amount are also computed in the SELECT list
        -- so the invoice amount reflects only the net received position.
        -- -----------------------------------------------------------------------
        CURSOR c_receipts IS
            SELECT
                -- Receipt identifiers
                rsh.receipt_num                                     receipt_number,
                rt.transaction_id                                   rcv_transaction_id,
                rt.transaction_date                                 receipt_date,
                rt.po_line_location_id                             po_line_location_id,

                -- PO details
                ph.po_header_id                                     po_header_id,
                ph.segment1                                         po_number,
                ph.type_lookup_code                                 po_type,
                ph.currency_code                                    po_currency,
                pl.line_num                                         po_line_num,
                pl.item_description                                 item_description,
                pl.purchase_basis                                   purchase_basis,
                pll.shipment_num                                    shipment_num,

                -- Gross received quantities and amounts (from this RECEIVE transaction)
                rt.quantity                                         gross_qty_received,
                rt.po_unit_price                                    unit_price,
                (rt.quantity * rt.po_unit_price)                   gross_amount,

                -- NET quantity after subtracting all RTVs on this PO line location
                -- This is the RTV Scenario A netting applied at cursor level
                (
                    SELECT NVL(SUM(
                               CASE
                                   WHEN rt2.transaction_type = 'RECEIVE'          THEN  rt2.quantity
                                   WHEN rt2.transaction_type = 'RETURN TO VENDOR' THEN -rt2.quantity
                                   ELSE 0
                               END
                           ), 0)
                    FROM   rcv_transactions@LEGACY_INSTANCE rt2
                    WHERE  rt2.po_line_location_id  = rt.po_line_location_id
                    AND    rt2.transaction_type     IN ('RECEIVE', 'RETURN TO VENDOR')
                    AND    rt2.source_document_code = 'PO'
                )                                                   net_qty_received,

                -- Total RTV qty for logging purposes
                (
                    SELECT NVL(SUM(rt3.quantity), 0)
                    FROM   rcv_transactions@LEGACY_INSTANCE rt3
                    WHERE  rt3.po_line_location_id  = rt.po_line_location_id
                    AND    rt3.transaction_type     = 'RETURN TO VENDOR'
                    AND    rt3.source_document_code = 'PO'
                )                                                   total_rtv_qty,

                -- Services: use amount-based received figure
                pll.amount_received                                 amount_received,

                -- UOM
                rt.unit_of_measure                                  uom_code,
                rt.currency_code                                    receipt_currency,
                rt.currency_conversion_rate                         conversion_rate,

                -- Supplier
                ph.vendor_id                                        vendor_id,
                ph.vendor_site_id                                   vendor_site_id,
                aps.vendor_name                                     vendor_name,
                apss.vendor_site_code                               vendor_site_code,

                -- Distribution / accounting
                pod.distribution_num                                distribution_num,
                pod.code_combination_id                             legacy_ccid,
                pod.quantity_billed                                 qty_billed,

                -- Operating unit
                ph.org_id                                           org_id,
                hou.name                                            operating_unit_name

            FROM
                rcv_transactions@LEGACY_INSTANCE            rt
                JOIN rcv_shipment_headers@LEGACY_INSTANCE   rsh ON rsh.shipment_header_id   = rt.shipment_header_id
                JOIN rcv_shipment_lines@LEGACY_INSTANCE     rsl ON rsl.shipment_line_id     = rt.shipment_line_id
                JOIN po_headers_all@LEGACY_INSTANCE         ph  ON ph.po_header_id          = rt.po_header_id
                JOIN po_lines_all@LEGACY_INSTANCE           pl  ON pl.po_header_id          = rt.po_header_id
                                                                AND pl.po_line_id            = rt.po_line_id
                JOIN po_line_locations_all@LEGACY_INSTANCE  pll ON pll.line_location_id     = rt.po_line_location_id
                JOIN po_distributions_all@LEGACY_INSTANCE   pod ON pod.line_location_id     = rt.po_line_location_id
                JOIN ap_suppliers@LEGACY_INSTANCE           aps ON aps.vendor_id            = ph.vendor_id
                JOIN ap_supplier_sites_all@LEGACY_INSTANCE  apss ON apss.vendor_site_id     = ph.vendor_site_id
                JOIN hr_operating_units@LEGACY_INSTANCE     hou ON hou.organization_id      = ph.org_id

            WHERE
                -- Only confirmed RECEIVE transactions (not RTVs, CORRECTs etc.)
                rt.transaction_type             = 'RECEIVE'
                AND rt.source_document_code     = 'PO'

                -- Active PO only
                AND ph.closed_code              NOT IN ('FINALLY CLOSED', 'CANCELLED')
                AND ph.cancel_flag              = 'N'
                AND pll.cancel_flag             = 'N'

                -- Exclude fully billed lines
                AND NVL(pod.quantity_billed, 0) < rt.quantity

                -- RTV NETTING (Scenario A): Exclude where net received qty <= 0
                -- i.e. all goods have been returned before any invoice was created
                AND (
                    SELECT NVL(SUM(
                               CASE
                                   WHEN rt2.transaction_type = 'RECEIVE'          THEN  rt2.quantity
                                   WHEN rt2.transaction_type = 'RETURN TO VENDOR' THEN -rt2.quantity
                                   ELSE 0
                               END
                           ), 0)
                    FROM   rcv_transactions@LEGACY_INSTANCE rt2
                    WHERE  rt2.po_line_location_id  = rt.po_line_location_id
                    AND    rt2.transaction_type     IN ('RECEIVE', 'RETURN TO VENDOR')
                    AND    rt2.source_document_code = 'PO'
                ) > 0

                -- Optional filters
                AND (p_operating_unit    IS NULL OR hou.name       = p_operating_unit)
                AND (p_receipt_date_from IS NULL OR rt.transaction_date >= TO_DATE(p_receipt_date_from, 'DD-MON-YYYY'))
                AND (p_receipt_date_to   IS NULL OR rt.transaction_date <= TO_DATE(p_receipt_date_to,   'DD-MON-YYYY') + 1)
                AND (p_po_number         IS NULL OR ph.segment1    = p_po_number)

            ORDER BY ph.vendor_id, ph.segment1, pl.line_num, rt.transaction_date;

        -- Local variables
        l_run_id                VARCHAR2(50)    := 'INV-' || TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS');
        l_group_id              NUMBER;
        l_invoice_num           VARCHAR2(50);
        l_new_ccid              NUMBER;
        l_rejection_reason      VARCHAR2(2000);
        l_is_valid              BOOLEAN;
        l_records_processed     NUMBER          := 0;
        l_records_rejected      NUMBER          := 0;
        l_records_skipped       NUMBER          := 0;
        l_records_errored       NUMBER          := 0;
        l_invoice_interface_id  NUMBER;
        l_line_interface_id     NUMBER;
        l_net_invoice_amount    NUMBER;

    BEGIN
        p_retcode := 0;
        SELECT AP_INVOICES_INTERFACE_S.NEXTVAL INTO l_group_id FROM DUAL;

        log_message('============================================================');
        log_message('PROCESS A: Receipt-to-Invoice Interface (v2.0 with RTV Netting)');
        log_message('Run ID: ' || l_run_id || '  |  Group ID: ' || l_group_id);
        log_message('Operating Unit  : ' || NVL(p_operating_unit,    'ALL'));
        log_message('Receipt Date From: ' || NVL(p_receipt_date_from, 'NONE'));
        log_message('Receipt Date To  : ' || NVL(p_receipt_date_to,   'NONE'));
        log_message('PO Number Filter : ' || NVL(p_po_number,         'ALL'));
        log_message('============================================================');

        FOR r IN c_receipts LOOP
            BEGIN
                -- -------------------------------------------------------------
                -- Compute net invoice amount
                -- For Goods:    net_qty_received x unit_price
                -- For Services: amount_received (already net from Oracle)
                -- -------------------------------------------------------------
                IF r.purchase_basis IN ('TEMP LABOR', 'FIXED PRICE') THEN
                    l_net_invoice_amount := r.amount_received;
                ELSE
                    l_net_invoice_amount := r.net_qty_received * r.unit_price;
                END IF;

                -- Log if RTV netting reduced the invoice amount
                IF r.total_rtv_qty > 0 AND p_debug_mode = 'Y' THEN
                    log_message('RTV NETTING: PO#' || r.po_number || ' Line:' || r.po_line_num ||
                                ' Gross:' || r.gross_qty_received || ' RTV:' || r.total_rtv_qty ||
                                ' Net:' || r.net_qty_received || ' NetAmt:' || l_net_invoice_amount);
                END IF;

                -- -------------------------------------------------------------
                -- VALIDATE
                -- -------------------------------------------------------------
                l_new_ccid  := get_new_ccid(r.legacy_ccid);
                l_is_valid  := validate_receipt(
                    p_rcv_transaction_id    => r.rcv_transaction_id,
                    p_vendor_id             => r.vendor_id,
                    p_net_quantity          => r.net_qty_received,
                    p_net_amount            => l_net_invoice_amount,
                    p_legacy_ccid           => r.legacy_ccid,
                    p_rejection_reason      => l_rejection_reason
                );

                IF NOT l_is_valid THEN
                    -- Determine if this is a hard rejection or a zero-net skip
                    DECLARE
                        l_status VARCHAR2(20) := c_status_rejected;
                    BEGIN
                        IF l_rejection_reason LIKE '%zero or negative%' THEN
                            l_status := c_status_skipped;
                            l_records_skipped := l_records_skipped + 1;
                        ELSE
                            l_records_rejected := l_records_rejected + 1;
                        END IF;

                        INSERT INTO XXCUST_PO_AP_INTERFACE_LOG (
                            run_id, transaction_class,
                            legacy_rcv_transaction_id, legacy_po_header_id,
                            legacy_po_number, legacy_po_line_num, legacy_vendor_id,
                            vendor_name, receipt_number, receipt_date,
                            receipt_quantity, rtv_quantity, net_quantity,
                            interface_status, rejection_reason
                        ) VALUES (
                            l_run_id, c_class_invoice,
                            r.rcv_transaction_id, r.po_header_id,
                            r.po_number, r.po_line_num, r.vendor_id,
                            r.vendor_name, r.receipt_number, r.receipt_date,
                            r.gross_qty_received, r.total_rtv_qty, r.net_qty_received,
                            l_status, l_rejection_reason
                        );
                    END;
                    CONTINUE;
                END IF;

                -- -------------------------------------------------------------
                -- GENERATE INVOICE NUMBER
                -- Convention: RCPT-{receipt_number}-L{po_line_num}
                -- -------------------------------------------------------------
                l_invoice_num := 'RCPT-' || r.receipt_number || '-L' || LPAD(r.po_line_num, 3, '0');

                SELECT AP_INVOICES_INTERFACE_S.NEXTVAL  INTO l_invoice_interface_id FROM DUAL;
                SELECT AP_INVOICE_LINES_INTERFACE_S.NEXTVAL INTO l_line_interface_id FROM DUAL;

                -- -------------------------------------------------------------
                -- LOAD: AP Invoice Header
                -- -------------------------------------------------------------
                INSERT INTO AP_INVOICES_INTERFACE (
                    invoice_id, invoice_num, invoice_type_lookup_code,
                    invoice_date, vendor_id, vendor_site_id,
                    invoice_amount, invoice_currency_code,
                    exchange_rate, exchange_rate_type, exchange_date,
                    description, source, group_id, org_id,
                    pay_group_lookup_code, goods_received_date,
                    receipt_verified_flag, po_number,
                    created_by, creation_date, last_updated_by, last_update_date, status
                ) VALUES (
                    l_invoice_interface_id,
                    l_invoice_num,
                    c_invoice_type,
                    TRUNC(SYSDATE),
                    r.vendor_id,
                    r.vendor_site_id,
                    l_net_invoice_amount,
                    r.po_currency,
                    r.conversion_rate,
                    'User',
                    r.receipt_date,
                    'PO Receipt Invoice: PO# ' || r.po_number ||
                        ' | Receipt# ' || r.receipt_number ||
                        ' | Net Qty: ' || r.net_qty_received ||
                        CASE WHEN r.total_rtv_qty > 0
                             THEN ' (Net of RTV: ' || r.total_rtv_qty || ')'
                             ELSE '' END,
                    c_source_name,
                    l_group_id,
                    r.org_id,
                    c_pay_group,
                    r.receipt_date,
                    'Y',
                    r.po_number,
                    c_created_by, SYSDATE, c_created_by, SYSDATE,
                    NULL
                );

                -- -------------------------------------------------------------
                -- LOAD: AP Invoice Line
                -- -------------------------------------------------------------
                INSERT INTO AP_INVOICE_LINES_INTERFACE (
                    invoice_id, invoice_line_id, line_number,
                    line_type_lookup_code, amount,
                    quantity_invoiced, unit_price, unit_meas_lookup_code,
                    description, po_header_id, rcv_transaction_id,
                    dist_code_combination_id, org_id,
                    created_by, creation_date, last_updated_by, last_update_date
                ) VALUES (
                    l_invoice_interface_id,
                    l_line_interface_id,
                    r.po_line_num,
                    'ITEM',
                    l_net_invoice_amount,
                    CASE WHEN r.purchase_basis IN ('TEMP LABOR','FIXED PRICE') THEN NULL ELSE r.net_qty_received END,
                    CASE WHEN r.purchase_basis IN ('TEMP LABOR','FIXED PRICE') THEN NULL ELSE r.unit_price END,
                    r.uom_code,
                    r.item_description || ' | PO Line: ' || r.po_line_num ||
                        ' | Received: ' || TO_CHAR(r.receipt_date,'DD-MON-YYYY') ||
                        ' | Net Qty: ' || r.net_qty_received,
                    r.po_header_id,
                    r.rcv_transaction_id,
                    l_new_ccid,
                    r.org_id,
                    c_created_by, SYSDATE, c_created_by, SYSDATE
                );

                -- -------------------------------------------------------------
                -- LOG successful processing
                -- -------------------------------------------------------------
                INSERT INTO XXCUST_PO_AP_INTERFACE_LOG (
                    run_id, transaction_class,
                    legacy_rcv_transaction_id, legacy_po_header_id,
                    legacy_po_number, legacy_po_line_num, legacy_vendor_id,
                    vendor_name, receipt_number, receipt_date,
                    receipt_quantity, rtv_quantity, net_quantity,
                    receipt_amount, invoice_num, invoice_amount, interface_status
                ) VALUES (
                    l_run_id, c_class_invoice,
                    r.rcv_transaction_id, r.po_header_id,
                    r.po_number, r.po_line_num, r.vendor_id,
                    r.vendor_name, r.receipt_number, r.receipt_date,
                    r.gross_qty_received, r.total_rtv_qty, r.net_qty_received,
                    l_net_invoice_amount, l_invoice_num, l_net_invoice_amount,
                    c_status_processed
                );

                l_records_processed := l_records_processed + 1;

                IF p_debug_mode = 'Y' THEN
                    log_message('PROCESSED: PO#' || r.po_number ||
                                ' Receipt:' || r.receipt_number ||
                                ' Invoice:' || l_invoice_num ||
                                ' NetAmt:' || l_net_invoice_amount);
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    INSERT INTO XXCUST_PO_AP_INTERFACE_LOG (
                        run_id, transaction_class,
                        legacy_rcv_transaction_id, legacy_po_header_id,
                        legacy_po_number, legacy_po_line_num, legacy_vendor_id,
                        vendor_name, receipt_number, receipt_date,
                        interface_status, rejection_reason
                    ) VALUES (
                        l_run_id, c_class_invoice,
                        r.rcv_transaction_id, r.po_header_id,
                        r.po_number, r.po_line_num, r.vendor_id,
                        r.vendor_name, r.receipt_number, r.receipt_date,
                        c_status_error, 'Unexpected error: ' || SQLERRM
                    );
                    l_records_errored := l_records_errored + 1;
                    log_message('ERROR: PO#' || r.po_number || ' | ' || SQLERRM);
            END;
        END LOOP;

        COMMIT;

        log_message('============================================================');
        log_message('PROCESS A Complete - Run ID: ' || l_run_id);
        log_message('  Processed : ' || l_records_processed);
        log_message('  Rejected  : ' || l_records_rejected);
        log_message('  Skipped   : ' || l_records_skipped || '  (net qty = 0 after RTV netting)');
        log_message('  Errors    : ' || l_records_errored);
        log_message('Next: Run Payables Open Interface Import | Source: ' || c_source_name ||
                    ' | Group ID: ' || l_group_id);
        log_message('============================================================');

        IF l_records_rejected > 0 OR l_records_errored > 0 THEN
            p_retcode := 1;
            p_errbuf  := 'Process A completed with ' || l_records_rejected ||
                         ' rejections and ' || l_records_errored || ' errors. Review XXCUST_PO_AP_INTERFACE_LOG.';
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            p_retcode := 2;
            p_errbuf  := 'Process A fatal error: ' || SQLERRM;
            log_message('FATAL ERROR (Process A): ' || SQLERRM);
    END run_receipt_interface;


    /* =========================================================================
       PROCESS B: run_rtv_interface
       RTV-to-Credit-Memo Interface (Scenarios B and C)

       Handles RETURN TO VENDOR transactions that occurred AFTER an AP Invoice
       was already created in the new AP instance. Generates Credit Memo
       (CREDIT type) to offset the original Standard invoice.

       Scenario B (Full RTV):    Credit Memo = full returned qty x unit_price
       Scenario C (Partial RTV): Credit Memo = partial returned qty x unit_price

       RTVs where no AP Invoice was previously created (Scenario A) are
       identified via get_original_invoice_num() returning NULL, logged as
       SKIPPED, and excluded from Credit Memo creation.
       ========================================================================= */
    PROCEDURE run_rtv_interface (
        p_errbuf            OUT     VARCHAR2,
        p_retcode           OUT     NUMBER,
        p_operating_unit    IN      VARCHAR2    DEFAULT NULL,
        p_rtv_date_from     IN      VARCHAR2    DEFAULT NULL,
        p_rtv_date_to       IN      VARCHAR2    DEFAULT NULL,
        p_po_number         IN      VARCHAR2    DEFAULT NULL,
        p_debug_mode        IN      VARCHAR2    DEFAULT 'N'
    ) IS

        -- -----------------------------------------------------------------------
        -- RTV extraction cursor.
        -- Selects all RETURN TO VENDOR transactions from the legacy instance.
        -- Only RTVs sourced from POs are included.
        -- The interface logic downstream determines Scenario A vs B/C.
        -- -----------------------------------------------------------------------
        CURSOR c_rtvs IS
            SELECT
                rsh.receipt_num                                     receipt_number,
                rt.transaction_id                                   rtv_transaction_id,
                rt.transaction_date                                 rtv_date,
                rt.po_line_location_id                             po_line_location_id,

                ph.po_header_id                                     po_header_id,
                ph.segment1                                         po_number,
                ph.type_lookup_code                                 po_type,
                ph.currency_code                                    po_currency,
                pl.line_num                                         po_line_num,
                pl.item_description                                 item_description,
                pl.purchase_basis                                   purchase_basis,

                -- RTV quantities and amounts
                rt.quantity                                         rtv_quantity,
                rt.po_unit_price                                    unit_price,
                (rt.quantity * rt.po_unit_price)                   rtv_amount,

                -- Services: use amount-based figure
                pll.amount_received                                 amount_received,

                rt.unit_of_measure                                  uom_code,
                rt.currency_code                                    receipt_currency,
                rt.currency_conversion_rate                         conversion_rate,

                ph.vendor_id                                        vendor_id,
                ph.vendor_site_id                                   vendor_site_id,
                aps.vendor_name                                     vendor_name,

                pod.code_combination_id                             legacy_ccid,
                ph.org_id                                           org_id,
                hou.name                                            operating_unit_name

            FROM
                rcv_transactions@LEGACY_INSTANCE            rt
                JOIN rcv_shipment_headers@LEGACY_INSTANCE   rsh ON rsh.shipment_header_id   = rt.shipment_header_id
                JOIN rcv_shipment_lines@LEGACY_INSTANCE     rsl ON rsl.shipment_line_id     = rt.shipment_line_id
                JOIN po_headers_all@LEGACY_INSTANCE         ph  ON ph.po_header_id          = rt.po_header_id
                JOIN po_lines_all@LEGACY_INSTANCE           pl  ON pl.po_header_id          = rt.po_header_id
                                                                AND pl.po_line_id            = rt.po_line_id
                JOIN po_line_locations_all@LEGACY_INSTANCE  pll ON pll.line_location_id     = rt.po_line_location_id
                JOIN po_distributions_all@LEGACY_INSTANCE   pod ON pod.line_location_id     = rt.po_line_location_id
                JOIN ap_suppliers@LEGACY_INSTANCE           aps ON aps.vendor_id            = ph.vendor_id
                JOIN ap_supplier_sites_all@LEGACY_INSTANCE  apss ON apss.vendor_site_id     = ph.vendor_site_id
                JOIN hr_operating_units@LEGACY_INSTANCE     hou ON hou.organization_id      = ph.org_id

            WHERE
                -- Only RETURN TO VENDOR transactions
                rt.transaction_type             = 'RETURN TO VENDOR'
                AND rt.source_document_code     = 'PO'

                -- Active PO
                AND ph.cancel_flag              = 'N'

                -- RTV must have positive quantity
                AND rt.quantity                 > 0

                -- Optional filters
                AND (p_operating_unit   IS NULL OR hou.name         = p_operating_unit)
                AND (p_rtv_date_from    IS NULL OR rt.transaction_date >= TO_DATE(p_rtv_date_from, 'DD-MON-YYYY'))
                AND (p_rtv_date_to      IS NULL OR rt.transaction_date <= TO_DATE(p_rtv_date_to,   'DD-MON-YYYY') + 1)
                AND (p_po_number        IS NULL OR ph.segment1      = p_po_number)

            ORDER BY ph.vendor_id, ph.segment1, pl.line_num, rt.transaction_date;

        -- Local variables
        l_run_id                VARCHAR2(50)    := 'RTV-' || TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS');
        l_group_id              NUMBER;
        l_credit_memo_num       VARCHAR2(50);
        l_original_invoice_num  VARCHAR2(50);
        l_original_inv_amount   NUMBER;
        l_new_ccid              NUMBER;
        l_rejection_reason      VARCHAR2(2000);
        l_is_valid              BOOLEAN;
        l_records_processed     NUMBER          := 0;
        l_records_rejected      NUMBER          := 0;
        l_records_skipped       NUMBER          := 0;
        l_records_errored       NUMBER          := 0;
        l_invoice_interface_id  NUMBER;
        l_line_interface_id     NUMBER;
        l_credit_amount         NUMBER;

    BEGIN
        p_retcode := 0;
        SELECT AP_INVOICES_INTERFACE_S.NEXTVAL INTO l_group_id FROM DUAL;

        log_message('============================================================');
        log_message('PROCESS B: RTV-to-Credit-Memo Interface (v2.0)');
        log_message('Run ID: ' || l_run_id || '  |  Group ID: ' || l_group_id);
        log_message('Operating Unit : ' || NVL(p_operating_unit,  'ALL'));
        log_message('RTV Date From  : ' || NVL(p_rtv_date_from,   'NONE'));
        log_message('RTV Date To    : ' || NVL(p_rtv_date_to,     'NONE'));
        log_message('PO Number      : ' || NVL(p_po_number,       'ALL'));
        log_message('============================================================');

        FOR r IN c_rtvs LOOP
            BEGIN
                -- -------------------------------------------------------------
                -- STEP 1: Check if original AP Invoice was ever created
                -- Determines Scenario A (skip) vs Scenario B/C (credit memo)
                -- -------------------------------------------------------------
                l_original_invoice_num := get_original_invoice_num(
                    p_po_line_location_id   => r.po_line_location_id,
                    p_po_header_id          => r.po_header_id
                );

                -- If no original invoice found = Scenario A (net qty handled it)
                IF l_original_invoice_num IS NULL THEN
                    INSERT INTO XXCUST_PO_AP_INTERFACE_LOG (
                        run_id, transaction_class,
                        legacy_rcv_transaction_id, legacy_po_header_id,
                        legacy_po_number, legacy_po_line_num, legacy_vendor_id,
                        vendor_name, receipt_number, receipt_date,
                        rtv_quantity, interface_status, rejection_reason
                    ) VALUES (
                        l_run_id, c_class_credit_memo,
                        r.rtv_transaction_id, r.po_header_id,
                        r.po_number, r.po_line_num, r.vendor_id,
                        r.vendor_name, r.receipt_number, r.rtv_date,
                        r.rtv_quantity,
                        c_status_skipped,
                        'Scenario A RTV: no AP Invoice was previously created for this PO line. ' ||
                        'Net quantity logic handled this return. No Credit Memo required.'
                    );
                    l_records_skipped := l_records_skipped + 1;
                    IF p_debug_mode = 'Y' THEN
                        log_message('SKIPPED (Scenario A): PO#' || r.po_number ||
                                    ' Line:' || r.po_line_num || ' RTV Qty:' || r.rtv_quantity);
                    END IF;
                    CONTINUE;
                END IF;

                -- -------------------------------------------------------------
                -- STEP 2: Get original invoice amount for over-credit check
                -- -------------------------------------------------------------
                BEGIN
                    SELECT invoice_amount
                    INTO   l_original_inv_amount
                    FROM   AP_INVOICES_ALL
                    WHERE  invoice_num  = l_original_invoice_num
                    AND    vendor_id    = r.vendor_id
                    AND    ROWNUM       = 1;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        l_original_inv_amount := r.rtv_amount; -- fallback: allow credit
                END;

                -- -------------------------------------------------------------
                -- STEP 3: Compute Credit Memo amount
                -- Scenario B (full RTV):    full rtv_amount
                -- Scenario C (partial RTV): partial rtv_amount (rtv_qty x unit_price)
                -- For Services: use amount_received
                -- -------------------------------------------------------------
                IF r.purchase_basis IN ('TEMP LABOR', 'FIXED PRICE') THEN
                    l_credit_amount := r.amount_received;
                ELSE
                    l_credit_amount := r.rtv_quantity * r.unit_price;
                END IF;

                -- -------------------------------------------------------------
                -- STEP 4: Validate RTV record
                -- -------------------------------------------------------------
                l_new_ccid  := get_new_ccid(r.legacy_ccid);
                l_is_valid  := validate_rtv(
                    p_rtv_transaction_id    => r.rtv_transaction_id,
                    p_po_line_location_id   => r.po_line_location_id,
                    p_po_header_id          => r.po_header_id,
                    p_rtv_quantity          => r.rtv_quantity,
                    p_rtv_amount            => l_credit_amount,
                    p_original_invoice_num  => l_original_invoice_num,
                    p_original_inv_amount   => l_original_inv_amount,
                    p_legacy_ccid           => r.legacy_ccid,
                    p_rejection_reason      => l_rejection_reason
                );

                IF NOT l_is_valid THEN
                    INSERT INTO XXCUST_PO_AP_INTERFACE_LOG (
                        run_id, transaction_class,
                        legacy_rcv_transaction_id, legacy_po_header_id,
                        legacy_po_number, legacy_po_line_num, legacy_vendor_id,
                        vendor_name, receipt_number, receipt_date,
                        rtv_quantity, interface_status, rejection_reason
                    ) VALUES (
                        l_run_id, c_class_credit_memo,
                        r.rtv_transaction_id, r.po_header_id,
                        r.po_number, r.po_line_num, r.vendor_id,
                        r.vendor_name, r.receipt_number, r.rtv_date,
                        r.rtv_quantity,
                        c_status_rejected, l_rejection_reason
                    );
                    l_records_rejected := l_records_rejected + 1;
                    log_message('REJECTED (RTV): PO#' || r.po_number ||
                                ' Line:' || r.po_line_num || ' | ' || l_rejection_reason);
                    CONTINUE;
                END IF;

                -- -------------------------------------------------------------
                -- STEP 5: Generate Credit Memo number
                -- Convention: CM-{original_invoice_num}
                -- -------------------------------------------------------------
                l_credit_memo_num := 'CM-' || l_original_invoice_num;

                SELECT AP_INVOICES_INTERFACE_S.NEXTVAL  INTO l_invoice_interface_id FROM DUAL;
                SELECT AP_INVOICE_LINES_INTERFACE_S.NEXTVAL INTO l_line_interface_id FROM DUAL;

                -- -------------------------------------------------------------
                -- LOAD: Credit Memo Header (invoice_type_lookup_code = CREDIT)
                -- Note: Credit amounts are loaded as POSITIVE values.
                -- Oracle AP handles the sign reversal for CREDIT type invoices.
                -- -------------------------------------------------------------
                INSERT INTO AP_INVOICES_INTERFACE (
                    invoice_id, invoice_num, invoice_type_lookup_code,
                    invoice_date, vendor_id, vendor_site_id,
                    invoice_amount, invoice_currency_code,
                    exchange_rate, exchange_rate_type, exchange_date,
                    description, source, group_id, org_id,
                    pay_group_lookup_code, goods_received_date,
                    po_number,
                    created_by, creation_date, last_updated_by, last_update_date, status
                ) VALUES (
                    l_invoice_interface_id,
                    l_credit_memo_num,
                    c_credit_type,                              -- CREDIT type = Credit Memo
                    TRUNC(SYSDATE),
                    r.vendor_id,
                    r.vendor_site_id,
                    l_credit_amount,                            -- positive; AP reverses sign for CREDIT type
                    r.po_currency,
                    r.conversion_rate,
                    'User',
                    r.rtv_date,
                    'RTV Credit Memo: PO# ' || r.po_number ||
                        ' | Original Invoice: ' || l_original_invoice_num ||
                        ' | RTV Date: ' || TO_CHAR(r.rtv_date,'DD-MON-YYYY') ||
                        ' | RTV Qty: ' || r.rtv_quantity,
                    c_source_name,
                    l_group_id,
                    r.org_id,
                    c_pay_group,
                    r.rtv_date,
                    r.po_number,
                    c_created_by, SYSDATE, c_created_by, SYSDATE,
                    NULL
                );

                -- -------------------------------------------------------------
                -- LOAD: Credit Memo Line
                -- line_type_lookup_code = ITEM, amount = positive (AP reverses)
                -- -------------------------------------------------------------
                INSERT INTO AP_INVOICE_LINES_INTERFACE (
                    invoice_id, invoice_line_id, line_number,
                    line_type_lookup_code, amount,
                    quantity_invoiced, unit_price, unit_meas_lookup_code,
                    description, po_header_id, rcv_transaction_id,
                    dist_code_combination_id, org_id,
                    created_by, creation_date, last_updated_by, last_update_date
                ) VALUES (
                    l_invoice_interface_id,
                    l_line_interface_id,
                    r.po_line_num,
                    'ITEM',
                    l_credit_amount,
                    CASE WHEN r.purchase_basis IN ('TEMP LABOR','FIXED PRICE') THEN NULL ELSE r.rtv_quantity END,
                    CASE WHEN r.purchase_basis IN ('TEMP LABOR','FIXED PRICE') THEN NULL ELSE r.unit_price END,
                    r.uom_code,
                    'RTV: ' || r.item_description ||
                        ' | PO Line: ' || r.po_line_num ||
                        ' | RTV Date: ' || TO_CHAR(r.rtv_date,'DD-MON-YYYY'),
                    r.po_header_id,
                    r.rtv_transaction_id,
                    l_new_ccid,
                    r.org_id,
                    c_created_by, SYSDATE, c_created_by, SYSDATE
                );

                -- -------------------------------------------------------------
                -- LOG successful Credit Memo load
                -- -------------------------------------------------------------
                INSERT INTO XXCUST_PO_AP_INTERFACE_LOG (
                    run_id, transaction_class,
                    legacy_rcv_transaction_id, legacy_po_header_id,
                    legacy_po_number, legacy_po_line_num, legacy_vendor_id,
                    vendor_name, receipt_number, receipt_date,
                    rtv_quantity, receipt_amount,
                    invoice_num, invoice_amount, interface_status
                ) VALUES (
                    l_run_id, c_class_credit_memo,
                    r.rtv_transaction_id, r.po_header_id,
                    r.po_number, r.po_line_num, r.vendor_id,
                    r.vendor_name, r.receipt_number, r.rtv_date,
                    r.rtv_quantity, l_credit_amount,
                    l_credit_memo_num, l_credit_amount,
                    c_status_processed
                );

                l_records_processed := l_records_processed + 1;

                IF p_debug_mode = 'Y' THEN
                    log_message('PROCESSED (RTV): PO#' || r.po_number ||
                                ' CreditMemo:' || l_credit_memo_num ||
                                ' Amount:' || l_credit_amount ||
                                ' OrigInvoice:' || l_original_invoice_num);
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    INSERT INTO XXCUST_PO_AP_INTERFACE_LOG (
                        run_id, transaction_class,
                        legacy_rcv_transaction_id, legacy_po_header_id,
                        legacy_po_number, legacy_po_line_num, legacy_vendor_id,
                        vendor_name, receipt_number, receipt_date,
                        interface_status, rejection_reason
                    ) VALUES (
                        l_run_id, c_class_credit_memo,
                        r.rtv_transaction_id, r.po_header_id,
                        r.po_number, r.po_line_num, r.vendor_id,
                        r.vendor_name, r.receipt_number, r.rtv_date,
                        c_status_error, 'Unexpected error: ' || SQLERRM
                    );
                    l_records_errored := l_records_errored + 1;
                    log_message('ERROR (RTV): PO#' || r.po_number || ' | ' || SQLERRM);
            END;
        END LOOP;

        COMMIT;

        log_message('============================================================');
        log_message('PROCESS B Complete - Run ID: ' || l_run_id);
        log_message('  Credit Memos Processed : ' || l_records_processed);
        log_message('  Rejected               : ' || l_records_rejected);
        log_message('  Skipped (Scenario A)   : ' || l_records_skipped ||
                    '  (no prior AP Invoice; handled by net qty logic)');
        log_message('  Errors                 : ' || l_records_errored);
        log_message('Next: Run Payables Open Interface Import | Source: ' || c_source_name ||
                    ' | Group ID: ' || l_group_id);
        log_message('============================================================');

        IF l_records_rejected > 0 OR l_records_errored > 0 THEN
            p_retcode := 1;
            p_errbuf  := 'Process B completed with ' || l_records_rejected ||
                         ' rejections and ' || l_records_errored || ' errors. Review XXCUST_PO_AP_INTERFACE_LOG.';
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            p_retcode := 2;
            p_errbuf  := 'Process B fatal error: ' || SQLERRM;
            log_message('FATAL ERROR (Process B): ' || SQLERRM);
    END run_rtv_interface;


    -- =========================================================================
    -- Purge successfully processed log records older than N days
    -- =========================================================================
    PROCEDURE purge_log (p_days_to_keep IN NUMBER DEFAULT 90) IS
        l_count NUMBER;
    BEGIN
        DELETE FROM XXCUST_PO_AP_INTERFACE_LOG
        WHERE  creation_date    < SYSDATE - p_days_to_keep
        AND    interface_status = c_status_processed;

        l_count := SQL%ROWCOUNT;
        COMMIT;
        log_message('Purge complete. ' || l_count || ' log records deleted (older than ' ||
                    p_days_to_keep || ' days).');
    END purge_log;


END XXCUST_PO_AP_INTERFACE_PKG;
/


/* ============================================================================
   SECTION 4: RECONCILIATION QUERIES
   ============================================================================ */

-- -----------------------------------------------------------------------
-- Query 4A: Receipt-to-Invoice Reconciliation
-- Run after Payables Open Interface Import to confirm all receipts
-- were successfully converted to Standard AP Invoices.
-- -----------------------------------------------------------------------
SELECT
    log.run_id,
    log.transaction_class,
    log.legacy_po_number                        po_number,
    log.vendor_name,
    log.receipt_number,
    log.receipt_date,
    log.receipt_quantity                        gross_qty,
    log.rtv_quantity                            rtv_qty,
    log.net_quantity                            net_qty,
    log.invoice_num,
    log.invoice_amount                          interfaced_amount,
    ai.invoice_id                               ap_invoice_id,
    ai.invoice_amount                           ap_invoice_amount,
    ai.approval_status,
    CASE
        WHEN log.interface_status = c_status_processed AND ai.invoice_id IS NULL
            THEN 'WARNING: Loaded to interface but AP Invoice not found. Re-run AP Import.'
        WHEN log.interface_status = c_status_rejected
            THEN 'REJECTED: ' || log.rejection_reason
        WHEN log.interface_status = c_status_skipped
            THEN 'SKIPPED (net qty=0): ' || log.rejection_reason
        WHEN log.interface_status = c_status_error
            THEN 'ERROR: ' || log.rejection_reason
        WHEN log.interface_status = c_status_processed AND ai.invoice_id IS NOT NULL
            THEN 'OK'
        ELSE 'UNKNOWN'
    END                                         reconciliation_status
FROM
    XXCUST_PO_AP_INTERFACE_LOG  log
    LEFT JOIN AP_INVOICES_ALL   ai  ON ai.invoice_num = log.invoice_num
                                   AND ai.vendor_id   = log.legacy_vendor_id
WHERE
    log.run_id              = :p_run_id
    AND log.transaction_class = 'INVOICE'
ORDER BY log.interface_status, log.legacy_po_number;


-- -----------------------------------------------------------------------
-- Query 4B: RTV-to-Credit-Memo Reconciliation
-- Run after Payables Open Interface Import to confirm all RTVs
-- were successfully converted to AP Credit Memos.
-- -----------------------------------------------------------------------
SELECT
    log.run_id,
    log.transaction_class,
    log.legacy_po_number                        po_number,
    log.vendor_name,
    log.receipt_date                            rtv_date,
    log.rtv_quantity,
    log.invoice_num                             credit_memo_num,
    log.invoice_amount                          credit_memo_amount,
    ai.invoice_id                               ap_credit_memo_id,
    ai.invoice_amount                           ap_credit_amount,
    ai.approval_status,
    CASE
        WHEN log.interface_status = c_status_processed AND ai.invoice_id IS NULL
            THEN 'WARNING: Loaded to interface but Credit Memo not found. Re-run AP Import.'
        WHEN log.interface_status = c_status_rejected
            THEN 'REJECTED: ' || log.rejection_reason
        WHEN log.interface_status = c_status_skipped
            THEN 'SKIPPED (Scenario A - no prior invoice): ' || log.rejection_reason
        WHEN log.interface_status = c_status_processed AND ai.invoice_id IS NOT NULL
            THEN 'OK'
        ELSE 'UNKNOWN'
    END                                         reconciliation_status
FROM
    XXCUST_PO_AP_INTERFACE_LOG  log
    LEFT JOIN AP_INVOICES_ALL   ai  ON ai.invoice_num = log.invoice_num
                                   AND ai.vendor_id   = log.legacy_vendor_id
                                   AND ai.invoice_type_lookup_code = 'CREDIT'
WHERE
    log.run_id              = :p_run_id
    AND log.transaction_class = 'CREDIT_MEMO'
ORDER BY log.interface_status, log.legacy_po_number;


-- -----------------------------------------------------------------------
-- Query 4C: Net AP Balance per PO Line (Master Reconciliation)
-- Confirms net AP exposure = net received qty x unit price for each PO line.
-- Run this as the final sign-off check after both Process A and Process B.
-- -----------------------------------------------------------------------
SELECT
    inv_log.legacy_po_number                            po_number,
    inv_log.legacy_po_line_num                          po_line,
    inv_log.vendor_name,
    inv_log.receipt_quantity                            gross_received_qty,
    NVL(rtv_log.rtv_quantity, 0)                        total_rtv_qty,
    (inv_log.receipt_quantity - NVL(rtv_log.rtv_quantity,0))  net_qty,
    inv_log.invoice_amount                              invoice_amount,
    NVL(rtv_log.invoice_amount, 0)                      credit_memo_amount,
    (inv_log.invoice_amount - NVL(rtv_log.invoice_amount,0))  net_ap_balance,
    CASE
        WHEN ABS((inv_log.invoice_amount - NVL(rtv_log.invoice_amount,0)) -
                 ((inv_log.receipt_quantity - NVL(rtv_log.rtv_quantity,0)) * inv_log.invoice_amount
                  / NULLIF(inv_log.receipt_quantity,0))) < 0.01
            THEN 'BALANCED'
        ELSE 'VARIANCE - REVIEW'
    END                                                 balance_status
FROM
    (   -- Aggregated invoice log per PO line
        SELECT  legacy_po_number, legacy_po_line_num, vendor_name,
                SUM(receipt_quantity)   receipt_quantity,
                SUM(invoice_amount)     invoice_amount
        FROM    XXCUST_PO_AP_INTERFACE_LOG
        WHERE   transaction_class   = 'INVOICE'
        AND     interface_status    = 'PROCESSED'
        GROUP BY legacy_po_number, legacy_po_line_num, vendor_name
    ) inv_log
    LEFT JOIN (
        -- Aggregated credit memo log per PO line
        SELECT  legacy_po_number, legacy_po_line_num,
                SUM(rtv_quantity)   rtv_quantity,
                SUM(invoice_amount) invoice_amount
        FROM    XXCUST_PO_AP_INTERFACE_LOG
        WHERE   transaction_class   = 'CREDIT_MEMO'
        AND     interface_status    = 'PROCESSED'
        GROUP BY legacy_po_number, legacy_po_line_num
    ) rtv_log
        ON  rtv_log.legacy_po_number    = inv_log.legacy_po_number
        AND rtv_log.legacy_po_line_num  = inv_log.legacy_po_line_num
ORDER BY balance_status DESC, inv_log.legacy_po_number;


/* ============================================================================
   SECTION 5: EXECUTION GUIDE
   ============================================================================

   INITIAL SETUP (one-time)
   -------------------------
   1. Run DDL in Section 1 to create XXCUST_PO_AP_INTERFACE_LOG and
      XXCUST_COA_MAPPING tables in the new Oracle instance.
      (If upgrading from v1.0, run ALTER TABLE statements instead.)

   2. Create DB link on new instance:
      CREATE DATABASE LINK LEGACY_INSTANCE
        CONNECT TO <legacy_user> IDENTIFIED BY <password>
        USING '<legacy_tns_alias>';

   3. Grant SELECT on legacy tables via DB link to new instance schema user.

   4. Populate XXCUST_COA_MAPPING with all legacy-to-new account mappings:
      INSERT INTO XXCUST_COA_MAPPING (
          legacy_ccid, legacy_account_num,
          new_account_segment1, new_account_segment2,
          new_account_segment3, new_account_segment4, new_ccid)
      VALUES (12345, '01-5500-00000', '100', 'DEPT01', '55000', '0000', 67890);
      COMMIT;


   PROCESS A — Receipt-to-Invoice (Run regularly; e.g. nightly)
   -------------------------------------------------------------
   DECLARE
       l_errbuf   VARCHAR2(2000);
       l_retcode  NUMBER;
   BEGIN
       XXCUST_PO_AP_INTERFACE_PKG.run_receipt_interface(
           p_errbuf            => l_errbuf,
           p_retcode           => l_retcode,
           p_operating_unit    => NULL,            -- NULL = all OUs
           p_receipt_date_from => '01-JAN-2025',
           p_receipt_date_to   => '31-DEC-2025',
           p_po_number         => NULL,            -- NULL = all POs
           p_debug_mode        => 'Y'
       );
       DBMS_OUTPUT.PUT_LINE('Return Code: ' || l_retcode);
       DBMS_OUTPUT.PUT_LINE('Message    : ' || l_errbuf);
   END;
   /

   PROCESS B — RTV-to-Credit-Memo (Run after Process A completes)
   ---------------------------------------------------------------
   DECLARE
       l_errbuf   VARCHAR2(2000);
       l_retcode  NUMBER;
   BEGIN
       XXCUST_PO_AP_INTERFACE_PKG.run_rtv_interface(
           p_errbuf            => l_errbuf,
           p_retcode           => l_retcode,
           p_operating_unit    => NULL,
           p_rtv_date_from     => '01-JAN-2025',
           p_rtv_date_to       => '31-DEC-2025',
           p_po_number         => NULL,
           p_debug_mode        => 'Y'
       );
       DBMS_OUTPUT.PUT_LINE('Return Code: ' || l_retcode);
       DBMS_OUTPUT.PUT_LINE('Message    : ' || l_errbuf);
   END;
   /

   ORACLE AP OPEN INTERFACE IMPORT (run after each Process A or B execution)
   --------------------------------------------------------------------------
   Navigation: Payables Manager > Other > Import > Invoices
     Source              : XXCUST_PO_RECEIPT
     Group ID            : (from interface run output log)
     Hold Unmatched      : No
     Create One Dist/Line: Yes

   RECONCILIATION SEQUENCE
   ------------------------
   1. Run Query 4A (Process A reconciliation) with Run ID from Process A
   2. Run Query 4B (Process B reconciliation) with Run ID from Process B
   3. Run Query 4C (master net balance check) — no Run ID required; covers all runs

   ============================================================================ */
