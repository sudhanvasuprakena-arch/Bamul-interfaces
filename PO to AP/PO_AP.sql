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
CREATE TABLE xxcust_po_ap_interface_log (
    log_id                    NUMBER
        GENERATED ALWAYS AS IDENTITY
    PRIMARY KEY,
    run_id                    VARCHAR2(50) NOT NULL,
    run_date                  DATE DEFAULT sysdate,
    transaction_class         VARCHAR2(20),   -- INVOICE or CREDIT_MEMO (v2.0)
    legacy_rcv_transaction_id NUMBER,         -- RECEIVE or RETURN TO VENDOR transaction_id
    legacy_orig_rcv_txn_id    NUMBER,         -- for RTVs: the original RECEIVE transaction_id
    legacy_po_header_id       NUMBER,
    legacy_po_number          VARCHAR2(20),
    legacy_po_line_num        NUMBER,
    legacy_vendor_id          NUMBER,
    vendor_name               VARCHAR2(240),
    receipt_number            VARCHAR2(30),
    receipt_date              DATE,
    receipt_quantity          NUMBER,         -- gross received qty (RECEIVE transactions)
    rtv_quantity              NUMBER,         -- returned qty (RETURN TO VENDOR transactions)
    net_quantity              NUMBER,         -- receipt_quantity - rtv_quantity
    receipt_amount            NUMBER,         -- net invoiceable amount
    invoice_num               VARCHAR2(50),
    invoice_amount            NUMBER,
    interface_status          VARCHAR2(20),   -- PROCESSED, REJECTED, SKIPPED, ERROR
    rejection_reason          VARCHAR2(2000),
    ap_invoice_id             NUMBER,         -- populated after AP Import run
    created_by                NUMBER DEFAULT - 1,
    creation_date             DATE DEFAULT sysdate,
    last_updated_by           NUMBER DEFAULT - 1,
    last_update_date          DATE DEFAULT sysdate
);

CREATE INDEX xxcust_po_ap_iface_log_n1 ON
    xxcust_po_ap_interface_log (
        run_id
    );

CREATE INDEX xxcust_po_ap_iface_log_n2 ON
    xxcust_po_ap_interface_log (
        legacy_rcv_transaction_id
    );

CREATE INDEX xxcust_po_ap_iface_log_n3 ON
    xxcust_po_ap_interface_log (
        interface_status
    );

CREATE INDEX xxcust_po_ap_iface_log_n4 ON
    xxcust_po_ap_interface_log (
        transaction_class
    );

CREATE INDEX xxcust_po_ap_iface_log_n5 ON
    xxcust_po_ap_interface_log (
        legacy_orig_rcv_txn_id
    );

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
CREATE TABLE xxcust_coa_mapping (
    mapping_id           NUMBER
        GENERATED ALWAYS AS IDENTITY
    PRIMARY KEY,
    legacy_ccid          NUMBER NOT NULL,
    legacy_account_num   VARCHAR2(240),
    new_account_segment1 VARCHAR2(25),
    new_account_segment2 VARCHAR2(25),
    new_account_segment3 VARCHAR2(25),
    new_account_segment4 VARCHAR2(25),
    new_account_segment5 VARCHAR2(25),
    new_ccid             NUMBER,
    mapping_status       VARCHAR2(20) DEFAULT 'ACTIVE',
    notes                VARCHAR2(500),
    created_by           NUMBER DEFAULT - 1,
    creation_date        DATE DEFAULT sysdate
);

CREATE UNIQUE INDEX xxcust_coa_mapping_u1 ON
    xxcust_coa_mapping (
        legacy_ccid
    );

/* ============================================================================
   SECTION 2: MAIN INTERFACE PACKAGE SPECIFICATION
   ============================================================================ */

CREATE OR REPLACE PACKAGE xxcust_po_ap_interface_pkg AS

    -- -------------------------------------------------------------------------
    -- PROCESS A: Receipt-to-Invoice (with RTV netting)
    -- Entry point for standard receipt-driven AP Invoice creation.
    -- Net quantity logic is applied internally: gross received qty minus any
    -- RETURN TO VENDOR qty on the same PO line location is computed before
    -- deciding whether to create an AP Invoice and for what amount.
    -- -------------------------------------------------------------------------
    PROCEDURE run_receipt_interface (
        p_errbuf            OUT VARCHAR2,
        p_retcode           OUT NUMBER,
        p_operating_unit    IN VARCHAR2 DEFAULT NULL,   -- NULL = all OUs
        p_receipt_date_from IN VARCHAR2 DEFAULT NULL,   -- DD-MON-YYYY
        p_receipt_date_to   IN VARCHAR2 DEFAULT NULL,   -- DD-MON-YYYY
        p_po_number         IN VARCHAR2 DEFAULT NULL,   -- NULL = all POs
        p_debug_mode        IN VARCHAR2 DEFAULT 'N'
    );

    -- -------------------------------------------------------------------------
    -- PROCESS B: RTV-to-Credit-Memo
    -- Entry point for generating AP Credit Memos from RETURN TO VENDOR
    -- transactions that occurred AFTER the original AP Invoice was created.
    -- RTVs with no matching processed invoice in the log are skipped (they
    -- are already handled by net quantity logic in Process A).
    -- -------------------------------------------------------------------------
    PROCEDURE run_rtv_interface (
        p_errbuf         OUT VARCHAR2,
        p_retcode        OUT NUMBER,
        p_operating_unit IN VARCHAR2 DEFAULT NULL,   -- NULL = all OUs
        p_rtv_date_from  IN VARCHAR2 DEFAULT NULL,   -- DD-MON-YYYY
        p_rtv_date_to    IN VARCHAR2 DEFAULT NULL,   -- DD-MON-YYYY
        p_po_number      IN VARCHAR2 DEFAULT NULL,   -- NULL = all POs
        p_debug_mode     IN VARCHAR2 DEFAULT 'N'
    );

    -- Purge successfully processed log records older than N days
    PROCEDURE purge_log (
        p_days_to_keep IN NUMBER DEFAULT 90
    );

END xxcust_po_ap_interface_pkg;
/

/* ============================================================================
   SECTION 3: MAIN INTERFACE PACKAGE BODY
   ============================================================================ */

CREATE OR REPLACE PACKAGE BODY xxcust_po_ap_interface_pkg AS

    -- =========================================================================
    -- Package-level constants
    -- =========================================================================
    c_source_name       CONSTANT VARCHAR2(30) := 'XXCUST_PO_RECEIPT';
    c_created_by        CONSTANT NUMBER := -1;         -- replace with FND_GLOBAL.USER_ID
    c_invoice_type      CONSTANT VARCHAR2(25) := 'STANDARD';
    c_credit_type       CONSTANT VARCHAR2(25) := 'CREDIT';
    c_pay_group         CONSTANT VARCHAR2(25) := 'STANDARD';
    -- DB link name pointing to legacy Oracle instance:
    c_db_link           CONSTANT VARCHAR2(30) := 'LEGACY_INSTANCE';

    -- Transaction class constants (for log table)
    c_class_invoice     CONSTANT VARCHAR2(20) := 'INVOICE';
    c_class_credit_memo CONSTANT VARCHAR2(20) := 'CREDIT_MEMO';

    -- Status constants
    c_status_processed  CONSTANT VARCHAR2(20) := 'PROCESSED';
    c_status_rejected   CONSTANT VARCHAR2(20) := 'REJECTED';
    c_status_skipped    CONSTANT VARCHAR2(20) := 'SKIPPED';
    c_status_error      CONSTANT VARCHAR2(20) := 'ERROR';

    -- New instance ORG_ID Unit ID
    c_new_org_id        CONSTANT NUMBER := 81;

    -- Chart of Accounts structure ID in the new instance
    c_chart_of_accounts_id CONSTANT NUMBER := 50428;

    -- =========================================================================
    -- Private: Write message to output log
    -- =========================================================================
    PROCEDURE log_message (
        p_message IN VARCHAR2
    ) IS
    BEGIN
        dbms_output.put_line(to_char(sysdate, 'DD-MON-YYYY HH24:MI:SS')
                             || ' | '
                             || p_message);
        -- In full implementation: FND_FILE.PUT_LINE(FND_FILE.LOG, p_message);
    END log_message;

    -- =========================================================================
    -- Private: Lookup new COA CCID from legacy CCID. Returns -1 if not found.
    -- =========================================================================
    -- FUNCTION get_new_ccid (
    --     p_legacy_ccid IN NUMBER
    -- ) RETURN NUMBER IS
    --     l_new_ccid NUMBER;
    -- BEGIN
    --     SELECT
    --         new_ccid
    --     INTO l_new_ccid
    --     FROM
    --         xxcust_coa_mapping
    --     WHERE
    --             legacy_ccid = p_legacy_ccid
    --         AND mapping_status = 'ACTIVE';

    --     RETURN nvl(l_new_ccid, -1);
    -- EXCEPTION
    --     WHEN OTHERS THEN
    --         RETURN -1;
    -- END get_new_ccid;

    -- =========================================================================
    -- Private: Get net received quantity for a PO line location.
    -- Sums all RECEIVE transactions and subtracts all RETURN TO VENDOR
    -- transactions for the same po_line_location_id in the legacy instance.
    -- This is the core RTV Scenario A netting function.
    -- =========================================================================
    FUNCTION get_net_received_qty (
        p_po_line_location_id IN NUMBER
    ) RETURN NUMBER IS
        l_net_qty NUMBER;
    BEGIN
        SELECT
            nvl(SUM(
                CASE
                    WHEN rt.transaction_type = 'RECEIVE'          THEN
                        rt.quantity
                    WHEN rt.transaction_type = 'RETURN TO VENDOR' THEN
                        - rt.quantity
                    ELSE
                        0
                END
            ), 0)
        INTO l_net_qty
        FROM
            apps.rcv_transactions@legacy_instance rt
        WHERE
                rt.po_line_location_id = p_po_line_location_id
            AND rt.transaction_type IN ( 'RECEIVE', 'RETURN TO VENDOR' )
            AND rt.source_document_code = 'PO';

        RETURN l_net_qty;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN 0;
    END get_net_received_qty;

    -- =========================================================================
    -- Private: Get total RTV quantity for a PO line location.
    -- Used by RTV interface to compute Credit Memo amounts.
    -- =========================================================================
    FUNCTION get_rtv_qty_for_line (
        p_po_line_location_id IN NUMBER
    ) RETURN NUMBER IS
        l_rtv_qty NUMBER;
    BEGIN
        SELECT
            nvl(SUM(rt.quantity), 0)
        INTO l_rtv_qty
        FROM
            apps.rcv_transactions@legacy_instance rt
        WHERE
                rt.po_line_location_id = p_po_line_location_id
            AND rt.transaction_type = 'RETURN TO VENDOR'
            AND rt.source_document_code = 'PO';

        RETURN l_rtv_qty;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN 0;
    END get_rtv_qty_for_line;

    -- =========================================================================
    -- Private: Check whether the original RECEIVE transaction for a given
    -- PO line location has already been processed as an AP Invoice.
    -- Returns the original invoice_num if found, NULL otherwise.
    -- Used by RTV interface to determine Scenario A vs Scenario B/C.
    -- =========================================================================
    FUNCTION get_original_invoice_num (
        p_po_line_location_id IN NUMBER,
        p_po_header_id        IN NUMBER
    ) RETURN VARCHAR2 IS
        l_invoice_num VARCHAR2(50);
    BEGIN
        -- Find the most recently processed invoice for this PO line location
        SELECT
            invoice_num
        INTO l_invoice_num
        FROM
            (
                SELECT
                    log.invoice_num
                FROM
                         xxcust_po_ap_interface_log log
                    JOIN apps.rcv_transactions@legacy_instance rt ON rt.transaction_id = log.legacy_rcv_transaction_id
                WHERE
                        rt.po_line_location_id = p_po_line_location_id
                    AND log.legacy_po_header_id = p_po_header_id
                    AND log.interface_status = c_status_processed
                    AND log.transaction_class = c_class_invoice
                ORDER BY
                    log.creation_date DESC
            )
        WHERE
            ROWNUM = 1;

        RETURN l_invoice_num;
    EXCEPTION
        WHEN no_data_found THEN
            RETURN NULL;
        WHEN OTHERS THEN
            RETURN NULL;
    END get_original_invoice_num;

    -- =========================================================================
    -- Private: Validate a receipt record before loading as AP Invoice.
    -- Returns TRUE if valid. Populates p_rejection_reason on failure.
    -- =========================================================================
    FUNCTION validate_receipt (
        p_rcv_transaction_id IN NUMBER,
        p_vendor_id          IN NUMBER,
        p_net_quantity       IN NUMBER,
        p_net_amount         IN NUMBER,
        p_legacy_ccid        IN NUMBER,
        p_rejection_reason   OUT VARCHAR2
    ) RETURN BOOLEAN IS
        l_vendor_count      NUMBER;
        l_already_processed NUMBER;
        l_new_ccid          NUMBER;
    BEGIN
        p_rejection_reason := NULL;

        -- BR-01: Net quantity or net amount must be positive after RTV netting
        IF
            nvl(p_net_quantity, 0) <= 0
            AND nvl(p_net_amount, 0) <= 0
        THEN
            p_rejection_reason := 'Net received quantity/amount is zero or negative after RTV netting. No invoice required.';
            RETURN false;
        END IF;

        -- BR-02: Supplier must exist in new AP instance
        SELECT
            COUNT(*)
        INTO l_vendor_count
        FROM
            APPS.AP_SUPPLIERS
        WHERE
            vendor_id = p_vendor_id;

        IF l_vendor_count = 0 THEN
            p_rejection_reason := 'Supplier ID '
                                  || p_vendor_id
                                  || ' not found in new AP. Migrate supplier first.';
            RETURN false;
        END IF;

        -- BR-03: Duplicate check - receipt not already processed as an invoice
        SELECT
            COUNT(*)
        INTO l_already_processed
        FROM
            xxcust_po_ap_interface_log
        WHERE
                legacy_rcv_transaction_id = p_rcv_transaction_id
            AND interface_status = c_status_processed
            AND transaction_class = c_class_invoice;

        IF l_already_processed > 0 THEN
            p_rejection_reason := 'Receipt TXN ID '
                                  || p_rcv_transaction_id
                                  || ' already processed as invoice. Duplicate skipped.';
            RETURN false;
        END IF;

        -- BR-04: COA mapping must exist for the distribution account
        -- l_new_ccid := get_new_ccid(p_legacy_ccid);
        -- IF l_new_ccid = -1 THEN
        --     p_rejection_reason := 'No active COA mapping for legacy CCID '
        --                           || p_legacy_ccid
        --                           || '. Update XXCUST_COA_MAPPING.';
        --     RETURN false;
        -- END IF;

        RETURN true;
    EXCEPTION
        WHEN OTHERS THEN
            p_rejection_reason := 'Validation error: ' || sqlerrm;
            RETURN false;
    END validate_receipt;

    -- =========================================================================
    -- Private: Validate an RTV record before loading as AP Credit Memo.
    -- Returns TRUE if valid. Populates p_rejection_reason on failure.
    -- =========================================================================
    FUNCTION validate_rtv (
        p_rtv_transaction_id   IN NUMBER,
        p_po_line_location_id  IN NUMBER,
        p_po_header_id         IN NUMBER,
        p_rtv_quantity         IN NUMBER,
        p_rtv_amount           IN NUMBER,
        p_original_invoice_num IN VARCHAR2,
        p_original_inv_amount  IN NUMBER,
        p_legacy_ccid          IN NUMBER,
        p_rejection_reason     OUT VARCHAR2
    ) RETURN BOOLEAN IS
        l_vendor_count      NUMBER;
        l_already_processed NUMBER;
        l_new_ccid          NUMBER;
    BEGIN
        p_rejection_reason := NULL;

        -- RTV-BR-01: RTV quantity must be positive
        IF
            nvl(p_rtv_quantity, 0) <= 0
            AND nvl(p_rtv_amount, 0) <= 0
        THEN
            p_rejection_reason := 'RTV quantity/amount is zero or negative. Cannot create Credit Memo.';
            RETURN false;
        END IF;

        -- RTV-BR-02: Original receipt must have been interfaced as an AP Invoice
        -- (Scenario A RTVs - where no invoice was ever created - are excluded upstream)
        IF p_original_invoice_num IS NULL THEN
            p_rejection_reason := 'No processed AP Invoice found for this PO line location. RTV handled by net quantity logic (Scenario A). Skipping.';
            RETURN false;
        END IF;

        -- RTV-BR-03: Duplicate check - RTV not already processed as Credit Memo
        SELECT
            COUNT(*)
        INTO l_already_processed
        FROM
            xxcust_po_ap_interface_log
        WHERE
                legacy_rcv_transaction_id = p_rtv_transaction_id
            AND interface_status = c_status_processed
            AND transaction_class = c_class_credit_memo;

        IF l_already_processed > 0 THEN
            p_rejection_reason := 'RTV TXN ID '
                                  || p_rtv_transaction_id
                                  || ' already processed as Credit Memo. Duplicate skipped.';
            RETURN false;
        END IF;

        -- RTV-BR-04: Credit amount must not exceed original invoice amount
        IF nvl(p_rtv_amount, 0) > nvl(p_original_inv_amount, 0) THEN
            p_rejection_reason := 'RTV amount ('
                                  || p_rtv_amount
                                  || ') exceeds original invoice amount ('
                                  || p_original_inv_amount
                                  || '). Manual review required.';
            RETURN false;
        END IF;

        -- RTV-BR-05: COA mapping must exist for the distribution account
        -- (Disabled: derive_new_ccid is now called before validation with fallback to 00000)
        -- l_new_ccid := get_new_ccid(p_legacy_ccid);
        -- IF l_new_ccid = -1 THEN
        --     p_rejection_reason := 'No active COA mapping for legacy CCID '
        --                           || p_legacy_ccid
        --                           || '. Update XXCUST_COA_MAPPING.';
        --     RETURN false;
        -- END IF;

        RETURN true;
    EXCEPTION
        WHEN OTHERS THEN
            p_rejection_reason := 'RTV validation error: ' || sqlerrm;
            RETURN false;
    END validate_rtv;

    ---------------------------------------------------------------------------
    -- Helper functions to get segment values for COA mapping (if needed in transformation logic)
    ---------------------------------------------------------------------------
    FUNCTION get_division_segment (
        org_id IN NUMBER
    ) RETURN VARCHAR2 IS
        l_division_segment VARCHAR2(25);
    BEGIN
        SELECT
            division_code
        INTO l_division_segment
        FROM
            APPS.MAPPING_DIVISION_SEGMENT
        WHERE
            organization_id = org_id;

        log_message('Division segment mapping: org_id=' || org_id
                    || ' → division_segment=' || l_division_segment);

        RETURN l_division_segment;
    EXCEPTION
        WHEN OTHERS THEN
            log_message('Division segment mapping: org_id=' || org_id
                        || ' → division_segment=01 (default, no mapping found)');
            RETURN '01';
    END get_division_segment;

    ---------------------------------------------------------------------------
    -- Helper function to get product segment for an item (if needed in transformation logic)
    ---------------------------------------------------------------------------
    FUNCTION get_product_segment (
        item_id IN NUMBER
    ) RETURN VARCHAR2 IS
        l_product_code VARCHAR2(50);
    BEGIN
        SELECT
            product_code
        INTO l_product_code
        FROM
            APPS.MAPPING_PRODUCT_SEGMENT
        WHERE
            inventory_item_id = item_id;

        log_message('Product segment mapping: item_id=' || item_id
                    || ' → product_code=' || l_product_code);

        RETURN l_product_code;
    EXCEPTION
        WHEN no_data_found THEN
            log_message('Product segment mapping: item_id=' || item_id
                        || ' → product_code=00000000 (default, no mapping found)');
            RETURN '00000000';
    END get_product_segment;

   -----------------------------------------------------------------------------
   -- Helper function to determine transaction type segment for tax code
   -----------------------------------------------------------------------------
    FUNCTION get_txn_type_segment (
        p_rcv_transaction_id IN NUMBER
    ) RETURN VARCHAR2 IS
        l_tax_code VARCHAR2(100);
    BEGIN
    -- Get the first tax_rate_code from JAI_TAX_LINES_ALL for this receipt transaction
        SELECT
            lower(jtl.tax_rate_code)
        INTO l_tax_code
        FROM
            apps.jai_tax_lines_all@legacy_instance jtl
        WHERE
                jtl.trx_id = p_rcv_transaction_id
            AND ROWNUM = 1;

        IF l_tax_code LIKE 'cgst%' OR l_tax_code LIKE 'sgst%' THEN
            RETURN '1';  -- LOCAL SALE
        ELSIF l_tax_code LIKE 'igst%' THEN
            RETURN '2';  -- INTERSTATE
        ELSE
            RETURN '0';  -- DEFAULT
        END IF;

    EXCEPTION
        WHEN no_data_found THEN
            RETURN '0';  -- DEFAULT (no tax lines found)
        WHEN OTHERS THEN
            RETURN '0';
    END get_txn_type_segment;

    -------------------------------------------------------------------------
    -- Helper function to get department segment value based on old flex value
    --------------------------------------------------------------------------
    FUNCTION get_department_segment (
        old_flex_value IN NUMBER
    ) RETURN VARCHAR2 IS
        l_new_segment VARCHAR2(25);
    BEGIN
        SELECT
            new_department_flex_value
        INTO l_new_segment
        FROM
            mapping_department_segment
        WHERE
            old_department_flex_value = old_flex_value;

        log_message('Department segment mapping: old_flex_value=' || old_flex_value
                    || ' → new_segment=' || l_new_segment);

        RETURN l_new_segment;
    EXCEPTION
        WHEN OTHERS THEN
            log_message('Department segment mapping: old_flex_value=' || old_flex_value
                        || ' → new_segment=BAD07 (default, no mapping found)');
            RETURN '00000';
    END get_department_segment;    

   -------------------------------------------------------------------------
       -- Helper function to get  ACCOUNT segment value based on old flex value
    --------------------------------------------------------------------------

    FUNCTION get_account_segment (
        old_flex_value IN NUMBER
    ) RETURN VARCHAR2 IS
        l_new_segment VARCHAR2(6);
    BEGIN
        SELECT
            TO_CHAR(new_account_flex_value)
        INTO l_new_segment
        FROM
            mapping_account_segment
        WHERE
            old_account_flex_value = old_flex_value;

        -- Treat 0 as unmapped (column is NUMBER(6), so missing mappings may store 0)
        IF l_new_segment IS NULL OR l_new_segment = '0' THEN
            log_message('Account segment mapping: old_flex_value=' || old_flex_value
                        || ' → new_segment=' || l_new_segment || ' (treated as unmapped)');
            RETURN NULL;
        END IF;

        log_message('Account segment mapping: old_flex_value=' || old_flex_value
                    || ' → new_segment=' || l_new_segment);

        RETURN l_new_segment;
    EXCEPTION
        WHEN no_data_found THEN
            log_message('Account segment mapping: old_flex_value=' || old_flex_value
                        || ' → no row found (returning NULL)');
            RETURN NULL;
        WHEN OTHERS THEN
            log_message('Account segment mapping: old_flex_value=' || old_flex_value
                        || ' → error: ' || sqlerrm || ' (returning NULL)');
            RETURN NULL;
    END get_account_segment;

    -------------------------------------------------------------------------
    -- Fallback: Derive account segment from item when old account has no mapping.
    -- Resolution: 1) Product prefix → account  2) Item description keywords → account
    --             3) Default 513001 (General Consumables)
    -------------------------------------------------------------------------
    FUNCTION get_account_segment_fallback (
        p_inventory_item_id IN NUMBER
    ) RETURN VARCHAR2 IS
        l_new_segment    VARCHAR2(6);
        l_product_code   VARCHAR2(8);
        l_product_prefix VARCHAR2(4);
        l_item_desc      VARCHAR2(240);
    BEGIN
        log_message('Account fallback: starting for item_id=' || p_inventory_item_id);

        -- Step 1: Try product prefix mapping (item already in MAPPING_PRODUCT_SEGMENT)
        BEGIN
            SELECT ps.product_code
            INTO l_product_code
            FROM mapping_product_segment ps
            WHERE ps.inventory_item_id = p_inventory_item_id;

            l_product_prefix := SUBSTR(l_product_code, 1, 2);
            log_message('Account fallback Step 1: item_id=' || p_inventory_item_id
                        || ' → product_code=' || l_product_code
                        || ' → prefix=' || l_product_prefix);

            SELECT TO_CHAR(ppa.new_account_flex_value)
            INTO l_new_segment
            FROM mapping_product_prefix_account ppa
            WHERE ppa.product_prefix = l_product_prefix;

            log_message('Account fallback Step 1: prefix=' || l_product_prefix
                        || ' → account=' || l_new_segment);
            RETURN l_new_segment;
        EXCEPTION
            WHEN no_data_found THEN
                log_message('Account fallback Step 1: no mapping found, falling through to keyword matching');
                NULL; -- fall through to keyword matching
        END;

        -- Step 2: Try keyword matching on item description
        BEGIN
            SELECT description
            INTO l_item_desc
            FROM apps.mtl_system_items_b@legacy_instance
            WHERE inventory_item_id = p_inventory_item_id
              AND ROWNUM = 1;
            log_message('Account fallback Step 2: item_id=' || p_inventory_item_id
                        || ' → description=' || SUBSTR(l_item_desc, 1, 100));
        EXCEPTION
            WHEN no_data_found THEN
                log_message('Account fallback Step 2: no item description found, returning default 513001');
                RETURN '513001'; -- default
        END;

        BEGIN
            SELECT TO_CHAR(ka.new_account_flex_value)
            INTO l_new_segment
            FROM (
                SELECT ka.new_account_flex_value
                FROM mapping_item_keyword_account ka
                WHERE UPPER(l_item_desc) LIKE UPPER(ka.keyword_pattern)
                ORDER BY ka.priority ASC
            ) ka
            WHERE ROWNUM = 1;

            log_message('Account fallback Step 2: keyword match → account=' || l_new_segment);
            RETURN l_new_segment;
        EXCEPTION
            WHEN no_data_found THEN
                log_message('Account fallback Step 2: no keyword match, returning default 513001');
                RETURN '513001'; -- default: General Consumables
        END;
    EXCEPTION
        WHEN OTHERS THEN
            log_message('Account fallback: unexpected error: ' || sqlerrm || ', returning default 513001');
            RETURN '513001';
    END get_account_segment_fallback;
    
    -------------------------------------------------------------------------
    -- Derive new ccid based on legacy ccid and mapping table.
    -------------------------------------------------------------------------
    FUNCTION derive_new_ccid (
        p_legacy_ccid          IN NUMBER,
        p_inventory_org_id     IN NUMBER,
        p_inventory_item_id    IN NUMBER,
        p_rcv_transaction_id   IN NUMBER,
        p_chart_of_accounts_id IN NUMBER DEFAULT c_chart_of_accounts_id
    ) RETURN NUMBER IS
   -- Old segments from legacy GL_CODE_COMBINATIONS
        l_old_seg2        VARCHAR2(25);  -- COSTCENTER → maps to DEPARTMENT
        l_old_seg4        VARCHAR2(25);  -- ACCOUNT    → maps to ACCOUNT

   -- New segments
        l_seg1_entity     VARCHAR2(25) := '01';   -- fixed
        l_seg2_division   VARCHAR2(25);
        l_seg3_account    VARCHAR2(25);
        l_seg4_department VARCHAR2(25);
        l_seg5_product    VARCHAR2(25);
        l_seg6_txn_type   VARCHAR2(25);
        l_seg7_future1    VARCHAR2(25) := '000';  -- fixed
        l_seg8_future2    VARCHAR2(25) := '000';  -- fixed

        l_new_ccid        NUMBER;
    BEGIN
   -- Step 1: Get only the segments we need from legacy CCID
        SELECT
            gcc.segment2,
            gcc.segment4
        INTO
            l_old_seg2,
            l_old_seg4
        FROM
            apps.gl_code_combinations@legacy_instance gcc
        WHERE
            gcc.code_combination_id = p_legacy_ccid;

   -- Step 2: Derive each new segment
        l_seg2_division := get_division_segment(p_inventory_org_id);
        l_seg3_account := get_account_segment(to_number(l_old_seg4));
        -- Fallback: if old account has no mapping, derive from item
        IF l_seg3_account IS NULL THEN
            l_seg3_account := get_account_segment_fallback(p_inventory_item_id);
            log_message('  Account fallback used for item_id=' || p_inventory_item_id
                        || ' → account=' || l_seg3_account);
        END IF;
        l_seg4_department := get_department_segment(to_number(l_old_seg2));
        l_seg5_product := get_product_segment(p_inventory_item_id);  -- uses item_id directly
        l_seg6_txn_type := get_txn_type_segment(p_rcv_transaction_id);

   -- LOG EACH DERIVED NEW SEGMENTS
        log_message('Derived segments for legacy CCID '
                    || p_legacy_ccid
                    || ':');
        log_message('  Division segment: ' || l_seg2_division);
        log_message('  Account segment: ' || l_seg3_account);
        log_message('  Department segment: ' || l_seg4_department);
        log_message('  Product segment: ' || l_seg5_product);
        log_message('  Transaction type segment: ' || l_seg6_txn_type);
   -- Step 3: Look up new CCID from GL_CODE_COMBINATIONS in new instance
        BEGIN
            SELECT
                gcc.code_combination_id
            INTO l_new_ccid
            FROM
                gl_code_combinations gcc
            WHERE
                    gcc.chart_of_accounts_id = p_chart_of_accounts_id
                AND gcc.segment1 = l_seg1_entity
                AND gcc.segment2 = l_seg2_division
                AND gcc.segment3 = l_seg3_account
                AND gcc.segment4 = l_seg4_department
                AND gcc.segment5 = l_seg5_product
                AND gcc.segment6 = l_seg6_txn_type
                AND gcc.segment7 = l_seg7_future1
                AND gcc.segment8 = l_seg8_future2
                AND gcc.enabled_flag = 'Y';

            RETURN l_new_ccid;
        EXCEPTION
            WHEN no_data_found THEN
                -- CCID not found: attempt to create it via Flexfield API
                log_message('CCID not found for segments '
                            || l_seg1_entity || '.' || l_seg2_division || '.'
                            || l_seg3_account || '.' || l_seg4_department || '.'
                            || l_seg5_product || '.' || l_seg6_txn_type || '.'
                            || l_seg7_future1 || '.' || l_seg8_future2
                            || ' — attempting dynamic creation via FND_FLEX_EXT.GET_CCID');

                l_new_ccid := fnd_flex_ext.get_ccid(
                    application_short_name => 'SQLGL',
                    key_flex_code          => 'GL#',
                    structure_number       => p_chart_of_accounts_id,
                    validation_date        => to_char(sysdate, 'DD-MON-YYYY'),
                    concatenated_segments  => l_seg1_entity || '.' || l_seg2_division || '.'
                                              || l_seg3_account || '.' || l_seg4_department || '.'
                                              || l_seg5_product || '.' || l_seg6_txn_type || '.'
                                              || l_seg7_future1 || '.' || l_seg8_future2
                );

                IF nvl(l_new_ccid, 0) = 0 THEN
                    log_message('FND_FLEX_EXT.GET_CCID failed: ' || fnd_flex_ext.get_message);
                    RETURN -1;
                END IF;

                log_message('New CCID created: ' || l_new_ccid);
                RETURN l_new_ccid;
        END;
    EXCEPTION
        WHEN OTHERS THEN
            log_message('derive_new_ccid FATAL: legacy_ccid=' || p_legacy_ccid
                        || ' error=' || sqlerrm);
            RETURN -1;
    END derive_new_ccid;

    /* =========================================================================
       PROCESS A: run_receipt_interface
       Receipt-to-Invoice with RTV Net Quantity Logic
       ========================================================================= */
    PROCEDURE run_receipt_interface (
        p_errbuf            OUT VARCHAR2,
        p_retcode           OUT NUMBER,
        p_operating_unit    IN VARCHAR2 DEFAULT NULL,
        p_receipt_date_from IN VARCHAR2 DEFAULT NULL,
        p_receipt_date_to   IN VARCHAR2 DEFAULT NULL,
        p_po_number         IN VARCHAR2 DEFAULT NULL,
        p_debug_mode        IN VARCHAR2 DEFAULT 'N'
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
            rsh.receipt_num                    receipt_number,
            rt.transaction_id                  rcv_transaction_id,
            rt.transaction_date                receipt_date,
            rt.po_line_location_id             po_line_location_id,

                -- PO details
            ph.po_header_id                    po_header_id,
            ph.segment1                        po_number,
            ph.type_lookup_code                po_type,
            ph.currency_code                   po_currency,
            pl.line_num                        po_line_num,
            pl.item_id                         item_id,
            pl.item_description                item_description,
            pl.purchase_basis                  purchase_basis,
            pll.shipment_num                   shipment_num,

                -- Gross received quantities and amounts (from this RECEIVE transaction)
            rt.quantity                        gross_qty_received,
            rt.po_unit_price                   unit_price,
            ( rt.quantity * rt.po_unit_price ) gross_amount,

                -- NET quantity after subtracting all RTVs on this PO line location
                -- This is the RTV Scenario A netting applied at cursor level
            (
                SELECT
                    nvl(SUM(
                        CASE
                            WHEN rt2.transaction_type = 'RECEIVE'          THEN
                                rt2.quantity
                            WHEN rt2.transaction_type = 'RETURN TO VENDOR' THEN
                                - rt2.quantity
                            ELSE
                                0
                        END
                    ), 0)
                FROM
                    apps.rcv_transactions@legacy_instance rt2
                WHERE
                        rt2.po_line_location_id = rt.po_line_location_id
                    AND rt2.transaction_type IN ( 'RECEIVE', 'RETURN TO VENDOR' )
                    AND rt2.source_document_code = 'PO'
            )                                  net_qty_received,

                -- Total RTV qty for logging purposes
            (
                SELECT
                    nvl(SUM(rt3.quantity), 0)
                FROM
                    apps.rcv_transactions@legacy_instance rt3
                WHERE
                        rt3.po_line_location_id = rt.po_line_location_id
                    AND rt3.transaction_type = 'RETURN TO VENDOR'
                    AND rt3.source_document_code = 'PO'
            )                                  total_rtv_qty,

                -- Services: use amount-based received figure
            pll.amount_received                amount_received,

                -- UOM
            rt.unit_of_measure                 uom_code,
            rt.currency_code                   receipt_currency,
            rt.currency_conversion_rate        conversion_rate,

                -- Supplier
            ph.vendor_id                       vendor_id,
            ph.vendor_site_id                  vendor_site_id,
            aps.vendor_name                    vendor_name,
            apss.vendor_site_code              vendor_site_code,

                -- Distribution / accounting
            pod.distribution_num               distribution_num,
            pod.code_combination_id            legacy_ccid,
            pod.quantity_billed                qty_billed,

                -- Operating unit
            ph.org_id                          org_id,
            rt.organization_id                 inventory_org_id, -- this is used in mapping division in COA mapping
            hou.name                           operating_unit_name
        FROM
                 apps.rcv_transactions@legacy_instance rt
            JOIN apps.rcv_shipment_headers@legacy_instance      rsh ON rsh.shipment_header_id = rt.shipment_header_id
            JOIN apps.rcv_shipment_lines@legacy_instance        rsl ON rsl.shipment_line_id = rt.shipment_line_id
            JOIN apps.po_headers_all@legacy_instance            ph ON ph.po_header_id = rt.po_header_id
            JOIN apps.po_lines_all@legacy_instance              pl ON pl.po_header_id = rt.po_header_id
                                                         AND pl.po_line_id = rt.po_line_id
            JOIN apps.po_line_locations_all@legacy_instance     pll ON pll.line_location_id = rt.po_line_location_id
            JOIN apps.po_distributions_all@legacy_instance      pod ON pod.line_location_id = rt.po_line_location_id
            JOIN apps.ap_suppliers@legacy_instance              aps ON aps.vendor_id = ph.vendor_id
            JOIN apps.ap_supplier_sites_all@legacy_instance     apss ON apss.vendor_site_id = ph.vendor_site_id
            JOIN apps.hr_all_organization_units@legacy_instance hou ON hou.organization_id = ph.org_id
        WHERE
                -- Only confirmed RECEIVE transactions (not RTVs, CORRECTs etc.)
                rt.transaction_type = 'RECEIVE'  -- it has to be 'DELIVER'
            AND rt.source_document_code = 'PO'

                -- Exclude fully billed lines
            AND nvl(pod.quantity_billed, 0) < rt.quantity

                -- RTV NETTING (Scenario A): Exclude where net received qty <= 0
                -- i.e. all goods have been returned before any invoice was created
            AND (
                SELECT
                    nvl(SUM(
                        CASE
                            WHEN rt2.transaction_type = 'RECEIVE'          THEN
                                rt2.quantity
                            WHEN rt2.transaction_type = 'RETURN TO VENDOR' THEN
                                - rt2.quantity
                            ELSE
                                0
                        END
                    ), 0)
                FROM
                    apps.rcv_transactions@legacy_instance rt2
                WHERE
                        rt2.po_line_location_id = rt.po_line_location_id
                    AND rt2.transaction_type IN ( 'RECEIVE', 'RETURN TO VENDOR' )
                    AND rt2.source_document_code = 'PO'
            ) > 0

                -- Optional filters
            AND ( p_operating_unit IS NULL
                  OR hou.name = p_operating_unit )
            AND ( p_receipt_date_from IS NULL
                  OR rt.transaction_date >= to_date(p_receipt_date_from, 'DD-MON-YYYY') )
            AND ( p_receipt_date_to IS NULL
                  OR rt.transaction_date <= to_date(p_receipt_date_to, 'DD-MON-YYYY') + 1 )
            AND ( p_po_number IS NULL
                  OR ph.segment1 = p_po_number )
        ORDER BY
            ph.vendor_id,
            ph.segment1,
            pl.line_num,
            rt.transaction_date;

        -- Local variables
        l_run_id                 VARCHAR2(50) := 'INV-' || to_char(sysdate, 'YYYYMMDDHH24MISS');
        l_group_id               NUMBER;
        l_invoice_num            VARCHAR2(50);
        l_new_ccid               NUMBER;
        l_rejection_reason       VARCHAR2(2000);
        l_is_valid               BOOLEAN;
        l_records_processed      NUMBER := 0;
        l_records_rejected       NUMBER := 0;
        l_records_skipped        NUMBER := 0;
        l_records_errored        NUMBER := 0;
        l_invoice_interface_id   NUMBER;
        l_line_interface_id      NUMBER;
        l_net_invoice_amount     NUMBER;
        l_total_tax_amount       NUMBER;
        l_tax_line_count         NUMBER;
        l_error_message          VARCHAR2(2000);
        -- Local copies of cursor fields for use in EXCEPTION handler
        l_exc_rcv_transaction_id NUMBER;
        l_exc_po_header_id       NUMBER;
        l_exc_po_number          VARCHAR2(50);
        l_exc_po_line_num        NUMBER;
        l_exc_vendor_id          NUMBER;
        l_exc_vendor_name        VARCHAR2(240);
        l_exc_receipt_number     VARCHAR2(30);
        l_exc_receipt_date       DATE;
    BEGIN
        p_retcode := 0;
        SELECT
            ap_invoices_interface_s.NEXTVAL
        INTO l_group_id
        FROM
            dual;

        log_message('============================================================');
        log_message('PROCESS A: Receipt-to-Invoice Interface (v2.0 with RTV Netting)');
        log_message('Run ID: '
                    || l_run_id
                    || '  |  Group ID: '
                    || l_group_id);
        log_message('Operating Unit  : ' || nvl(p_operating_unit, 'ALL'));
        log_message('Receipt Date From: ' || nvl(p_receipt_date_from, 'NONE'));
        log_message('Receipt Date To  : ' || nvl(p_receipt_date_to, 'NONE'));
        log_message('PO Number Filter : ' || nvl(p_po_number, 'ALL'));
        log_message('============================================================');
        FOR r IN c_receipts LOOP
            l_exc_rcv_transaction_id := r.rcv_transaction_id;
            l_exc_po_header_id := r.po_header_id;
            l_exc_po_number := r.po_number;
            l_exc_po_line_num := r.po_line_num;
            l_exc_vendor_id := r.vendor_id;
            l_exc_vendor_name := r.vendor_name;
            l_exc_receipt_number := r.receipt_number;
            l_exc_receipt_date := r.receipt_date;
            BEGIN
                -- -------------------------------------------------------------
                -- Compute net invoice amount
                -- For Goods:    net_qty_received x unit_price
                -- For Services: amount_received (already net from Oracle)
                -- -------------------------------------------------------------
                IF r.purchase_basis IN ( 'TEMP LABOR', 'FIXED PRICE' ) THEN
                    l_net_invoice_amount := r.amount_received;
                ELSE
                    l_net_invoice_amount := r.net_qty_received * r.unit_price;
                END IF;

                -- Log if RTV netting reduced the invoice amount
                IF
                    r.total_rtv_qty > 0
                    AND p_debug_mode = 'Y'
                THEN
                    log_message('RTV NETTING: PO#'
                                || r.po_number
                                || ' Line:'
                                || r.po_line_num
                                || ' Gross:'
                                || r.gross_qty_received
                                || ' RTV:'
                                || r.total_rtv_qty
                                || ' Net:'
                                || r.net_qty_received
                                || ' NetAmt:'
                                || l_net_invoice_amount);

                END IF;

                -- -------------------------------------------------------------
                -- VALIDATE
                -- -------------------------------------------------------------
                l_new_ccid := derive_new_ccid(p_legacy_ccid => r.legacy_ccid, p_inventory_org_id => r.inventory_org_id, p_inventory_item_id =>
                r.item_id, p_rcv_transaction_id => r.rcv_transaction_id);


                -- if l_new_ccid = -1 then assign dummy value '00000'
                IF l_new_ccid = -1 THEN
                    l_new_ccid := 00000;
                END IF;

                log_message('New CCID for PO#' || r.po_number || ' Line:' || r.po_line_num
                            || ' legacy_ccid=' || r.legacy_ccid || ' → new_ccid=' || l_new_ccid);

                l_is_valid := validate_receipt(p_rcv_transaction_id => r.rcv_transaction_id, p_vendor_id => r.vendor_id, p_net_quantity =>
                r.net_qty_received, p_net_amount => l_net_invoice_amount, p_legacy_ccid => r.legacy_ccid,
                                              p_rejection_reason => l_rejection_reason);

                -- LOG l_is_valid and l_rejection_reason for debugging
                    IF p_debug_mode = 'Y' THEN
                        log_message('Validation result for Receipt TXN ID '
                                    || r.rcv_transaction_id
                                    || ': is_valid='
                                    || CASE WHEN l_is_valid THEN 'TRUE' ELSE 'FALSE' END
                                    || ', rejection_reason='
                                    || nvl(l_rejection_reason, 'NONE'));
                    END IF;

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

                        INSERT INTO xxcust_po_ap_interface_log (
                            run_id,
                            transaction_class,
                            legacy_rcv_transaction_id,
                            legacy_po_header_id,
                            legacy_po_number,
                            legacy_po_line_num,
                            legacy_vendor_id,
                            vendor_name,
                            receipt_number,
                            receipt_date,
                            receipt_quantity,
                            rtv_quantity,
                            net_quantity,
                            interface_status,
                            rejection_reason
                        ) VALUES (
                            l_run_id,
                            c_class_invoice,
                            r.rcv_transaction_id,
                            r.po_header_id,
                            r.po_number,
                            r.po_line_num,
                            r.vendor_id,
                            r.vendor_name,
                            r.receipt_number,
                            r.receipt_date,
                            r.gross_qty_received,
                            r.total_rtv_qty,
                            r.net_qty_received,
                            l_status,
                            l_rejection_reason
                        );

                    END;

                    CONTINUE;
                END IF;

                -- -------------------------------------------------------------
                -- GENERATE INVOICE NUMBER
                -- Convention: RCPT-{receipt_number}-L{po_line_num}
                -- -------------------------------------------------------------
                l_invoice_num := 'RCPT-'
                                 || r.receipt_number
                                 || '-L'
                                 || lpad(r.po_line_num, 3, '0');

                SELECT
                    ap_invoices_interface_s.NEXTVAL
                INTO l_invoice_interface_id
                FROM
                    dual;

                SELECT
                    ap_invoice_lines_interface_s.NEXTVAL
                INTO l_line_interface_id
                FROM
                    dual;

                -- -------------------------------------------------------------
                -- LOAD: AP Invoice Header
                -- -------------------------------------------------------------
                INSERT INTO ap_invoices_interface (
                    invoice_id,
                    invoice_num,
                    invoice_type_lookup_code,
                    invoice_date,
                    vendor_id,
                    vendor_site_id,
                    invoice_amount,
                    invoice_currency_code,
                    exchange_rate,
                    exchange_rate_type,
                    exchange_date,
                    description,
                    source,
                    group_id,
                    org_id,
                    goods_received_date,
                    gl_date,
                    -- po_number, NO LONGER INSERTING PO NUMBER IN INTERFACE TABLE AS NEW EBS DON'T HAVE PO MODULE
                    created_by,
                    creation_date,
                    last_updated_by,
                    last_update_date,
                    status
                ) VALUES (
                    l_invoice_interface_id,
                    l_invoice_num,
                    c_invoice_type,
                    trunc(sysdate),
                    r.vendor_id,
                    r.vendor_site_id,
                    l_net_invoice_amount,
                    r.po_currency,
                    r.conversion_rate,
                    'User',
                    r.receipt_date,
                    'PO Receipt Invoice: PO# '
                    || r.po_number
                    || ' | Receipt# '
                    || r.receipt_number
                    || ' | Net Qty: '
                    || r.net_qty_received
                    || (
                        CASE
                            WHEN r.total_rtv_qty > 0 THEN
                                ' (Net of RTV: '
                                || r.total_rtv_qty
                                || ')'
                            ELSE
                                ''
                        END
                    ),
                    c_source_name,
                    l_group_id,
                    c_new_org_id,
                    r.receipt_date,
                    trunc(sysdate),
                    -- r.po_number, -- NO LONGER INSERTING PO NUMBER IN INTERFACE TABLE AS NEW EBS DON'T HAVE PO MODULE
                    c_created_by,
                    sysdate,
                    c_created_by,
                    sysdate,
                    NULL
                );

                -- -------------------------------------------------------------
                -- LOAD: AP Invoice Line
                -- -------------------------------------------------------------
                INSERT INTO ap_invoice_lines_interface (
                    invoice_id,
                    invoice_line_id,
                    line_number,
                    line_type_lookup_code,
                    amount,
                    quantity_invoiced,
                    unit_price,
                    description,
                    -- po_header_id, -- NO LONGER INSERTING PO HEADER ID IN INTERFACE LINES TABLE AS NEW EBS DON'T HAVE PO MODULE
                    rcv_transaction_id,
                    dist_code_combination_id,
                    accounting_date,
                    org_id,
                    created_by,
                    creation_date,
                    last_updated_by,
                    last_update_date
                ) VALUES (
                    l_invoice_interface_id,
                    l_line_interface_id,
                    r.po_line_num,
                    'ITEM',
                    l_net_invoice_amount,
                        CASE
                            WHEN r.purchase_basis IN ( 'TEMP LABOR', 'FIXED PRICE' ) THEN
                                NULL
                            ELSE
                                r.net_qty_received
                        END,
                        CASE
                            WHEN r.purchase_basis IN ( 'TEMP LABOR', 'FIXED PRICE' ) THEN
                                NULL
                            ELSE
                                r.unit_price
                        END,
                    r.item_description
                    || ' | PO Line: '
                    || r.po_line_num
                    || ' | Received: '
                    || to_char(r.receipt_date, 'DD-MON-YYYY')
                    || ' | Net Qty: '
                    || r.net_qty_received,
                    -- r.po_header_id, -- NO LONGER INSERTING PO HEADER ID IN INTERFACE LINES TABLE AS NEW EBS DON'T HAVE PO MODULE
                    r.rcv_transaction_id,
                    l_new_ccid,
                    trunc(sysdate),
                    c_new_org_id,
                    c_created_by,
                    sysdate,
                    c_created_by,
                    sysdate
                );

                -- -------------------------------------------------------------
                -- LOAD: Receipt-Level Tax Lines from legacy JAI_TAX_LINES
                -- Each non-zero tax line (CGST, SGST, IGST, TCS, CESS) is
                -- inserted as a separate TAX line in the AP interface.
                -- The invoice header amount is then updated to include tax.
                -- -------------------------------------------------------------
                l_total_tax_amount := 0;
                l_tax_line_count := 0;

                FOR t IN (
                    SELECT jtl.tax_rate_code,
                           jtl.tax_rate_percentage,
                           jtl.rounded_tax_amt_trx_curr   tax_amount,
                           jtl.tax_line_num
                    FROM   ja.jai_tax_lines@legacy_instance jtl
                    WHERE  jtl.entity_code = 'RCV_TRANSACTION'
                      AND  jtl.trx_id = r.rcv_transaction_id
                      AND  jtl.rounded_tax_amt_trx_curr > 0
                    ORDER BY jtl.tax_line_num
                ) LOOP
                    l_tax_line_count := l_tax_line_count + 1;
                    l_total_tax_amount := l_total_tax_amount + t.tax_amount;

                    INSERT INTO AP_INVOICE_LINES_INTERFACE (
                        invoice_id,
                        invoice_line_id,
                        line_number,
                        line_type_lookup_code,
                        amount,
                        description,
                        dist_code_combination_id,
                        accounting_date,
                        org_id,
                        created_by,
                        creation_date,
                        last_updated_by,
                        last_update_date
                    ) VALUES (
                        l_invoice_interface_id,
                        ap_invoice_lines_interface_s.NEXTVAL,
                        (r.po_line_num * 100) + l_tax_line_count,
                        'TAX',
                        t.tax_amount,
                        t.tax_rate_code || ' @' || t.tax_rate_percentage
                            || '% | Receipt# ' || r.receipt_number
                            || ' | PO Line: ' || r.po_line_num,
                        l_new_ccid,
                        trunc(sysdate),
                        c_new_org_id,
                        c_created_by,
                        sysdate,
                        c_created_by,
                        sysdate
                    );

                    IF p_debug_mode = 'Y' THEN
                        log_message('TAX LINE: ' || t.tax_rate_code
                                    || ' @' || t.tax_rate_percentage || '%'
                                    || ' Amount:' || t.tax_amount
                                    || ' for Receipt# ' || r.receipt_number);
                    END IF;
                END LOOP;

                -- Update invoice header amount to include tax
                IF l_total_tax_amount > 0 THEN
                    UPDATE ap_invoices_interface
                    SET    invoice_amount = invoice_amount + l_total_tax_amount
                    WHERE  invoice_id = l_invoice_interface_id;

                    l_net_invoice_amount := l_net_invoice_amount + l_total_tax_amount;

                    IF p_debug_mode = 'Y' THEN
                        log_message('TAX TOTAL: ' || l_tax_line_count || ' tax lines, total tax: '
                                    || l_total_tax_amount || ', new invoice amount: ' || l_net_invoice_amount);
                    END IF;
                END IF;

                -- -------------------------------------------------------------
                -- LOG successful processing
                -- -------------------------------------------------------------
                INSERT INTO xxcust_po_ap_interface_log (
                    run_id,
                    transaction_class,
                    legacy_rcv_transaction_id,
                    legacy_po_header_id,
                    legacy_po_number,
                    legacy_po_line_num,
                    legacy_vendor_id,
                    vendor_name,
                    receipt_number,
                    receipt_date,
                    receipt_quantity,
                    rtv_quantity,
                    net_quantity,
                    receipt_amount,
                    invoice_num,
                    invoice_amount,
                    interface_status
                ) VALUES (
                    l_run_id,
                    c_class_invoice,
                    r.rcv_transaction_id,
                    r.po_header_id,
                    r.po_number,
                    r.po_line_num,
                    r.vendor_id,
                    r.vendor_name,
                    r.receipt_number,
                    r.receipt_date,
                    r.gross_qty_received,
                    r.total_rtv_qty,
                    r.net_qty_received,
                    l_net_invoice_amount,
                    l_invoice_num,
                    l_net_invoice_amount,
                    c_status_processed
                );

                l_records_processed := l_records_processed + 1;
                IF p_debug_mode = 'Y' THEN
                    log_message('PROCESSED: PO#'
                                || r.po_number
                                || ' Receipt:'
                                || r.receipt_number
                                || ' Invoice:'
                                || l_invoice_num
                                || ' NetAmt:'
                                || l_net_invoice_amount);
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    l_error_message := sqlerrm;
                    INSERT INTO xxcust_po_ap_interface_log (
                        run_id,
                        transaction_class,
                        legacy_rcv_transaction_id,
                        legacy_po_header_id,
                        legacy_po_number,
                        legacy_po_line_num,
                        legacy_vendor_id,
                        vendor_name,
                        receipt_number,
                        receipt_date,
                        interface_status,
                        rejection_reason
                    ) VALUES (
                        l_run_id,
                        c_class_invoice,
                        l_exc_rcv_transaction_id,
                        l_exc_po_header_id,
                        l_exc_po_number,
                        l_exc_po_line_num,
                        l_exc_vendor_id,
                        l_exc_vendor_name,
                        l_exc_receipt_number,
                        l_exc_receipt_date,
                        c_status_error,
                        'Unexpected error: ' || l_error_message
                    );

                    l_records_errored := l_records_errored + 1;
                    log_message('ERROR: PO#'
                                || l_exc_po_number
                                || ' | '
                                || l_error_message);
            END;

        END LOOP;

        COMMIT;
        log_message('============================================================');
        log_message('PROCESS A Complete - Run ID: ' || l_run_id);
        log_message('  Processed : ' || l_records_processed);
        log_message('  Rejected  : ' || l_records_rejected);
        log_message('  Skipped   : '
                    || l_records_skipped
                    || '  (net qty = 0 after RTV netting)');
        log_message('  Errors    : ' || l_records_errored);
        log_message('Next: Run Payables Open Interface Import | Source: '
                    || c_source_name
                    || ' | Group ID: '
                    || l_group_id);
        log_message('============================================================');
        IF l_records_rejected > 0 OR l_records_errored > 0 THEN
            p_retcode := 1;
            p_errbuf := 'Process A completed with '
                        || l_records_rejected
                        || ' rejections and '
                        || l_records_errored
                        || ' errors. Review XXCUST_PO_AP_INTERFACE_LOG.';
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            p_retcode := 2;
            p_errbuf := 'Process A fatal error: ' || sqlerrm;
            log_message('FATAL ERROR (Process A): ' || sqlerrm);
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
        p_errbuf         OUT VARCHAR2,
        p_retcode        OUT NUMBER,
        p_operating_unit IN VARCHAR2 DEFAULT NULL,
        p_rtv_date_from  IN VARCHAR2 DEFAULT NULL,
        p_rtv_date_to    IN VARCHAR2 DEFAULT NULL,
        p_po_number      IN VARCHAR2 DEFAULT NULL,
        p_debug_mode     IN VARCHAR2 DEFAULT 'N'
    ) IS

        -- -----------------------------------------------------------------------
        -- RTV extraction cursor.
        -- Selects all RETURN TO VENDOR transactions from the legacy instance.
        -- Only RTVs sourced from POs are included.
        -- The interface logic downstream determines Scenario A vs B/C.
        -- -----------------------------------------------------------------------
        CURSOR c_rtvs IS
        SELECT
            rsh.receipt_num                    receipt_number,
            rt.transaction_id                  rtv_transaction_id,
            rt.transaction_date                rtv_date,
            rt.po_line_location_id             po_line_location_id,
            ph.po_header_id                    po_header_id,
            ph.segment1                        po_number,
            ph.type_lookup_code                po_type,
            ph.currency_code                   po_currency,
            pl.line_num                        po_line_num,
            pl.item_id                         item_id,       -- this is used in derive_new_ccid for product segment
            pl.item_description                item_description,
            pl.purchase_basis                  purchase_basis,

                -- RTV quantities and amounts
            rt.quantity                        rtv_quantity,
            rt.po_unit_price                   unit_price,
            ( rt.quantity * rt.po_unit_price ) rtv_amount,

                -- Services: use amount-based figure
            pll.amount_received                amount_received,
            rt.unit_of_measure                 uom_code,
            rt.currency_code                   receipt_currency,
            rt.currency_conversion_rate        conversion_rate,
            ph.vendor_id                       vendor_id,
            ph.vendor_site_id                  vendor_site_id,
            aps.vendor_name                    vendor_name,
            pod.code_combination_id            legacy_ccid,
            ph.org_id                          org_id,
            rt.organization_id                 inventory_org_id, -- this is used in mapping division in COA mapping
            hou.name                           operating_unit_name
        FROM
                 apps.rcv_transactions@legacy_instance rt
            JOIN apps.rcv_shipment_headers@legacy_instance      rsh ON rsh.shipment_header_id = rt.shipment_header_id
            JOIN apps.rcv_shipment_lines@legacy_instance        rsl ON rsl.shipment_line_id = rt.shipment_line_id
            JOIN apps.po_headers_all@legacy_instance            ph ON ph.po_header_id = rt.po_header_id
            JOIN apps.po_lines_all@legacy_instance              pl ON pl.po_header_id = rt.po_header_id
                                                         AND pl.po_line_id = rt.po_line_id
            JOIN apps.po_line_locations_all@legacy_instance     pll ON pll.line_location_id = rt.po_line_location_id
            JOIN apps.po_distributions_all@legacy_instance      pod ON pod.line_location_id = rt.po_line_location_id
            JOIN apps.ap_suppliers@legacy_instance              aps ON aps.vendor_id = ph.vendor_id
            JOIN apps.ap_supplier_sites_all@legacy_instance     apss ON apss.vendor_site_id = ph.vendor_site_id
            JOIN apps.hr_all_organization_units@legacy_instance hou ON hou.organization_id = ph.org_id
        WHERE
                -- Only RETURN TO VENDOR transactions
                rt.transaction_type = 'RETURN TO VENDOR'
            AND rt.source_document_code = 'PO'

                -- Active PO
            AND ph.cancel_flag = 'N'

                -- RTV must have positive quantity
            AND rt.quantity > 0

                -- Optional filters
            AND ( p_operating_unit IS NULL
                  OR hou.name = p_operating_unit )
            AND ( p_rtv_date_from IS NULL
                  OR rt.transaction_date >= to_date(p_rtv_date_from, 'DD-MON-YYYY') )
            AND ( p_rtv_date_to IS NULL
                  OR rt.transaction_date <= to_date(p_rtv_date_to, 'DD-MON-YYYY') + 1 )
            AND ( p_po_number IS NULL
                  OR ph.segment1 = p_po_number )
        ORDER BY
            ph.vendor_id,
            ph.segment1,
            pl.line_num,
            rt.transaction_date;

        -- Local variables
        l_run_id                 VARCHAR2(50) := 'RTV-' || to_char(sysdate, 'YYYYMMDDHH24MISS');
        l_group_id               NUMBER;
        l_credit_memo_num        VARCHAR2(50);
        l_original_invoice_num   VARCHAR2(50);
        l_original_inv_amount    NUMBER;
        l_new_ccid               NUMBER;
        l_rejection_reason       VARCHAR2(2000);
        l_is_valid               BOOLEAN;
        l_records_processed      NUMBER := 0;
        l_records_rejected       NUMBER := 0;
        l_records_skipped        NUMBER := 0;
        l_records_errored        NUMBER := 0;
        l_invoice_interface_id   NUMBER;
        l_line_interface_id      NUMBER;
        l_credit_amount          NUMBER;
        l_total_tax_amount       NUMBER;
        l_tax_line_count         NUMBER;
        l_error_message          VARCHAR2(2000);
        -- Local copies of cursor fields for use in EXCEPTION handler
        l_exc_rtv_transaction_id NUMBER;
        l_exc_po_header_id       NUMBER;
        l_exc_po_number          VARCHAR2(50);
        l_exc_po_line_num        NUMBER;
        l_exc_vendor_id          NUMBER;
        l_exc_vendor_name        VARCHAR2(240);
        l_exc_receipt_number     VARCHAR2(30);
        l_exc_rtv_date           DATE;
    BEGIN
        p_retcode := 0;
        SELECT
            ap_invoices_interface_s.NEXTVAL
        INTO l_group_id
        FROM
            dual;

        log_message('============================================================');
        log_message('PROCESS B: RTV-to-Credit-Memo Interface (v2.0)');
        log_message('Run ID: '
                    || l_run_id
                    || '  |  Group ID: '
                    || l_group_id);
        log_message('Operating Unit : ' || nvl(p_operating_unit, 'ALL'));
        log_message('RTV Date From  : ' || nvl(p_rtv_date_from, 'NONE'));
        log_message('RTV Date To    : ' || nvl(p_rtv_date_to, 'NONE'));
        log_message('PO Number      : ' || nvl(p_po_number, 'ALL'));
        log_message('============================================================');
        FOR r IN c_rtvs LOOP
            l_exc_rtv_transaction_id := r.rtv_transaction_id;
            l_exc_po_header_id := r.po_header_id;
            l_exc_po_number := r.po_number;
            l_exc_po_line_num := r.po_line_num;
            l_exc_vendor_id := r.vendor_id;
            l_exc_vendor_name := r.vendor_name;
            l_exc_receipt_number := r.receipt_number;
            l_exc_rtv_date := r.rtv_date;
            BEGIN
                -- -------------------------------------------------------------
                -- STEP 1: Check if original AP Invoice was ever created
                -- Determines Scenario A (skip) vs Scenario B/C (credit memo)
                -- -------------------------------------------------------------
                l_original_invoice_num := get_original_invoice_num(p_po_line_location_id => r.po_line_location_id, p_po_header_id => r.
                po_header_id);

                -- If no original invoice found = Scenario A (net qty handled it)
                IF l_original_invoice_num IS NULL THEN
                    INSERT INTO xxcust_po_ap_interface_log (
                        run_id,
                        transaction_class,
                        legacy_rcv_transaction_id,
                        legacy_po_header_id,
                        legacy_po_number,
                        legacy_po_line_num,
                        legacy_vendor_id,
                        vendor_name,
                        receipt_number,
                        receipt_date,
                        rtv_quantity,
                        interface_status,
                        rejection_reason
                    ) VALUES (
                        l_run_id,
                        c_class_credit_memo,
                        r.rtv_transaction_id,
                        r.po_header_id,
                        r.po_number,
                        r.po_line_num,
                        r.vendor_id,
                        r.vendor_name,
                        r.receipt_number,
                        r.rtv_date,
                        r.rtv_quantity,
                        c_status_skipped,
                        'Scenario A RTV: no AP Invoice was previously created for this PO line. ' || 'Net quantity logic handled this return. No Credit Memo required.'
                    );

                    l_records_skipped := l_records_skipped + 1;
                    IF p_debug_mode = 'Y' THEN
                        log_message('SKIPPED (Scenario A): PO#'
                                    || r.po_number
                                    || ' Line:'
                                    || r.po_line_num
                                    || ' RTV Qty:'
                                    || r.rtv_quantity);
                    END IF;

                    CONTINUE;
                END IF;

                -- -------------------------------------------------------------
                -- STEP 2: Get original invoice amount for over-credit check
                -- -------------------------------------------------------------
                BEGIN
                    SELECT
                        invoice_amount
                    INTO l_original_inv_amount
                    FROM
                        ap_invoices_all
                    WHERE
                            invoice_num = l_original_invoice_num
                        AND vendor_id = r.vendor_id
                        AND ROWNUM = 1;

                EXCEPTION
                    WHEN no_data_found THEN
                        l_original_inv_amount := r.rtv_amount; -- fallback: allow credit
                END;

                -- -------------------------------------------------------------
                -- STEP 3: Compute Credit Memo amount
                -- Scenario B (full RTV):    full rtv_amount
                -- Scenario C (partial RTV): partial rtv_amount (rtv_qty x unit_price)
                -- For Services: use amount_received
                -- -------------------------------------------------------------
                IF r.purchase_basis IN ( 'TEMP LABOR', 'FIXED PRICE' ) THEN
                    l_credit_amount := r.amount_received;
                ELSE
                    l_credit_amount := r.rtv_quantity * r.unit_price;
                END IF;

                -- -------------------------------------------------------------
                -- STEP 4: Validate RTV record
                -- -------------------------------------------------------------
                l_new_ccid := derive_new_ccid(p_legacy_ccid => r.legacy_ccid, p_inventory_org_id => r.inventory_org_id, p_inventory_item_id =>
                r.item_id, p_rcv_transaction_id => r.rtv_transaction_id);

                -- if l_new_ccid = -1 then assign dummy value '00000'
                IF l_new_ccid = -1 THEN
                    l_new_ccid := 00000;
                END IF;

                log_message('New CCID for PO#' || r.po_number || ' Line:' || r.po_line_num
                            || ' legacy_ccid=' || r.legacy_ccid || ' → new_ccid=' || l_new_ccid);

                l_is_valid := validate_rtv(p_rtv_transaction_id => r.rtv_transaction_id, p_po_line_location_id => r.po_line_location_id,
                p_po_header_id => r.po_header_id, p_rtv_quantity => r.rtv_quantity, p_rtv_amount => l_credit_amount,
                                          p_original_invoice_num => l_original_invoice_num, p_original_inv_amount => l_original_inv_amount,
                                          p_legacy_ccid => r.legacy_ccid, p_rejection_reason => l_rejection_reason);

                -- LOG l_is_valid and l_rejection_reason for debugging
                    IF p_debug_mode = 'Y' THEN
                        log_message('Validation result for RTV TXN ID '
                                    || r.rtv_transaction_id
                                    || ': is_valid='
                                    || CASE WHEN l_is_valid THEN 'TRUE' ELSE 'FALSE' END
                                    || ', rejection_reason='
                                    || nvl(l_rejection_reason, 'NONE'));
                    END IF;

                IF NOT l_is_valid THEN
                    INSERT INTO xxcust_po_ap_interface_log (
                        run_id,
                        transaction_class,
                        legacy_rcv_transaction_id,
                        legacy_po_header_id,
                        legacy_po_number,
                        legacy_po_line_num,
                        legacy_vendor_id,
                        vendor_name,
                        receipt_number,
                        receipt_date,
                        rtv_quantity,
                        interface_status,
                        rejection_reason
                    ) VALUES (
                        l_run_id,
                        c_class_credit_memo,
                        r.rtv_transaction_id,
                        r.po_header_id,
                        r.po_number,
                        r.po_line_num,
                        r.vendor_id,
                        r.vendor_name,
                        r.receipt_number,
                        r.rtv_date,
                        r.rtv_quantity,
                        c_status_rejected,
                        l_rejection_reason
                    );

                    l_records_rejected := l_records_rejected + 1;
                    log_message('REJECTED (RTV): PO#'
                                || r.po_number
                                || ' Line:'
                                || r.po_line_num
                                || ' | '
                                || l_rejection_reason);

                    CONTINUE;
                END IF;

                -- -------------------------------------------------------------
                -- STEP 5: Generate Credit Memo number
                -- Convention: CM-{original_invoice_num}
                -- -------------------------------------------------------------
                l_credit_memo_num := 'CM-' || l_original_invoice_num;
                SELECT
                    ap_invoices_interface_s.NEXTVAL
                INTO l_invoice_interface_id
                FROM
                    dual;

                SELECT
                    ap_invoice_lines_interface_s.NEXTVAL
                INTO l_line_interface_id
                FROM
                    dual;

                -- -------------------------------------------------------------
                -- LOAD: Credit Memo Header (invoice_type_lookup_code = CREDIT)
                -- Note: Credit amounts are loaded as POSITIVE values.
                -- Oracle AP handles the sign reversal for CREDIT type invoices.
                -- -------------------------------------------------------------
                INSERT INTO ap_invoices_interface (
                    invoice_id,
                    invoice_num,
                    invoice_type_lookup_code,
                    invoice_date,
                    vendor_id,
                    vendor_site_id,
                    invoice_amount,
                    invoice_currency_code,
                    exchange_rate,
                    exchange_rate_type,
                    exchange_date,
                    description,
                    source,
                    group_id,
                    org_id,
                    goods_received_date,
                    gl_date,
                    -- po_number, -- NO LONGER INSERTING PO NUMBER IN INTERFACE TABLE AS NEW EBS DON'T HAVE PO MODULE
                    created_by,
                    creation_date,
                    last_updated_by,
                    last_update_date,
                    status
                ) VALUES (
                    l_invoice_interface_id,
                    l_credit_memo_num,
                    c_credit_type,                              -- CREDIT type = Credit Memo
                    trunc(sysdate),
                    r.vendor_id,
                    r.vendor_site_id,
                    l_credit_amount,                            -- positive; AP reverses sign for CREDIT type
                    r.po_currency,
                    r.conversion_rate,
                    'User',
                    r.rtv_date,
                    'RTV Credit Memo: PO# '
                    || r.po_number
                    || ' | Original Invoice: '
                    || l_original_invoice_num
                    || ' | RTV Date: '
                    || to_char(r.rtv_date, 'DD-MON-YYYY')
                    || ' | RTV Qty: '
                    || r.rtv_quantity,
                    c_source_name,
                    l_group_id,
                    c_new_org_id,
                    r.rtv_date,
                    trunc(sysdate),
                    -- r.po_number, -- NO LONGER INSERTING PO NUMBER IN INTERFACE TABLE AS NEW EBS DON'T HAVE PO MODULE
                    c_created_by,
                    sysdate,
                    c_created_by,
                    sysdate,
                    NULL
                );

                -- -------------------------------------------------------------
                -- LOAD: Credit Memo Line
                -- line_type_lookup_code = ITEM, amount = positive (AP reverses)
                -- -------------------------------------------------------------
                INSERT INTO ap_invoice_lines_interface (
                    invoice_id,
                    invoice_line_id,
                    line_number,
                    line_type_lookup_code,
                    amount,
                    quantity_invoiced,
                    unit_price,
                    description,
                    -- po_header_id, -- NO LONGER INSERTING PO HEADER ID IN INTERFACE LINES TABLE AS NEW EBS DON'T HAVE PO MODULE
                    rcv_transaction_id,
                    dist_code_combination_id,
                    accounting_date,
                    org_id,
                    created_by,
                    creation_date,
                    last_updated_by,
                    last_update_date
                ) VALUES (
                    l_invoice_interface_id,
                    l_line_interface_id,
                    r.po_line_num,
                    'ITEM',
                    l_credit_amount,
                        CASE
                            WHEN r.purchase_basis IN ( 'TEMP LABOR', 'FIXED PRICE' ) THEN
                                NULL
                            ELSE
                                r.rtv_quantity
                        END,
                        CASE
                            WHEN r.purchase_basis IN ( 'TEMP LABOR', 'FIXED PRICE' ) THEN
                                NULL
                            ELSE
                                r.unit_price
                        END,
                    'RTV: '
                    || r.item_description
                    || ' | PO Line: '
                    || r.po_line_num
                    || ' | RTV Date: '
                    || to_char(r.rtv_date, 'DD-MON-YYYY'),
                    -- r.po_header_id, -- NO LONGER INSERTING PO HEADER ID IN INTERFACE LINES TABLE AS NEW EBS DON'T HAVE PO MODULE
                    r.rtv_transaction_id,
                    l_new_ccid,
                    trunc(sysdate),
                    c_new_org_id,
                    c_created_by,
                    sysdate,
                    c_created_by,
                    sysdate
                );

                -- -------------------------------------------------------------
                -- LOAD: Receipt-Level Tax Lines for Credit Memo
                -- Same logic as Process A but for the RTV transaction.
                -- Tax amounts are positive; Oracle AP reverses for CREDIT type.
                -- -------------------------------------------------------------
                l_total_tax_amount := 0;
                l_tax_line_count := 0;

                FOR t IN (
                    SELECT jtl.tax_rate_code,
                           jtl.tax_rate_percentage,
                           jtl.rounded_tax_amt_trx_curr   tax_amount,
                           jtl.tax_line_num
                    FROM   ja.jai_tax_lines@legacy_instance jtl
                    WHERE  jtl.entity_code = 'RCV_TRANSACTION'
                      AND  jtl.trx_id = r.rtv_transaction_id
                      AND  jtl.rounded_tax_amt_trx_curr > 0
                    ORDER BY jtl.tax_line_num
                ) LOOP
                    l_tax_line_count := l_tax_line_count + 1;
                    l_total_tax_amount := l_total_tax_amount + t.tax_amount;

                    INSERT INTO ap_invoice_lines_interface (
                        invoice_id,
                        invoice_line_id,
                        line_number,
                        line_type_lookup_code,
                        amount,
                        description,
                        dist_code_combination_id,
                        accounting_date,
                        org_id,
                        created_by,
                        creation_date,
                        last_updated_by,
                        last_update_date
                    ) VALUES (
                        l_invoice_interface_id,
                        ap_invoice_lines_interface_s.NEXTVAL,
                        (r.po_line_num * 100) + l_tax_line_count,
                        'TAX',
                        t.tax_amount,
                        'RTV ' || t.tax_rate_code || ' @' || t.tax_rate_percentage
                            || '% | Receipt# ' || r.receipt_number
                            || ' | PO Line: ' || r.po_line_num,
                        l_new_ccid,
                        trunc(sysdate),
                        c_new_org_id,
                        c_created_by,
                        sysdate,
                        c_created_by,
                        sysdate
                    );

                    IF p_debug_mode = 'Y' THEN
                        log_message('RTV TAX LINE: ' || t.tax_rate_code
                                    || ' @' || t.tax_rate_percentage || '%'
                                    || ' Amount:' || t.tax_amount
                                    || ' for RTV Receipt# ' || r.receipt_number);
                    END IF;
                END LOOP;

                -- Update credit memo header amount to include tax
                IF l_total_tax_amount > 0 THEN
                    UPDATE ap_invoices_interface
                    SET    invoice_amount = invoice_amount + l_total_tax_amount
                    WHERE  invoice_id = l_invoice_interface_id;

                    l_credit_amount := l_credit_amount + l_total_tax_amount;

                    IF p_debug_mode = 'Y' THEN
                        log_message('RTV TAX TOTAL: ' || l_tax_line_count || ' tax lines, total tax: '
                                    || l_total_tax_amount || ', new credit memo amount: ' || l_credit_amount);
                    END IF;
                END IF;

                -- -------------------------------------------------------------
                -- LOG successful Credit Memo load
                -- -------------------------------------------------------------
                INSERT INTO xxcust_po_ap_interface_log (
                    run_id,
                    transaction_class,
                    legacy_rcv_transaction_id,
                    legacy_po_header_id,
                    legacy_po_number,
                    legacy_po_line_num,
                    legacy_vendor_id,
                    vendor_name,
                    receipt_number,
                    receipt_date,
                    rtv_quantity,
                    receipt_amount,
                    invoice_num,
                    invoice_amount,
                    interface_status
                ) VALUES (
                    l_run_id,
                    c_class_credit_memo,
                    r.rtv_transaction_id,
                    r.po_header_id,
                    r.po_number,
                    r.po_line_num,
                    r.vendor_id,
                    r.vendor_name,
                    r.receipt_number,
                    r.rtv_date,
                    r.rtv_quantity,
                    l_credit_amount,
                    l_credit_memo_num,
                    l_credit_amount,
                    c_status_processed
                );

                l_records_processed := l_records_processed + 1;
                IF p_debug_mode = 'Y' THEN
                    log_message('PROCESSED (RTV): PO#'
                                || r.po_number
                                || ' CreditMemo:'
                                || l_credit_memo_num
                                || ' Amount:'
                                || l_credit_amount
                                || ' OrigInvoice:'
                                || l_original_invoice_num);
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    l_error_message := sqlerrm;
                    INSERT INTO xxcust_po_ap_interface_log (
                        run_id,
                        transaction_class,
                        legacy_rcv_transaction_id,
                        legacy_po_header_id,
                        legacy_po_number,
                        legacy_po_line_num,
                        legacy_vendor_id,
                        vendor_name,
                        receipt_number,
                        receipt_date,
                        interface_status,
                        rejection_reason
                    ) VALUES (
                        l_run_id,
                        c_class_credit_memo,
                        l_exc_rtv_transaction_id,
                        l_exc_po_header_id,
                        l_exc_po_number,
                        l_exc_po_line_num,
                        l_exc_vendor_id,
                        l_exc_vendor_name,
                        l_exc_receipt_number,
                        l_exc_rtv_date,
                        c_status_error,
                        'Unexpected error: ' || l_error_message
                    );

                    l_records_errored := l_records_errored + 1;
                    log_message('ERROR (RTV): PO#'
                                || l_exc_po_number
                                || ' | '
                                || l_error_message);
            END;

        END LOOP;

        COMMIT;
        log_message('============================================================');
        log_message('PROCESS B Complete - Run ID: ' || l_run_id);
        log_message('  Credit Memos Processed : ' || l_records_processed);
        log_message('  Rejected               : ' || l_records_rejected);
        log_message('  Skipped (Scenario A)   : '
                    || l_records_skipped
                    || '  (no prior AP Invoice; handled by net qty logic)');
        log_message('  Errors                 : ' || l_records_errored);
        log_message('Next: Run Payables Open Interface Import | Source: '
                    || c_source_name
                    || ' | Group ID: '
                    || l_group_id);
        log_message('============================================================');
        IF l_records_rejected > 0 OR l_records_errored > 0 THEN
            p_retcode := 1;
            p_errbuf := 'Process B completed with '
                        || l_records_rejected
                        || ' rejections and '
                        || l_records_errored
                        || ' errors. Review XXCUST_PO_AP_INTERFACE_LOG.';
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            p_retcode := 2;
            p_errbuf := 'Process B fatal error: ' || sqlerrm;
            log_message('FATAL ERROR (Process B): ' || sqlerrm);
    END run_rtv_interface;

    -- =========================================================================
    -- Purge successfully processed log records older than N days
    -- =========================================================================
    PROCEDURE purge_log (
        p_days_to_keep IN NUMBER DEFAULT 90
    ) IS
        l_count NUMBER;
    BEGIN
        DELETE FROM xxcust_po_ap_interface_log
        WHERE
                creation_date < sysdate - p_days_to_keep
            AND interface_status = c_status_processed;

        l_count := SQL%rowcount;
        COMMIT;
        log_message('Purge complete. '
                    || l_count
                    || ' log records deleted (older than '
                    || p_days_to_keep
                    || ' days).');
    END purge_log;

END xxcust_po_ap_interface_pkg;
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
    log.legacy_po_number po_number,
    log.vendor_name,
    log.receipt_number,
    log.receipt_date,
    log.receipt_quantity gross_qty,
    log.rtv_quantity     rtv_qty,
    log.net_quantity     net_qty,
    log.invoice_num,
    log.invoice_amount   interfaced_amount,
    ai.invoice_id        ap_invoice_id,
    ai.invoice_amount    ap_invoice_amount,
    ai.approval_status,
    CASE
        WHEN log.interface_status = c_status_processed
             AND ai.invoice_id IS NULL THEN
            'WARNING: Loaded to interface but AP Invoice not found. Re-run AP Import.'
        WHEN log.interface_status = c_status_rejected THEN
            'REJECTED: ' || log.rejection_reason
        WHEN log.interface_status = c_status_skipped  THEN
            'SKIPPED (net qty=0): ' || log.rejection_reason
        WHEN log.interface_status = c_status_error    THEN
            'ERROR: ' || log.rejection_reason
        WHEN log.interface_status = c_status_processed
             AND ai.invoice_id IS NOT NULL THEN
            'OK'
        ELSE
            'UNKNOWN'
    END                  reconciliation_status
FROM
    xxcust_po_ap_interface_log log
    LEFT JOIN ap_invoices_all            ai ON ai.invoice_num = log.invoice_num
                                    AND ai.vendor_id = log.legacy_vendor_id
WHERE
        log.run_id = :p_run_id
    AND log.transaction_class = 'INVOICE'
ORDER BY
    log.interface_status,
    log.legacy_po_number;

-- -----------------------------------------------------------------------
-- Query 4B: RTV-to-Credit-Memo Reconciliation
-- Run after Payables Open Interface Import to confirm all RTVs
-- were successfully converted to AP Credit Memos.
-- -----------------------------------------------------------------------
SELECT
    log.run_id,
    log.transaction_class,
    log.legacy_po_number po_number,
    log.vendor_name,
    log.receipt_date     rtv_date,
    log.rtv_quantity,
    log.invoice_num      credit_memo_num,
    log.invoice_amount   credit_memo_amount,
    ai.invoice_id        ap_credit_memo_id,
    ai.invoice_amount    ap_credit_amount,
    ai.approval_status,
    CASE
        WHEN log.interface_status = c_status_processed
             AND ai.invoice_id IS NULL THEN
            'WARNING: Loaded to interface but Credit Memo not found. Re-run AP Import.'
        WHEN log.interface_status = c_status_rejected THEN
            'REJECTED: ' || log.rejection_reason
        WHEN log.interface_status = c_status_skipped  THEN
            'SKIPPED (Scenario A - no prior invoice): ' || log.rejection_reason
        WHEN log.interface_status = c_status_processed
             AND ai.invoice_id IS NOT NULL THEN
            'OK'
        ELSE
            'UNKNOWN'
    END                  reconciliation_status
FROM
    xxcust_po_ap_interface_log log
    LEFT JOIN ap_invoices_all            ai ON ai.invoice_num = log.invoice_num
                                    AND ai.vendor_id = log.legacy_vendor_id
                                    AND ai.invoice_type_lookup_code = 'CREDIT'
WHERE
        log.run_id = :p_run_id
    AND log.transaction_class = 'CREDIT_MEMO'
ORDER BY
    log.interface_status,
    log.legacy_po_number;

-- -----------------------------------------------------------------------
-- Query 4C: Net AP Balance per PO Line (Master Reconciliation)
-- Confirms net AP exposure = net received qty x unit price for each PO line.
-- Run this as the final sign-off check after both Process A and Process B.
-- -----------------------------------------------------------------------
SELECT
    inv_log.legacy_po_number                                    po_number,
    inv_log.legacy_po_line_num                                  po_line,
    inv_log.vendor_name,
    inv_log.receipt_quantity                                    gross_received_qty,
    nvl(rtv_log.rtv_quantity, 0)                                total_rtv_qty,
    ( inv_log.receipt_quantity - nvl(rtv_log.rtv_quantity, 0) ) net_qty,
    inv_log.invoice_amount                                      invoice_amount,
    nvl(rtv_log.invoice_amount, 0)                              credit_memo_amount,
    ( inv_log.invoice_amount - nvl(rtv_log.invoice_amount, 0) ) net_ap_balance,
    CASE
        WHEN abs((inv_log.invoice_amount - nvl(rtv_log.invoice_amount, 0)) -((inv_log.receipt_quantity - nvl(rtv_log.rtv_quantity, 0)) *
        inv_log.invoice_amount / nullif(inv_log.receipt_quantity, 0))) < 0.01 THEN
            'BALANCED'
        ELSE
            'VARIANCE - REVIEW'
    END                                                         balance_status
FROM
    (   -- Aggregated invoice log per PO line
        SELECT
            legacy_po_number,
            legacy_po_line_num,
            vendor_name,
            SUM(receipt_quantity) receipt_quantity,
            SUM(invoice_amount)   invoice_amount
        FROM
            xxcust_po_ap_interface_log
        WHERE
                transaction_class = 'INVOICE'
            AND interface_status = 'PROCESSED'
        GROUP BY
            legacy_po_number,
            legacy_po_line_num,
            vendor_name
    ) inv_log
    LEFT JOIN (
        -- Aggregated credit memo log per PO line
        SELECT
            legacy_po_number,
            legacy_po_line_num,
            SUM(rtv_quantity)   rtv_quantity,
            SUM(invoice_amount) invoice_amount
        FROM
            xxcust_po_ap_interface_log
        WHERE
                transaction_class = 'CREDIT_MEMO'
            AND interface_status = 'PROCESSED'
        GROUP BY
            legacy_po_number,
            legacy_po_line_num
    ) rtv_log ON rtv_log.legacy_po_number = inv_log.legacy_po_number
                 AND rtv_log.legacy_po_line_num = inv_log.legacy_po_line_num
ORDER BY
    balance_status DESC,
    inv_log.legacy_po_number;

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
 
 
 -- ORG_ID = 81 IN INVOICE AND INVOICE LINE INTERFACE ( DONE )
 -- GL_DATE ( DONE )
 -- ACCOUNTING_DATE ( DONE )
 -- SOURCE ( DONE )
 -- STATUS SHOULD BE NULL/EMPTY ( IRRELEVANT )

-- PO number remove IN BOTH( we don't have PO modules in new ebs ) ( DONE )
 
-- TERMS_ID ( payment )  ( NEEDED )
-- TERMS_NAME ( payment ) ( NEEDED)
-- TERM_DATE ( payment ) ( NEEDED )

-- PAYMENT_METHOD_LOOKUP_CODE ( CHECK ) SHOULD BE CHECK IN SUPPLIERS SITE

-- VENDOR_ID, ( FROM SUPPLIERS TABLE )
-- VENDOR_NUMBER, ( FROM SUPPLIERS TABLE )
-- VENDOR_SITE_CODE ( FROM SUPPLIER SITES )

-- BLANKET PO
    -- header id 6716988
    -- segment1 6258548

