-- =============================================================================
-- BAMUL OM to AR Interface - Package Body v7
-- Run on: TARGET (New EBS) instance as APPS user
-- See changelog.txt for full change history
-- [CHG-46] Added OM to GL COGS Interface call before AutoInvoice submission.
--          Calls xxcust_om_gl_interface_pkg.run_interface, then submits
--          Journal Import (GLLEZL) if GL group_id returned successfully.
-- =============================================================================

CREATE OR REPLACE PACKAGE BODY      bamul_om_ar_interface_pkg AS

    g_user_id   NUMBER := fnd_global.user_id;
    g_login_id  NUMBER := fnd_global.login_id;
    g_org_id    NUMBER;                                         -- [CHG-1] initialized in package body BEGIN block

    -- =========================================================================
    -- Helper: Get new division code from legacy org_id
    -- =========================================================================
    FUNCTION get_division (p_org_id IN NUMBER) RETURN VARCHAR2 IS
        l_division VARCHAR2(2);
    BEGIN
        SELECT division_code INTO l_division
          FROM apps.mapping_division_segment
         WHERE organization_id = p_org_id;
        RETURN l_division;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN RETURN '00';
    END get_division;

    -- =========================================================================
    -- Helper: Map legacy account segment to new account segment
    -- =========================================================================
    FUNCTION get_new_account (p_old_account IN NUMBER) RETURN NUMBER IS
        l_account NUMBER;
    BEGIN
        SELECT new_account_flex_value INTO l_account
          FROM apps.mapping_account_segment
         WHERE old_account_flex_value = p_old_account;
        RETURN l_account;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN RETURN p_old_account;
    END get_new_account;

    -- =========================================================================
    -- Helper: Map legacy cost center to new department
    -- =========================================================================
    FUNCTION get_new_department (p_old_dept IN VARCHAR2) RETURN VARCHAR2 IS
        l_dept VARCHAR2(30);
    BEGIN
        SELECT new_department_flex_value INTO l_dept
          FROM apps.mapping_department_segment
         WHERE old_department_flex_value = p_old_dept;
        RETURN l_dept;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN RETURN p_old_dept;
    END get_new_department;

    -- =========================================================================
    -- Helper: Map legacy inventory_item_id to new product code
    -- =========================================================================
    FUNCTION get_new_product (p_item_id IN NUMBER) RETURN VARCHAR2 IS
        l_product VARCHAR2(8);
    BEGIN
        SELECT product_code INTO l_product
          FROM apps.mapping_product_segment
         WHERE inventory_item_id = p_item_id;
        RETURN l_product;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN RETURN '00000000';
    END get_new_product;

    -- =========================================================================
    -- Helper: Derive TXN segment from GST tax rate code
    -- CGST/SGST -> 1 (Local), IGST -> 2 (Interstate), else -> 0
    -- =========================================================================
    FUNCTION get_txn_type_segment (p_line_id IN NUMBER) RETURN VARCHAR2 IS
        l_tax_code VARCHAR2(100);
    BEGIN
        SELECT LOWER(jtl.tax_rate_code) INTO l_tax_code
          FROM apps.jai_tax_lines_all@legacy_instance jtl
         WHERE jtl.trx_line_id = p_line_id
           AND jtl.entity_code = 'OE_ORDER_HEADERS'
           AND ROWNUM = 1;
        IF l_tax_code LIKE 'cgst%' OR l_tax_code LIKE 'sgst%' THEN
            RETURN '1';
        ELSIF l_tax_code LIKE 'igst%' THEN
            RETURN '2';
        ELSE
            RETURN '0';
        END IF;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN RETURN '0';
    END get_txn_type_segment;

    -- =========================================================================
    -- [CHG-2] NEW: Validate AR period is OPEN for a given date and org
    -- Returns TRUE if open, FALSE if closed/missing
    -- [CHG-39] FIX: Use TRUNC(p_date) to strip time component. Period end_date
    --          is stored as midnight (00:00:00) of the last day, so any shipment
    --          date with time > 00:00:00 on the last day would fail BETWEEN.
    -- =========================================================================
    FUNCTION is_period_open (
        p_date   IN DATE,
        p_org_id IN NUMBER
    ) RETURN BOOLEAN IS
        l_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO l_count
          FROM apps.gl_period_statuses gps
         WHERE gps.application_id = 222       -- AR
           AND gps.ledger_id      = (SELECT set_of_books_id
                                       FROM apps.ar_system_parameters_all
                                      WHERE org_id = p_org_id
                                        AND ROWNUM = 1)
           AND gps.closing_status = 'O'       -- Open
           AND TRUNC(p_date) BETWEEN gps.start_date AND gps.end_date;  -- [CHG-39]
        RETURN (l_count > 0);
    EXCEPTION
        WHEN OTHERS THEN RETURN FALSE;
    END is_period_open;

    -- =========================================================================
    -- [CHG-3] NEW: Validate customer exists and is active in new instance
    -- Returns TRUE if found, FALSE if missing/inactive
    -- =========================================================================
    FUNCTION customer_exists (p_cust_account_id IN NUMBER) RETURN BOOLEAN IS
        l_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO l_count
          FROM apps.hz_cust_accounts
         WHERE cust_account_id = p_cust_account_id
           AND status          = 'A';
        RETURN (l_count > 0);
    EXCEPTION
        WHEN OTHERS THEN RETURN FALSE;
    END customer_exists;

    -- =========================================================================
    -- Main Interface Procedure
    -- =========================================================================
    PROCEDURE run_interface (
        ERRBUF OUT VARCHAR2,
        RETCODE OUT NUMBER,
        p_ship_date_from IN VARCHAR2 DEFAULT NULL,
        p_ship_date_to   IN VARCHAR2 DEFAULT NULL,
        p_submit_autoinv IN VARCHAR2 DEFAULT 'Y'

    ) IS
        l_run_id            NUMBER;
        -- [CHG-45] 15-min interval: floor current IST time to previous 15-min boundary
        -- DB SYSDATE is UTC; convert to IST (+5:30) before flooring
        l_now_ist           DATE   := CAST(SYSTIMESTAMP AT TIME ZONE 'Asia/Calcutta' AS DATE);
        l_ship_to           DATE   := NVL(TO_DATE(p_ship_date_to, 'YYYY/MM/DD HH24:MI:SS'),
                                          TRUNC(l_now_ist) + (FLOOR((l_now_ist - TRUNC(l_now_ist)) * 24 * 60 / 15) * 15) / (24 * 60));
        l_ship_from         DATE   := NVL(TO_DATE(p_ship_date_from, 'YYYY/MM/DD HH24:MI:SS'),
                                          l_ship_to - (15 / (24 * 60)));
        l_lines_extracted   NUMBER := 0;
        l_lines_inserted    NUMBER := 0;
        l_lines_skipped     NUMBER := 0;                        -- [CHG-4]
        l_tax_lines_inserted NUMBER := 0;                       -- [CHG-22]
        l_interface_line_id NUMBER;

    CURSOR c_om_lines IS
        -- Branch 1: Lines with actual shipment date in range
        SELECT /*+ DRIVING_SITE(l) */
            h.header_id,
            h.order_number,
            h.order_type_id,
            h.sold_to_org_id,
            h.salesrep_id               AS legacy_salesrep_id,
            h.transactional_curr_code,
            h.ordered_date,
            l.line_id,
            l.line_number,
            l.ordered_item,
            l.inventory_item_id,
            l.ordered_quantity,
            l.order_quantity_uom,
            l.unit_selling_price,
            l.unit_list_price,
            l.invoice_to_org_id,
            l.ship_to_org_id,
            l.ship_from_org_id,
            l.actual_shipment_date,
            l.fulfillment_date,
            l.line_category_code,
            l.reference_header_id,
            l.org_id,
            gcc.segment1                AS old_company,
            gcc.segment2                AS old_cost_center,
            gcc.segment3                AS old_product,
            gcc.segment4                AS old_account,
            gcc.segment5                AS old_project,
            gcc.segment6                AS old_future1,
            gcc.segment7                AS old_future2,
            hca.account_number          AS legacy_account_number,
            msib.description            AS item_description
        FROM ont.oe_order_lines_all@legacy_instance         l
        JOIN ont.oe_order_headers_all@legacy_instance       h
            ON  l.header_id             = h.header_id
        JOIN ar.hz_cust_accounts@legacy_instance            hca
            ON  hca.cust_account_id     = h.sold_to_org_id
        LEFT JOIN inv.mtl_system_items_b@legacy_instance    msib
            ON  msib.inventory_item_id  = l.inventory_item_id
            AND msib.organization_id    = l.ship_from_org_id
        LEFT JOIN (
            -- Deduplicate to avoid row multiplication
            SELECT DISTINCT
                tt.transaction_type_id,
                gcc_inner.segment1, gcc_inner.segment2, gcc_inner.segment3,
                gcc_inner.segment4, gcc_inner.segment5, gcc_inner.segment6,
                gcc_inner.segment7
            FROM   ont.oe_transaction_types_all@legacy_instance     tt
            LEFT JOIN ar.ra_cust_trx_types_all@legacy_instance      ctt
                ON  ctt.cust_trx_type_id    = tt.cust_trx_type_id
            LEFT JOIN gl.gl_code_combinations@legacy_instance        gcc_inner
                ON  gcc_inner.code_combination_id = ctt.gl_id_rev
        ) gcc ON gcc.transaction_type_id = h.order_type_id
        WHERE l.flow_status_code        = 'CLOSED'
            AND l.actual_shipment_date  BETWEEN l_ship_from AND l_ship_to

        UNION ALL

        -- Branch 2: Lines with NULL shipment date but fulfillment date in range
        SELECT /*+ DRIVING_SITE(l) */
            h.header_id,
            h.order_number,
            h.order_type_id,
            h.sold_to_org_id,
            h.salesrep_id               AS legacy_salesrep_id,
            h.transactional_curr_code,
            h.ordered_date,
            l.line_id,
            l.line_number,
            l.ordered_item,
            l.inventory_item_id,
            l.ordered_quantity,
            l.order_quantity_uom,
            l.unit_selling_price,
            l.unit_list_price,
            l.invoice_to_org_id,
            l.ship_to_org_id,
            l.ship_from_org_id,
            l.actual_shipment_date,
            l.fulfillment_date,
            l.line_category_code,
            l.reference_header_id,
            l.org_id,
            gcc.segment1                AS old_company,
            gcc.segment2                AS old_cost_center,
            gcc.segment3                AS old_product,
            gcc.segment4                AS old_account,
            gcc.segment5                AS old_project,
            gcc.segment6                AS old_future1,
            gcc.segment7                AS old_future2,
            hca.account_number          AS legacy_account_number,
            msib.description            AS item_description
        FROM ont.oe_order_lines_all@legacy_instance         l
        JOIN ont.oe_order_headers_all@legacy_instance       h
            ON  l.header_id             = h.header_id
        JOIN ar.hz_cust_accounts@legacy_instance            hca
            ON  hca.cust_account_id     = h.sold_to_org_id
        LEFT JOIN inv.mtl_system_items_b@legacy_instance    msib
            ON  msib.inventory_item_id  = l.inventory_item_id
            AND msib.organization_id    = l.ship_from_org_id
        LEFT JOIN (
            SELECT DISTINCT
                tt.transaction_type_id,
                gcc_inner.segment1, gcc_inner.segment2, gcc_inner.segment3,
                gcc_inner.segment4, gcc_inner.segment5, gcc_inner.segment6,
                gcc_inner.segment7
            FROM   ont.oe_transaction_types_all@legacy_instance     tt
            LEFT JOIN ar.ra_cust_trx_types_all@legacy_instance      ctt
                ON  ctt.cust_trx_type_id    = tt.cust_trx_type_id
            LEFT JOIN gl.gl_code_combinations@legacy_instance        gcc_inner
                ON  gcc_inner.code_combination_id = ctt.gl_id_rev
        ) gcc ON gcc.transaction_type_id = h.order_type_id
        WHERE l.flow_status_code        = 'CLOSED'
            AND l.actual_shipment_date  IS NULL
            AND l.fulfillment_date      BETWEEN l_ship_from AND l_ship_to;


        -- [CHG-22] Cursor: fetch tax components from legacy JAI for a given OM line
        CURSOR c_tax_lines (p_line_id IN NUMBER) IS
            SELECT tax_line_num AS tax_line_no,
                   tax_rate_code,
                   tax_rate_percentage,
                   rounded_tax_amt_trx_curr
              FROM apps.jai_tax_lines_all@legacy_instance
             WHERE entity_code  = 'OE_ORDER_HEADERS'
               AND trx_line_id  = p_line_id
               AND NVL(rounded_tax_amt_trx_curr, 0) > 0
             ORDER BY tax_line_num;

    BEGIN
        SELECT apps.bamul_om_ar_log_s.NEXTVAL INTO l_run_id FROM dual;
        INSERT INTO apps.bamul_om_ar_interface_log
            (run_id, ship_date_from, ship_date_to, status)
        VALUES (l_run_id, l_ship_from, l_ship_to, 'RUNNING');
        COMMIT;

        -- [CHG-29/34] Set MOAC context so VPD policy on ra_interface_salescredits
        -- allows inserts. Without this the insert silently fails with ORA-28115.

        fnd_global.apps_initialize(
        user_id      => g_user_id,
        resp_id      => fnd_global.resp_id,
        resp_appl_id => fnd_global.resp_appl_id
        );

        mo_global.set_policy_context('S', g_org_id);

        DBMS_OUTPUT.PUT_LINE('=============================================='); -- [CHG-15]
        DBMS_OUTPUT.PUT_LINE('BAMUL OM to AR Interface');
        DBMS_OUTPUT.PUT_LINE('Run ID     : ' || l_run_id);
        DBMS_OUTPUT.PUT_LINE('Ship From  : ' || TO_CHAR(l_ship_from, 'DD-MON-YYYY HH24:MI:SS'));
        DBMS_OUTPUT.PUT_LINE('Ship To    : ' || TO_CHAR(l_ship_to,   'DD-MON-YYYY HH24:MI:SS'));
        DBMS_OUTPUT.PUT_LINE('==============================================');

        -- =====================================================================
        -- [CHG-5] PRE-FLIGHT: Check AR period is open for the date range
        -- =====================================================================
        DBMS_OUTPUT.PUT_LINE('--- Pre-flight: Period Open Check ---');
        IF NOT is_period_open(l_ship_from, g_org_id) THEN
            DBMS_OUTPUT.PUT_LINE('WARNING: AR period CLOSED for ship_date_from='
                || TO_CHAR(l_ship_from, 'DD-MON-YYYY') || '. Lines in this period will be skipped.');
        ELSE
            DBMS_OUTPUT.PUT_LINE('OK: AR period OPEN for ' || TO_CHAR(l_ship_from, 'DD-MON-YYYY'));
        END IF;

        IF NOT is_period_open(l_ship_to, g_org_id) THEN
            DBMS_OUTPUT.PUT_LINE('WARNING: AR period CLOSED for ship_date_to='
                || TO_CHAR(l_ship_to, 'DD-MON-YYYY') || '. Lines in this period will be skipped.');
        ELSE
            DBMS_OUTPUT.PUT_LINE('OK: AR period OPEN for ' || TO_CHAR(l_ship_to, 'DD-MON-YYYY'));
        END IF;

        -- =====================================================================
        -- MAIN LOOP
        -- =====================================================================
        FOR rec IN c_om_lines LOOP
            l_lines_extracted := l_lines_extracted + 1;

            DECLARE
                l_new_entity     VARCHAR2(25) := '01';
                l_new_division   VARCHAR2(25) := get_division(rec.ship_from_org_id);
                l_new_account    NUMBER       := get_new_account(TO_NUMBER(NVL(rec.old_account, '0')));
                l_new_department VARCHAR2(30) := get_new_department(NVL(rec.old_cost_center, '00000'));
                l_new_product    VARCHAR2(8)  := get_new_product(rec.inventory_item_id);
                l_new_txn        VARCHAR2(25) := get_txn_type_segment(rec.line_id);
                l_new_future1    VARCHAR2(25) := '000';
                l_new_future2    VARCHAR2(25) := '000';
                l_cust_trx_type  VARCHAR2(240);
                l_batch_src      VARCHAR2(240);
                l_amount         NUMBER;
                l_new_cust_id    NUMBER;                         -- [CHG-35] target cust_account_id
                l_new_bill_to    NUMBER;                         -- [CHG-35] target bill-to site_use_id
                l_new_ship_to    NUMBER;                         -- [CHG-35] target ship-to site_use_id
                l_new_cust_ref   VARCHAR2(240);                  -- [CHG-36] target customer orig_system_reference
                l_new_bill_ref   VARCHAR2(240);                  -- [CHG-36] target bill-to site orig_system_reference
                l_new_ship_ref   VARCHAR2(240);                  -- target ship-to site orig_system_reference
                l_salesrep_id    NUMBER;                         -- [CHG-38] target salesrep_id
                l_salesrep_number VARCHAR2(30);                  -- [CHG-38] target salesrep_number
            BEGIN

                -- =============================================================
                -- [CHG-35] Resolve target customer by legacy account_number
                -- =============================================================
                BEGIN
                    SELECT cust_account_id, orig_system_reference
                      INTO l_new_cust_id, l_new_cust_ref
                      FROM hz_cust_accounts
                     WHERE account_number = rec.legacy_account_number
                       AND status = 'A';
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        l_new_cust_id := 1041;
                        l_new_cust_ref := '1041';
                        DBMS_OUTPUT.PUT_LINE('WARN line_id=' || rec.line_id
                            || ', order=' || rec.order_number
                            || ': Customer account_number=' || rec.legacy_account_number
                            || ' not found, defaulting to 1041');
                END;

                -- [CHG-35] Resolve target bill-to site_use_id (primary)
                BEGIN
                    SELECT hcsu.site_use_id, hcas.orig_system_reference
                      INTO l_new_bill_to, l_new_bill_ref
                      FROM hz_cust_site_uses_all hcsu
                      JOIN hz_cust_acct_sites_all hcas ON hcas.cust_acct_site_id = hcsu.cust_acct_site_id
                     WHERE hcas.cust_account_id = l_new_cust_id
                       AND hcsu.site_use_code = 'BILL_TO'
                       AND hcsu.status = 'A'
                       AND hcsu.primary_flag = 'Y'
                       AND ROWNUM = 1;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        DBMS_OUTPUT.PUT_LINE('SKIP line_id=' || rec.line_id
                            || ', order=' || rec.order_number
                            || ': No active primary BILL_TO site for account_number=' || rec.legacy_account_number);
                        l_lines_skipped := l_lines_skipped + 1;
                        GOTO next_line;
                END;

                -- [CHG-35] Resolve target ship-to site_use_id (primary)
                -- [CHG-41] If no ship-to found, set to NULL (skip ship-to derivation)
                BEGIN
                    SELECT hcsu.site_use_id, hcas.orig_system_reference
                      INTO l_new_ship_to, l_new_ship_ref
                      FROM hz_cust_site_uses_all hcsu
                      JOIN hz_cust_acct_sites_all hcas ON hcas.cust_acct_site_id = hcsu.cust_acct_site_id
                     WHERE hcas.cust_account_id = l_new_cust_id
                       AND hcsu.site_use_code = 'SHIP_TO'
                       AND hcsu.status = 'A'
                       AND hcsu.primary_flag = 'Y'
                       AND ROWNUM = 1;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        l_new_ship_to := NULL;
                        l_new_ship_ref := NULL;
                END;

                -- =============================================================
                -- [CHG-38] Resolve target salesrep by matching legacy salesrep name
                -- =============================================================
                BEGIN
                    SELECT rs_target.salesrep_id, rs_target.salesrep_number
                      INTO l_salesrep_id, l_salesrep_number
                      FROM apps.jtf_rs_salesreps@legacy_instance rs_legacy
                      JOIN apps.jtf_rs_salesreps rs_target
                        ON rs_target.name = rs_legacy.name
                       AND rs_target.org_id = g_org_id
                       AND rs_target.status = 'A'
                     WHERE rs_legacy.salesrep_id = rec.legacy_salesrep_id
                       AND ROWNUM = 1;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        l_salesrep_id := -3;
                        l_salesrep_number := '-3';
                END;

                -- =============================================================
                -- [CHG-6] Validation: Period open for this line's shipment date
                -- =============================================================
                IF NOT is_period_open(NVL(rec.actual_shipment_date, rec.fulfillment_date), g_org_id) THEN
                    DBMS_OUTPUT.PUT_LINE('SKIP line_id=' || rec.line_id
                        || ', order=' || rec.order_number
                        || ': AR period CLOSED for ' || TO_CHAR(NVL(rec.actual_shipment_date, rec.fulfillment_date), 'DD-MON-YYYY'));
                    l_lines_skipped := l_lines_skipped + 1;
                    GOTO next_line;
                END IF;

                -- =============================================================
                -- [CHG-7] Customer validation skipped for now
                -- TODO: Add mapping_customer lookup before go-live
                -- =============================================================

                -- =============================================================
                -- [CHG-8] Validation: Order type mapping with GOTO on miss
                -- [CHG-43] Dynamic CM resolution for RETURN lines:
                --   Look up original order via reference_header_id, get its
                --   INV type from mapping_order_type, then derive CM type.
                -- =============================================================
                IF rec.line_category_code = 'RETURN' THEN
                    DECLARE
                        l_orig_order_type_id NUMBER;
                        l_orig_inv_type      VARCHAR2(240);
                    BEGIN
                        -- Get original order's order_type_id via reference_header_id
                        SELECT h_orig.order_type_id
                          INTO l_orig_order_type_id
                          FROM ont.oe_order_headers_all@legacy_instance h_orig
                         WHERE h_orig.header_id = rec.reference_header_id;

                        -- Get the INV type for the original order
                        SELECT new_cust_trx_type_name
                          INTO l_orig_inv_type
                          FROM apps.mapping_order_type
                         WHERE old_order_type_id = l_orig_order_type_id
                           AND enabled_flag = 'Y';

                        -- Derive CM type from INV type
                        l_cust_trx_type := CASE l_orig_inv_type
                            WHEN 'INV MILK ' || CHR(38) || ' MILK PROD'  THEN 'CM MILK ' || CHR(38) || ' MILK PROD'
                            WHEN 'INV for KMF,KMF UNIT'  THEN 'CM KMF,KMF UNITS ' || CHR(38)
                            WHEN 'Invoice FROM MPCS'     THEN 'CM FROM MPCS'
                            WHEN 'INV for OTHER RECEIV'  THEN 'CM OTHER RECEIVABLE'
                            WHEN 'INV FOR SERVICE RECE'  THEN 'CM SERVICE RECEIPIEN'
                            ELSE 'CM MILK ' || CHR(38) || ' MILK PROD'
                        END;

                        -- ASD special case
                        IF UPPER(rec.ordered_item) = 'ASD' THEN
                            l_cust_trx_type := 'Credit ASD Return';
                        END IF;

                        l_batch_src := 'BAMUL_OM_IMPORT';

                    EXCEPTION
                        WHEN NO_DATA_FOUND THEN
                            DBMS_OUTPUT.PUT_LINE('SKIP line_id=' || rec.line_id
                                || ', order=' || rec.order_number
                                || ': Cannot resolve CM type - no reference order or mapping for RETURN line');
                            l_lines_skipped := l_lines_skipped + 1;
                            GOTO next_line;
                    END;
                ELSE
                    BEGIN
                        SELECT new_cust_trx_type_name, new_batch_source_name
                          INTO l_cust_trx_type, l_batch_src
                          FROM apps.mapping_order_type
                         WHERE old_order_type_id = rec.order_type_id
                           AND enabled_flag = 'Y';
                    EXCEPTION
                        WHEN NO_DATA_FOUND THEN
                            DBMS_OUTPUT.PUT_LINE('SKIP line_id=' || rec.line_id
                                || ', order=' || rec.order_number
                                || ': No order type mapping for order_type_id=' || rec.order_type_id);
                            l_lines_skipped := l_lines_skipped + 1;
                            GOTO next_line;
                    END;
                END IF;

                -- Override account for ASD items (liability account, not revenue)
                IF UPPER(rec.ordered_item) = 'ASD' THEN
                    l_new_account := 274002;
                END IF;

                -- =============================================================
                -- [CHG-42] Duplicate prevention: skip if line already in
                -- interface table (pending/errored) or already invoiced
                -- =============================================================
                DECLARE
                    l_dup_count NUMBER;
                BEGIN
                    -- Check interface table (pending/errored lines)
                    SELECT COUNT(*) INTO l_dup_count
                      FROM ra_interface_lines_all
                     WHERE interface_line_context    = 'BAMUL_OM_IMPORT'
                       AND interface_line_attribute1 = TO_CHAR(rec.order_number)
                       AND interface_line_attribute2 = TO_CHAR(rec.line_id)
                       AND interface_line_attribute3 = TO_CHAR(rec.line_number);

                    IF l_dup_count > 0 THEN
                        DBMS_OUTPUT.PUT_LINE('SKIP line_id=' || rec.line_id
                            || ', order=' || rec.order_number
                            || ': Already in interface table (pending/errored)');
                        l_lines_skipped := l_lines_skipped + 1;
                        GOTO next_line;
                    END IF;

                    -- Check if already converted to invoice
                    SELECT COUNT(*) INTO l_dup_count
                      FROM ra_customer_trx_lines_all
                     WHERE interface_line_context    = 'BAMUL_OM_IMPORT'
                       AND interface_line_attribute1 = TO_CHAR(rec.order_number)
                       AND interface_line_attribute2 = TO_CHAR(rec.line_id)
                       AND interface_line_attribute3 = TO_CHAR(rec.line_number);

                    IF l_dup_count > 0 THEN
                        DBMS_OUTPUT.PUT_LINE('SKIP line_id=' || rec.line_id
                            || ', order=' || rec.order_number
                            || ': Already invoiced');
                        l_lines_skipped := l_lines_skipped + 1;
                        GOTO next_line;
                    END IF;
                END;

                -- [CHG-13] Use ROUND to avoid amount precision mismatch
                l_amount := ROUND(rec.ordered_quantity * rec.unit_selling_price, 2);
                IF rec.line_category_code = 'RETURN' THEN     -- [CHG-31]
                    l_amount := l_amount * -1;
                END IF;

                SELECT ra_customer_trx_lines_s.NEXTVAL INTO l_interface_line_id FROM dual;

                INSERT INTO ar.ra_interface_lines_all (
                    interface_line_id,
                    interface_line_context,
                    interface_line_attribute1,
                    interface_line_attribute2,
                    interface_line_attribute3,
                    interface_line_attribute4,
                    interface_line_attribute5,
                    interface_line_attribute6,
                    interface_line_attribute7,
                    interface_line_attribute8,
                    interface_line_attribute9,
                    interface_line_attribute10,
                    interface_line_attribute11,
                    interface_line_attribute12,
                    interface_line_attribute13,
                    interface_line_attribute14,
                    batch_source_name,
                    line_type,
                    description,
                    currency_code,
                    amount,
                    cust_trx_type_name,
                    orig_system_bill_customer_ref,
                    orig_system_bill_address_ref,
                    orig_system_ship_customer_ref,
                    orig_system_ship_address_ref,
                    conversion_type,            -- [CHG-9] 'User' with explicit rate=1
                    conversion_rate,            -- [CHG-9] rate=1 for INR->INR
                    conversion_date,            -- [CHG-9] actual shipment date from legacy
                    trx_date,
                    gl_date,
                    quantity,
                    quantity_ordered,
                    unit_selling_price,
                    unit_standard_price,
                    uom_code,
                    inventory_item_id,          -- [CHG-12] set to NULL below
                    sales_order,
                    sales_order_line,
                    sales_order_date,
                    ship_date_actual,
                    warehouse_id,
                    org_id,
                    term_id,                    -- [CHG-10] added
                    term_name,                  -- [CHG-10] added
                    primary_salesrep_id,        -- [CHG-11] added
                    primary_salesrep_number,    -- [CHG-11] added
                    line_gdf_attr_category,     -- [CHG-37] JAI OFI TAX IMPORT flag
                    line_gdf_attribute19,       -- [CHG-37] TRANSACTION_NUM for JAI lookup
                    line_gdf_attribute20,       -- [CHG-37] TRANSACTION_LINE_NUM for JAI lookup
                    taxable_flag,               -- [CHG-37] Y = line is taxable for JAI
                    created_by,
                    creation_date,
                    last_updated_by,
                    last_update_date,
                    last_update_login
                ) VALUES (
                    l_interface_line_id,
                    'BAMUL_OM_IMPORT',
                    -- l_order_number,
                    TO_CHAR(rec.order_number),
                    TO_CHAR(rec.line_id),
                    TO_CHAR(rec.line_number),
                    '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', -- attr4-14
                    l_batch_src,
                    'LINE',
                    rec.ordered_item || '-' || NVL(rec.item_description, rec.ordered_item),
                    rec.transactional_curr_code,
                    l_amount,
                    l_cust_trx_type,
                    l_new_cust_ref,             -- [CHG-36] target customer orig_system_reference
                    l_new_bill_ref,             -- [CHG-36] target bill-to site orig_system_reference
                    CASE WHEN l_new_ship_ref IS NULL THEN NULL ELSE l_new_cust_ref END,  -- orig_system_ship_customer_ref [CHG-41]
                    l_new_ship_ref,             -- orig_system_ship_address_ref
                    'User',                     -- [CHG-9] User type with explicit rate
                    1,                          -- [CHG-9] rate=1 (INR->INR, no conversion)
                    NVL(rec.actual_shipment_date, rec.fulfillment_date),   -- [CHG-30] conversion date
                    NVL(rec.actual_shipment_date, rec.fulfillment_date),   -- trx_date
                    NVL(rec.actual_shipment_date, rec.fulfillment_date),   -- gl_date
                    CASE WHEN rec.line_category_code = 'RETURN' THEN NULL ELSE rec.ordered_quantity END,       -- [CHG-31]
                    CASE WHEN rec.line_category_code = 'RETURN' THEN NULL ELSE rec.ordered_quantity END,       -- [CHG-31]
                    CASE WHEN rec.line_category_code = 'RETURN' THEN NULL ELSE rec.unit_selling_price END,     -- [CHG-31]
                    CASE WHEN rec.line_category_code = 'RETURN' THEN NULL ELSE rec.unit_list_price END,        -- [CHG-31]
                    rec.order_quantity_uom,
                    NULL,                       -- [CHG-12] NULL to skip item validation org check
                    -- l_order_number,
                    TO_CHAR(rec.order_number),                  -- sales_order
                    TO_CHAR(rec.line_number),
                    rec.ordered_date,
                    NVL(rec.actual_shipment_date, rec.fulfillment_date),   -- [CHG-30] ship_date_actual
                    NULL,                       -- [CHG-19] warehouse_id=NULL
                    g_org_id,                   -- [CHG-14] constant org_id
                    CASE WHEN rec.line_category_code = 'RETURN' THEN NULL ELSE 5 END,            -- [CHG-31]
                    CASE WHEN rec.line_category_code = 'RETURN' THEN NULL ELSE 'IMMEDIATE' END,  -- [CHG-31]
                    l_salesrep_id,              -- [CHG-38] mapped salesrep_id
                    l_salesrep_number,          -- [CHG-38] mapped salesrep_number
                    'JG.IN.ARXTWMAI.OFI TAX IMPORT',  -- [CHG-37] triggers JAI hook
                    TO_CHAR(rec.order_number),         -- [CHG-37] TRANSACTION_NUM for JAI lookup
                    TO_CHAR(rec.line_number),          -- [CHG-37] TRANSACTION_LINE_NUM for JAI lookup
                    'Y',                              -- [CHG-37] taxable_flag
                    g_user_id,
                    SYSDATE,
                    g_user_id,
                    SYSDATE,
                    g_login_id
                );

                INSERT INTO ar.ra_interface_distributions_all (
                    interface_line_id,
                    interface_line_context,
                    interface_line_attribute1,
                    interface_line_attribute2,
                    interface_line_attribute3,
                    interface_line_attribute4,
                    interface_line_attribute5,
                    interface_line_attribute6,
                    interface_line_attribute7,
                    interface_line_attribute8,
                    interface_line_attribute9,
                    interface_line_attribute10,
                    interface_line_attribute11,
                    interface_line_attribute12,
                    interface_line_attribute13,
                    interface_line_attribute14,
                    account_class,
                    amount,
                    percent,
                    segment1,
                    segment2,
                    segment3,
                    segment4,
                    segment5,
                    segment6,
                    segment7,
                    segment8,
                    org_id,
                    created_by,
                    creation_date,
                    last_updated_by,
                    last_update_date,
                    last_update_login
                ) VALUES (
                    l_interface_line_id,
                    'BAMUL_OM_IMPORT',
                    -- l_order_number,
                    TO_CHAR(rec.order_number),
                    TO_CHAR(rec.line_id),
                    TO_CHAR(rec.line_number),
                    '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', -- attr4-14
                    'REV',
                    l_amount,                   -- [CHG-13] matches rounded amount
                    100,
                    l_new_entity,               -- SEGMENT1: ENTITY
                    l_new_division,             -- SEGMENT2: DIVISION
                    l_new_account,              -- SEGMENT3: ACCOUNT
                    l_new_department,           -- SEGMENT4: DEPARTMENT
                    l_new_product,              -- SEGMENT5: PRODUCT
                    l_new_txn,                  -- SEGMENT6: TXN TYPE
                    l_new_future1,              -- SEGMENT7: FUTURE1
                    l_new_future2,              -- SEGMENT8: FUTURE2
                    g_org_id,                   -- [CHG-14] constant org_id
                    g_user_id,
                    SYSDATE,
                    g_user_id,
                    SYSDATE,
                    g_login_id
                );

                l_lines_inserted := l_lines_inserted + 1;

                -- [CHG-20] Insert sales credit record
                -- Required when AR system option Require Salesreps = Y
                -- Must match all 14 interface_line_attributes of the line
                INSERT INTO ar.ra_interface_salescredits_all (
                    interface_line_context,
                    interface_line_attribute1,
                    interface_line_attribute2,
                    interface_line_attribute3,
                    interface_line_attribute4,
                    interface_line_attribute5,
                    interface_line_attribute6,
                    interface_line_attribute7,
                    interface_line_attribute8,
                    interface_line_attribute9,
                    interface_line_attribute10,
                    interface_line_attribute11,
                    interface_line_attribute12,
                    interface_line_attribute13,
                    interface_line_attribute14,
                    salesrep_number,
                    sales_credit_type_name,
                    sales_credit_type_id,
                    sales_credit_percent_split,
                    org_id,
                    created_by, creation_date,
                    last_updated_by, last_update_date, last_update_login
                ) VALUES (
                    'BAMUL_OM_IMPORT',
                    -- l_order_number,
                    TO_CHAR(rec.order_number),
                    TO_CHAR(rec.line_id),
                    TO_CHAR(rec.line_number),
                    '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
                    l_salesrep_number,
                    'Quota Sales Credit',
                    1,
                    100,
                    g_org_id,
                    g_user_id, SYSDATE,
                    g_user_id, SYSDATE, g_login_id
                );

                IF MOD(l_lines_inserted, 500) = 0 THEN
                    COMMIT;
                    DBMS_OUTPUT.PUT_LINE('Processed ' || l_lines_inserted || ' lines...');
                END IF;


                -- ============================================================
                -- [CHG-37] JAI OFI TAX IMPORT: Populate JAI interface tables
                -- JAI hook fires DURING AutoInvoice via ZX when it sees
                -- GLOBAL_ATTRIBUTE_CATEGORY = 'JG.IN.ARXTWMAI.OFI TAX IMPORT'
                -- ============================================================
                DECLARE
                    l_party_id         NUMBER;
                    l_party_site_id    NUMBER;
                    l_tax_category_id  NUMBER;
                    l_jai_intf_line_id NUMBER;
                    l_total_tax        NUMBER := 0;
                    l_trx_num          VARCHAR2(60) := TO_CHAR(rec.order_number);
                    l_trx_line_num     VARCHAR2(60) := TO_CHAR(rec.line_number);
                    l_bmo_org_id       NUMBER;
                    l_bmo_location_id  NUMBER;
                BEGIN
                    -- Check total tax amount for this line
                    SELECT NVL(SUM(rounded_tax_amt_trx_curr), 0)
                      INTO l_total_tax
                      FROM apps.jai_tax_lines_all@legacy_instance
                     WHERE entity_code = 'OE_ORDER_HEADERS'
                       AND trx_line_id = rec.line_id;

                    -- Only insert JAI interface records if total tax > 0
                    IF l_total_tax > 0 THEN
                        -- Derive organization_id and location_id for BMO
                        SELECT hou.location_id, mp.organization_id
                          INTO l_bmo_location_id, l_bmo_org_id
                          FROM mtl_parameters mp
                          JOIN hr_all_organization_units hou ON hou.organization_id = mp.organization_id
                         WHERE ROWNUM = 1;

                        -- party_id = cust_account_id
                        l_party_id := l_new_cust_id;

                        -- party_site_id = cust_acct_site_id
                        SELECT hcas.cust_acct_site_id
                          INTO l_party_site_id
                          FROM hz_cust_site_uses_all hcsu
                          JOIN hz_cust_acct_sites_all hcas ON hcas.cust_acct_site_id = hcsu.cust_acct_site_id
                         WHERE hcsu.site_use_id = l_new_bill_to;

                        -- Resolve tax_category_id from legacy jai_tax_det_factors by name matching
                        BEGIN
                            SELECT n1.tax_category_id
                              INTO l_tax_category_id
                              FROM apps.jai_tax_det_factors@legacy_instance jdf
                              JOIN apps.jai_tax_categories@legacy_instance l1 ON l1.tax_category_id = jdf.default_tax_category_id
                              JOIN apps.jai_tax_categories n1 ON n1.tax_category_name = l1.tax_category_name
                             WHERE jdf.entity_code = 'OE_ORDER_HEADERS'
                               AND jdf.trx_id = rec.header_id
                               AND jdf.trx_line_id = rec.line_id
                               AND jdf.default_tax_category_id IS NOT NULL
                               AND ROWNUM = 1;
                        EXCEPTION
                            WHEN NO_DATA_FOUND THEN
                                l_tax_category_id := NULL;
                        END;

                        -- Insert JAI_INTERFACE_LINES_ALL (header for this trx line)
                        SELECT ja.jai_interface_lines_all_s.NEXTVAL
                          INTO l_jai_intf_line_id FROM dual;

                        INSERT INTO ja.jai_interface_lines_all (
                            interface_line_id,
                            org_id,
                            organization_id,
                            location_id,
                            party_id,
                            party_site_id,
                            tax_category_id,
                            batch_source_name,
                            import_module,
                            transaction_num,
                            transaction_line_num,
                            taxable_basis,
                            taxable_event,
                            exclusive_tax_amount,
                            created_by, creation_date,
                            last_updated_by, last_update_date, last_update_login
                        ) VALUES (
                            l_jai_intf_line_id,
                            g_org_id,
                            l_bmo_org_id,       -- organization_id (BMO)
                            l_bmo_location_id,  -- location_id
                            l_party_id,         -- cust_account_id
                            l_party_site_id,    -- cust_acct_site_id
                            l_tax_category_id,
                            l_batch_src,
                            'AR',
                            l_trx_num,
                            l_trx_line_num,
                            ABS(l_amount),
                            'STANDARD',
                            NULL,               -- exclusive_tax_amount populated below
                            g_user_id, SYSDATE,
                            g_user_id, SYSDATE, g_login_id
                        );

                        -- Insert JAI_INTERFACE_TAX_LINES_ALL for each tax component
                        FOR tax_rec IN c_tax_lines(rec.line_id) LOOP
                            INSERT INTO ja.jai_interface_tax_lines_all (
                                interface_tax_line_id,
                                interface_line_id,
                                party_id,
                                party_site_id,
                                import_module,
                                transaction_num,
                                transaction_line_num,
                                tax_line_no,
                                precedence_1,
                                tax_id,
                                tax_rate,
                                tax_amount,
                                func_tax_amount,
                                base_tax_amount,
                                inclusive_tax_flag,
                                created_by, creation_date,
                                last_updated_by, last_update_date, last_update_login
                            ) VALUES (
                                ja.jai_interface_tax_lines_all_s.NEXTVAL,
                                l_jai_intf_line_id,
                                l_party_id,
                                l_party_site_id,
                                'AR',
                                l_trx_num,
                                l_trx_line_num,
                                tax_rec.tax_line_no,
                                0,              -- precedence_1 = 0 (base tax amount)
                                (SELECT jtr.tax_rate_id FROM ja.jai_tax_rates jtr
                                  WHERE jtr.tax_rate_code = tax_rec.tax_rate_code AND ROWNUM = 1),
                                tax_rec.tax_rate_percentage,
                                tax_rec.rounded_tax_amt_trx_curr,
                                tax_rec.rounded_tax_amt_trx_curr,  -- func = trx (INR=INR)
                                ABS(l_amount),                     -- base = taxable amount
                                'N',
                                g_user_id, SYSDATE,
                                g_user_id, SYSDATE, g_login_id
                            );
                            l_tax_lines_inserted := l_tax_lines_inserted + 1;
                        END LOOP;

                        -- Update exclusive_tax_amount on the header line
                        UPDATE ja.jai_interface_lines_all
                           SET exclusive_tax_amount = (
                               SELECT NVL(SUM(tax_amount), 0)
                                 FROM ja.jai_interface_tax_lines_all
                                WHERE interface_line_id = l_jai_intf_line_id
                           )
                         WHERE interface_line_id = l_jai_intf_line_id;

                    END IF; -- l_total_tax > 0

                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('ERROR JAI interface for line_id=' || rec.line_id
                            || ': ' || SQLERRM);
                END;
                <<next_line>>
                NULL;

            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('ERROR line_id=' || rec.line_id
                        || ', order=' || rec.order_number || ': ' || SQLERRM);
                    l_lines_skipped := l_lines_skipped + 1;
            END;
        END LOOP;

        COMMIT;

        UPDATE apps.bamul_om_ar_interface_log
           SET lines_extracted = l_lines_extracted,
               lines_inserted  = l_lines_inserted,
               status          = 'COMPLETED'
         WHERE run_id = l_run_id;
        COMMIT;

        DBMS_OUTPUT.PUT_LINE('==============================================');
        DBMS_OUTPUT.PUT_LINE('Lines Extracted : ' || l_lines_extracted);
        DBMS_OUTPUT.PUT_LINE('Lines Inserted  : ' || l_lines_inserted);
        DBMS_OUTPUT.PUT_LINE('Tax Lines Ins   : ' || l_tax_lines_inserted); -- [CHG-22]
        DBMS_OUTPUT.PUT_LINE('Lines Skipped   : ' || l_lines_skipped); -- [CHG-4]
        DBMS_OUTPUT.PUT_LINE('Status          : COMPLETED');
        DBMS_OUTPUT.PUT_LINE('==============================================');

        -- =====================================================================
        -- [CHG-46] OM to GL COGS Interface
        -- Called before AutoInvoice so COGS journals are staged in GL_INTERFACE
        -- before AR invoices are created. Journal Import (GLLEZL) is submitted
        -- only when the GL package returns a valid group_id (retcode 0 or 1).
        -- =====================================================================
        IF l_lines_inserted > 0 THEN
            DECLARE
                l_gl_errbuf        VARCHAR2(2000);
                l_gl_retcode       NUMBER;
                l_gl_group_id      NUMBER;
                l_request_id       NUMBER;
                l_sob_id           NUMBER;
                l_source_name      VARCHAR2(50);
                l_interface_run_id NUMBER;
                l_access_set_id    NUMBER;
            BEGIN
                DBMS_OUTPUT.PUT_LINE('==============================================');
                DBMS_OUTPUT.PUT_LINE('Calling OM to GL COGS Interface...');
                DBMS_OUTPUT.PUT_LINE('==============================================');

                apps.xxcust_om_gl_interface_pkg.run_interface(
                    p_errbuf            => l_gl_errbuf,
                    p_retcode           => l_gl_retcode,
                    p_group_id          => l_gl_group_id,
                    p_ar_batch_source   => 'BAMUL_OM_IMPORT',
                    p_invoice_date_from => TO_CHAR(l_ship_from, 'DD-MON-YYYY'),
                    p_invoice_date_to   => TO_CHAR(l_ship_to,   'DD-MON-YYYY'),
                    p_debug_mode        => 'N'
                );

                DBMS_OUTPUT.PUT_LINE('OM to GL Return Code: ' || l_gl_retcode);
                DBMS_OUTPUT.PUT_LINE('OM to GL Message    : ' || l_gl_errbuf);
                DBMS_OUTPUT.PUT_LINE('OM to GL Group ID   : ' || l_gl_group_id);

                IF l_gl_retcode IN (0, 1) AND l_gl_group_id IS NOT NULL THEN

                    -- Fetch ledger ID dynamically
                    SELECT MIN(set_of_books_id)
                      INTO l_sob_id
                      FROM apps.ar_system_parameters_all
                     WHERE set_of_books_id > 0;

                    -- Fetch GL source name dynamically
                    SELECT je_source_name
                      INTO l_source_name
                      FROM apps.gl_je_sources
                     WHERE user_je_source_name = 'XXCUST_OM_COGS'
                       AND ROWNUM = 1;

                    -- Insert control row required by GLLEZL
                    SELECT apps.gl_journal_import_s.NEXTVAL INTO l_interface_run_id FROM dual;

                    INSERT INTO apps.gl_interface_control (
                        je_source_name,
                        status,
                        interface_run_id,
                        group_id,
                        set_of_books_id
                    ) VALUES (
                        l_source_name,
                        'S',
                        l_interface_run_id,
                        l_gl_group_id,
                        l_sob_id
                    );
                    COMMIT;

                    -- Fetch data access set ID for the ledger
                    SELECT access_set_id
                      INTO l_access_set_id
                      FROM apps.gl_access_set_norm_assign
                     WHERE ledger_id = l_sob_id
                       AND ROWNUM = 1;

                    -- Submit Journal Import (GLLEZL)
                    l_request_id := fnd_request.submit_request(
                        application => 'SQLGL',
                        program     => 'GLLEZL',
                        description => 'Journal Import - COGS ' || TO_CHAR(SYSDATE, 'DD-MON-YYYY'),
                        start_time  => NULL,
                        sub_request => FALSE,
                        argument1   => TO_CHAR(l_interface_run_id),  -- Interface Run ID
                        argument2   => TO_CHAR(l_access_set_id),     -- Data Access Set ID
                        argument3   => 'N',                          -- Post Errors to Suspense
                        argument4   => '',                           -- From Date
                        argument5   => '',                           -- To Date
                        argument6   => 'N',                          -- Create Summary Journals
                        argument7   => 'N',                          -- Import DFF
                        argument8   => 'Y'                           -- Process
                    );
                    COMMIT;

                    IF l_request_id > 0 THEN
                        DBMS_OUTPUT.PUT_LINE('Journal Import submitted - Request ID: ' || l_request_id);
                    ELSE
                        DBMS_OUTPUT.PUT_LINE('ERROR: Journal Import submission failed');
                    END IF;

                ELSIF l_gl_retcode = 2 THEN
                    DBMS_OUTPUT.PUT_LINE('ERROR: OM to GL failed - ' || l_gl_errbuf);
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    -- Non-fatal: log the error but allow AutoInvoice to proceed
                    DBMS_OUTPUT.PUT_LINE('ERROR calling OM to GL: ' || SQLERRM);
                    NULL;
            END;
        END IF;
        -- =====================================================================
        -- End [CHG-46]
        -- =====================================================================

        -- [CHG-44] Submit AutoInvoice if new lines inserted OR pending lines exist
        IF NVL(p_submit_autoinv, 'Y') = 'Y' THEN
            DECLARE
                l_pending_count NUMBER;
            BEGIN
                SELECT COUNT(*) INTO l_pending_count
                  FROM ra_interface_lines_all
                 WHERE interface_line_context = 'BAMUL_OM_IMPORT'
                   AND interface_status IS NULL
                   AND org_id = g_org_id
                   AND ROWNUM = 1;

                IF l_lines_inserted > 0 OR l_pending_count > 0 THEN
                    submit_autoinvoice(
                        p_org_id            => g_org_id,
                        p_batch_source_name => 'BAMUL_OM_IMPORT'
                    );
                ELSE
                    DBMS_OUTPUT.PUT_LINE('No lines to process - AutoInvoice not submitted.');
                END IF;
            END;
        END IF;

        ERRBUF  := NULL;
        RETCODE := 0;  -- 0=Success, 1=Warning, 2=Error

    EXCEPTION
        WHEN OTHERS THEN
            DECLARE
                l_err_msg VARCHAR2(4000) := SQLERRM; -- [CHG-16] local var avoids ORA-00904
            BEGIN
                ROLLBACK;
                UPDATE apps.bamul_om_ar_interface_log
                   SET status        = 'ERROR',
                       error_message = SUBSTR(l_err_msg, 1, 4000)
                 WHERE run_id = l_run_id;
                COMMIT;
                ERRBUF  := l_err_msg;
                RETCODE := 2;  -- 2=Error
                DBMS_OUTPUT.PUT_LINE('FATAL ERROR: ' || l_err_msg);
            END;
    END run_interface;

    -- =========================================================================
    -- Submit AutoInvoice concurrent program
    -- =========================================================================
    PROCEDURE submit_autoinvoice (
        p_org_id            IN NUMBER,
        p_batch_source_name IN VARCHAR2
    ) IS
        l_request_id     NUMBER;
        l_batch_source_id NUMBER;
    BEGIN
        -- [CHG-17] Removed fnd_global.apps_initialize - not needed when called
        --          from within a running concurrent request context
        mo_global.set_policy_context('S', p_org_id);

        -- [CHG-44] Resolve batch_source_id from name - RAXMTR arg3 expects ID not name
        SELECT batch_source_id INTO l_batch_source_id
          FROM ra_batch_sources_all
         WHERE name = p_batch_source_name
           AND org_id = p_org_id
           AND status = 'A';

        l_request_id := fnd_request.submit_request(
            application => 'AR',
            program     => 'RAXMTR',
            description => 'BAMUL OM to AR - AutoInvoice',
            start_time  => NULL,
            sub_request => FALSE,
            argument1   => '1',                                    -- Number of Instances
            argument2   => TO_CHAR(p_org_id),                      -- Organization
            argument3   => TO_CHAR(l_batch_source_id),             -- Batch Source Id
            argument4   => p_batch_source_name,                    -- Batch Source Name
            argument5   => TO_CHAR(SYSDATE, 'YYYY/MM/DD HH24:MI:SS'), -- Default Date
            argument6   => NULL,                                   -- Transaction Flexfield
            argument7   => NULL,                                   -- Transaction Type
            argument8   => NULL,                                   -- (Low) Bill To Customer Number
            argument9   => NULL,                                   -- (High) Bill To Customer Number
            argument10  => NULL,                                   -- (Low) Bill To Customer Name
            argument11  => NULL,                                   -- (High) Bill To Customer Name
            argument12  => NULL,                                   -- (Low) GL Date
            argument13  => NULL,                                   -- (High) GL Date
            argument14  => NULL,                                   -- (Low) Ship Date
            argument15  => NULL,                                   -- (High) Ship Date
            argument16  => NULL,                                   -- (Low) Transaction Number
            argument17  => NULL,                                   -- (High) Transaction Number
            argument18  => NULL,                                   -- (Low) Sales Order Number
            argument19  => NULL,                                   -- (High) Sales Order Number
            argument20  => NULL,                                   -- (Low) Invoice Date
            argument21  => NULL,                                   -- (High) Invoice Date
            argument22  => NULL,                                   -- (Low) Ship To Customer Number
            argument23  => NULL,                                   -- (High) Ship To Customer Number
            argument24  => NULL,                                   -- (Low) Ship To Customer Name
            argument25  => NULL,                                   -- (High) Ship To Customer Name
            argument26  => 'Y',                                    -- Base Due Date on Trx Date
            argument27  => NULL                                    -- Due Date Adjustment Days
        );
        COMMIT;

        IF l_request_id > 0 THEN
            DBMS_OUTPUT.PUT_LINE('AutoInvoice submitted. Request ID: ' || l_request_id);
        ELSE
            DBMS_OUTPUT.PUT_LINE('ERROR: Failed to submit AutoInvoice');
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR submitting AutoInvoice: ' || SQLERRM);
    END submit_autoinvoice;

-- =========================================================================
-- Package Initialization Block
-- [CHG-1] Derive g_org_id from hr_all_organization_units at package load time
-- =========================================================================
BEGIN
    SELECT organization_id INTO g_org_id
      FROM apps.hr_all_organization_units
     WHERE name = 'BAMUL_OU';
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RAISE_APPLICATION_ERROR(-20001,
            'BAMUL_OU operating unit not found in hr_all_organization_units');
END bamul_om_ar_interface_pkg;