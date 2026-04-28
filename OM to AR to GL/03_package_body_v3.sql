-- =============================================================================
-- BAMUL OM to AR Interface - Package Body v3
-- Run on: TARGET (New EBS) instance as APPS user
--
-- CHANGES FROM ORIGINAL (03_package_body.sql):
-- [CHG-1] g_org_id derived dynamically from hr_operating_units WHERE name='BAMUL_OU'
-- [CHG-2] Added is_period_open() validation function (new)
-- [CHG-3] Added customer_exists() validation function (new)
-- [CHG-4] Added l_lines_skipped counter
-- [CHG-5] Pre-flight period open check before main loop
-- [CHG-6] Per-line period open check with GOTO skip
-- [CHG-7] Per-line customer exists check with GOTO skip
-- [CHG-8] Order type mapping moved into its own BEGIN/EXCEPTION block with GOTO
-- [CHG-9] conversion_type='User', conversion_rate=1, conversion_date=actual_shipment_date from legacy
-- [CHG-10] term_id/term_name added (IMMEDIATE = 5)
-- [CHG-11] primary_salesrep_id added (-3 = No Sales Credit)
-- [CHG-12] inventory_item_id set to NULL (avoids item validation org error)
-- [CHG-13] amount uses ROUND(qty * price, 2) to avoid precision mismatch
-- [CHG-14] org_id uses g_org_id constant instead of rec.org_id
-- [CHG-15] fnd_file replaced with DBMS_OUTPUT (works outside concurrent mgr)
-- [CHG-16] FATAL error handler uses local variable for SQLERRM (ORA-00904 fix)
-- [CHG-17] submit_autoinvoice: removed fnd_global.apps_initialize (not needed)
-- [CHG-18] submit_autoinvoice: uses g_org_id constant
-- [CHG-19] warehouse_id set to NULL (legacy ship_from_org not in new instance)
-- [CHG-20] Added RA_INTERFACE_SALESCREDITS insert (required when Require Salesreps=Y)
-- [CHG-21] Customer mapping via apps.mapping_customer table (old->new cust_account_id + sites)
-- [CHG-22] Tax components from JAI_TAX_LINES_ALL inserted as LINE type rows (no eBTax dependency)
-- [CHG-23] Tax rows use interface_line_attribute3 = line_number-tax_rate_code (e.g. 1-CGST-2.5) to keep flexfield unique
-- [CHG-24] Tax rows get REV distribution using same COA as parent item line
-- [CHG-25] Tax rows get sales credit row (Require Salesreps=Y applies to all LINE rows)
-- [CHG-26] flow_status_code filter changed to 'SHIPPED' only (removed 'FULFILLED')
-- [CHG-27] tax_exempt_flag='E', tax_exempt_reason_code='RESALE' on all LINE rows to prevent eBTax adding taxes
-- [CHG-28] TEST ONLY: l_order_number overrides rec.order_number with '12104745671' to avoid duplicate with already-posted invoice -- TODO: remove before go-live
-- [CHG-29] mo_global.set_policy_context called at start of run_interface to satisfy VPD policy on ra_interface_salescredits
-- =============================================================================
CREATE OR REPLACE PACKAGE BODY apps.bamul_om_ar_interface_pkg AS

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
           AND p_date BETWEEN gps.start_date AND gps.end_date;
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
        p_ship_date_from IN DATE DEFAULT NULL,
        p_ship_date_to   IN DATE DEFAULT NULL,
        p_submit_autoinv IN VARCHAR2 DEFAULT 'Y'
    ) IS
        l_run_id            NUMBER;
        l_ship_from         DATE   := NVL(p_ship_date_from, TRUNC(SYSDATE) - 1);
        l_ship_to           DATE   := NVL(p_ship_date_to,   TRUNC(SYSDATE) - 1) + 0.99999;
        l_lines_extracted   NUMBER := 0;
        l_lines_inserted    NUMBER := 0;
        l_lines_skipped     NUMBER := 0;                        -- [CHG-4]
        l_tax_lines_inserted NUMBER := 0;                       -- [CHG-22]
        l_interface_line_id NUMBER;

        CURSOR c_om_lines IS
            SELECT h.header_id, h.order_number, h.order_type_id,
                   h.sold_to_org_id, h.transactional_curr_code, h.ordered_date,
                   l.line_id, l.line_number, l.ordered_item, l.inventory_item_id,
                   l.ordered_quantity, l.order_quantity_uom,
                   l.unit_selling_price, l.unit_list_price,
                   l.invoice_to_org_id, l.ship_to_org_id, l.ship_from_org_id,
                   l.actual_shipment_date, l.org_id,
                   gcc.segment1 AS old_company,
                   gcc.segment2 AS old_cost_center,
                   gcc.segment3 AS old_product,
                   gcc.segment4 AS old_account,
                   gcc.segment5 AS old_project,
                   gcc.segment6 AS old_future1,
                   gcc.segment7 AS old_future2
              FROM ont.oe_order_lines_all@legacy_instance    l
              JOIN ont.oe_order_headers_all@legacy_instance  h   ON l.header_id       = h.header_id
              JOIN ont.oe_transaction_types_all@legacy_instance tt ON h.order_type_id = tt.transaction_type_id
              LEFT JOIN ar.ra_cust_trx_types_all@legacy_instance  ctt ON tt.cust_trx_type_id = ctt.cust_trx_type_id
              LEFT JOIN gl.gl_code_combinations@legacy_instance   gcc ON ctt.gl_id_rev        = gcc.code_combination_id
             WHERE l.flow_status_code = 'SHIPPED'
               AND l.actual_shipment_date BETWEEN l_ship_from AND l_ship_to;

        -- [CHG-22] Cursor: fetch tax components from legacy JAI for a given OM line
        CURSOR c_tax_lines (p_line_id IN NUMBER) IS
            SELECT tax_rate_code,
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

        -- [CHG-29] Set MOAC context so VPD policy on ra_interface_salescredits
        -- allows inserts. Without this the insert silently fails with ORA-28115.
        mo_global.set_policy_context('S', g_org_id);

        DBMS_OUTPUT.PUT_LINE('=============================================='); -- [CHG-15]
        DBMS_OUTPUT.PUT_LINE('BAMUL OM to AR Interface');
        DBMS_OUTPUT.PUT_LINE('Run ID     : ' || l_run_id);
        DBMS_OUTPUT.PUT_LINE('Ship From  : ' || TO_CHAR(l_ship_from, 'DD-MON-YYYY'));
        DBMS_OUTPUT.PUT_LINE('Ship To    : ' || TO_CHAR(l_ship_to,   'DD-MON-YYYY'));
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
                -- [CHG-28] TEST ONLY: override order number to avoid duplicate with already-posted invoice
                -- TODO: remove this before go-live
                l_order_number   VARCHAR2(50) := '1304745671';
            BEGIN

                -- =============================================================
                -- [CHG-6] Validation: Period open for this line's shipment date
                -- =============================================================
                IF NOT is_period_open(rec.actual_shipment_date, g_org_id) THEN
                    DBMS_OUTPUT.PUT_LINE('SKIP line_id=' || rec.line_id
                        || ', order=' || rec.order_number
                        || ': AR period CLOSED for ' || TO_CHAR(rec.actual_shipment_date, 'DD-MON-YYYY'));
                    l_lines_skipped := l_lines_skipped + 1;
                    GOTO next_line;
                END IF;

                -- =============================================================
                -- [CHG-7] Customer validation skipped for now
                -- TODO: Add mapping_customer lookup before go-live
                -- =============================================================

                -- =============================================================
                -- [CHG-8] Validation: Order type mapping with GOTO on miss
                -- =============================================================
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

                -- [CHG-13] Use ROUND to avoid amount precision mismatch
                l_amount := ROUND(rec.ordered_quantity * rec.unit_selling_price, 2);

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
                    orig_system_bill_customer_id,
                    orig_system_bill_address_id,
                    orig_system_ship_customer_id,
                    orig_system_ship_address_id,
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
                    tax_exempt_flag,            -- [CHG-27] prevent eBTax adding taxes
                    tax_exempt_reason_code,     -- [CHG-27] required when exempt
                    created_by,
                    creation_date,
                    last_updated_by,
                    last_update_date,
                    last_update_login
                ) VALUES (
                    l_interface_line_id,
                    'BAMUL_OM_IMPORT',
                    l_order_number,                             -- [CHG-28]
                    TO_CHAR(rec.line_id),
                    TO_CHAR(rec.line_number),
                    '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', -- attr4-14
                    l_batch_src,
                    'LINE',
                    rec.ordered_item || ' - Order ' || l_order_number || ' Line ' || rec.line_number,
                    rec.transactional_curr_code,
                    l_amount,
                    l_cust_trx_type,
                    rec.sold_to_org_id,
                    rec.invoice_to_org_id,
                    rec.sold_to_org_id,
                    rec.ship_to_org_id,
                    'User',                     -- [CHG-9] User type with explicit rate
                    1,                          -- [CHG-9] rate=1 (INR->INR, no conversion)
                    rec.actual_shipment_date,   -- [CHG-9] conversion date from legacy shipment
                    rec.actual_shipment_date,
                    rec.actual_shipment_date,
                    rec.ordered_quantity,
                    rec.ordered_quantity,
                    rec.unit_selling_price,
                    rec.unit_list_price,
                    rec.order_quantity_uom,
                    NULL,                       -- [CHG-12] NULL to skip item validation org check
                    l_order_number,                             -- [CHG-28] sales_order
                    TO_CHAR(rec.line_number),
                    rec.ordered_date,
                    rec.actual_shipment_date,
                    NULL,                       -- [CHG-19] warehouse_id=NULL (legacy org not in new instance)
                    g_org_id,                   -- [CHG-14] constant org_id
                    5,                          -- [CHG-10] term_id = IMMEDIATE
                    'IMMEDIATE',                -- [CHG-10] term_name
                    -3,                         -- [CHG-11] salesrep_id = No Sales Credit
                    '-3',                       -- [CHG-11] salesrep_number
                    'E',                        -- [CHG-27] tax_exempt_flag = Exempt
                    'RESALE',                   -- [CHG-27] tax_exempt_reason_code
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
                    l_order_number,                             -- [CHG-28]
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
                INSERT INTO apps.ra_interface_salescredits (
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
                    l_order_number,                             -- [CHG-28]
                    TO_CHAR(rec.line_id),
                    TO_CHAR(rec.line_number),
                    '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
                    '-3',
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
                -- [CHG-22] TAX LOOP: insert each JAI tax component as a LINE
                -- [CHG-23] attribute4 = tax_rate_code keeps flexfield unique
                -- [CHG-24] REV distribution uses same COA as parent item line
                -- [CHG-25] NO sales credit for tax rows
                -- ============================================================
                FOR tax_rec IN c_tax_lines(rec.line_id) LOOP
                    DECLARE
                        l_tax_line_id NUMBER;
                    BEGIN
                        SELECT ra_customer_trx_lines_s.NEXTVAL INTO l_tax_line_id FROM dual;

                        INSERT INTO ar.ra_interface_lines_all (
                            interface_line_id,
                            interface_line_context,
                            interface_line_attribute1,
                            interface_line_attribute2,
                            interface_line_attribute3,  -- [CHG-23] line_num-tax_rate_code
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
                            orig_system_bill_customer_id,
                            orig_system_bill_address_id,
                            orig_system_ship_customer_id,
                            orig_system_ship_address_id,
                            conversion_type,
                            conversion_rate,
                            conversion_date,
                            trx_date,
                            gl_date,
                            sales_order,
                            sales_order_line,
                            sales_order_date,
                            ship_date_actual,
                            warehouse_id,
                            org_id,
                            term_id,
                            term_name,
                            primary_salesrep_id,
                            primary_salesrep_number,
                            tax_exempt_flag,            -- [CHG-27]
                            tax_exempt_reason_code,     -- [CHG-27]
                            created_by,
                            creation_date,
                            last_updated_by,
                            last_update_date,
                            last_update_login
                        ) VALUES (
                            l_tax_line_id,
                            'BAMUL_OM_IMPORT',
                            l_order_number,                             -- [CHG-28]
                            TO_CHAR(rec.line_id),
                            TO_CHAR(rec.line_number) || '-' || tax_rec.tax_rate_code, -- [CHG-23]
                            '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
                            l_batch_src,
                            'LINE',                         -- [CHG-22] LINE not TAX
                            tax_rec.tax_rate_code || ' ' || tax_rec.tax_rate_percentage
                                || '% - Order ' || l_order_number || ' Line ' || rec.line_number,
                            rec.transactional_curr_code,
                            tax_rec.rounded_tax_amt_trx_curr,
                            l_cust_trx_type,
                            rec.sold_to_org_id,
                            rec.invoice_to_org_id,
                            rec.sold_to_org_id,
                            rec.ship_to_org_id,
                            'User',
                            1,
                            rec.actual_shipment_date,
                            rec.actual_shipment_date,
                            rec.actual_shipment_date,
                            l_order_number,                             -- [CHG-28]
                            TO_CHAR(rec.line_number),
                            rec.ordered_date,
                            rec.actual_shipment_date,
                            NULL,
                            g_org_id,
                            5,
                            'IMMEDIATE',
                            -3,
                            '-3',
                            'E',                            -- [CHG-27] tax_exempt_flag
                            'RESALE',                       -- [CHG-27] tax_exempt_reason_code
                            g_user_id, SYSDATE,
                            g_user_id, SYSDATE, g_login_id
                        );

                        -- [CHG-24] REV distribution — same COA as parent item line
                        INSERT INTO ar.ra_interface_distributions_all (
                            interface_line_id,
                            interface_line_context,
                            interface_line_attribute1,
                            interface_line_attribute2,
                            interface_line_attribute3,  -- [CHG-23] must match line above
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
                            created_by, creation_date,
                            last_updated_by, last_update_date, last_update_login
                        ) VALUES (
                            l_tax_line_id,
                            'BAMUL_OM_IMPORT',
                            l_order_number,                             -- [CHG-28]
                            TO_CHAR(rec.line_id),
                            TO_CHAR(rec.line_number) || '-' || tax_rec.tax_rate_code, -- [CHG-23]
                            '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
                            'REV',
                            tax_rec.rounded_tax_amt_trx_curr,
                            100,
                            l_new_entity,
                            l_new_division,
                            l_new_account,
                            l_new_department,
                            l_new_product,
                            l_new_txn,
                            l_new_future1,
                            l_new_future2,
                            g_org_id,
                            g_user_id, SYSDATE,
                            g_user_id, SYSDATE, g_login_id
                        );

                        -- [CHG-25] Sales credit for tax row — required because Require Salesreps=Y
                        -- applies to ALL LINE rows, including tax components
                        INSERT INTO apps.ra_interface_salescredits (
                            interface_line_context,
                            interface_line_attribute1,
                            interface_line_attribute2,
                            interface_line_attribute3,  -- must match tax line's attribute3
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
                            l_order_number,                             -- [CHG-28]
                            TO_CHAR(rec.line_id),
                            TO_CHAR(rec.line_number) || '-' || tax_rec.tax_rate_code,
                            '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
                            '-3',
                            'Quota Sales Credit',
                            1,
                            100,
                            g_org_id,
                            g_user_id, SYSDATE,
                            g_user_id, SYSDATE, g_login_id
                        );

                        l_tax_lines_inserted := l_tax_lines_inserted + 1;

                    EXCEPTION
                        WHEN OTHERS THEN
                            DBMS_OUTPUT.PUT_LINE('ERROR tax line for line_id=' || rec.line_id
                                || ', tax=' || tax_rec.tax_rate_code || ': ' || SQLERRM);
                    END;
                END LOOP;

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

        IF p_submit_autoinv = 'Y' AND l_lines_inserted > 0 THEN
            submit_autoinvoice(
                p_org_id            => g_org_id,                -- [CHG-18]
                p_batch_source_name => 'BAMUL_OM_IMPORT'
            );
        END IF;

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
                DBMS_OUTPUT.PUT_LINE('FATAL ERROR: ' || l_err_msg);
                RAISE;
            END;
    END run_interface;

    -- =========================================================================
    -- Submit AutoInvoice concurrent program
    -- =========================================================================
    PROCEDURE submit_autoinvoice (
        p_org_id            IN NUMBER,
        p_batch_source_name IN VARCHAR2
    ) IS
        l_request_id NUMBER;
    BEGIN
        -- [CHG-17] Removed fnd_global.apps_initialize - not needed when called
        --          from within a running concurrent request context
        mo_global.set_policy_context('S', p_org_id);

        l_request_id := fnd_request.submit_request(
            application => 'AR',
            program     => 'RAXMTR',
            description => 'BAMUL OM to AR - AutoInvoice',
            start_time  => NULL,
            sub_request => FALSE,
            argument1   => '1',
            argument2   => p_org_id,
            argument3   => p_batch_source_name,
            argument4   => p_batch_source_name,
            argument5   => SYSDATE,
            argument6   => NULL, argument7  => NULL, argument8  => NULL,
            argument9   => NULL, argument10 => NULL, argument11 => NULL,
            argument12  => NULL, argument13 => NULL, argument14 => NULL,
            argument15  => NULL, argument16 => NULL, argument17 => NULL,
            argument18  => NULL, argument19 => NULL, argument20 => NULL,
            argument21  => NULL, argument22 => NULL, argument23 => NULL,
            argument24  => NULL, argument25 => 'Y'
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
/
