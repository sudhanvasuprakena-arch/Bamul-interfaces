/* ============================================================================
   FILE:         04_OM_to_GL_Package_Body.sql
   PACKAGE:      XXCUST_OM_GL_INTERFACE_PKG (Body)
   VERSION:      1.0 PRODUCTION
   
   DESCRIPTION:
   ------------
   Package body for OM to GL COGS journal interface.
   
   PROCESSING FLOW:
   1. Query AR interface lines from RA_INTERFACE_LINES_ALL (newly created invoices)
   2. For each line:
      a. Lookup product in XX_COGS_DETAILS
      b. Calculate total_cost = quantity × unit_cost
      c. Derive GL period and CCIDs
      d. Validate (period open, product exists, no duplicates)
      e. Insert DR + CR lines to GL_INTERFACE
      f. Log to XX_OM_GL_INTERFACE_LOG
   3. Commit
   
   ============================================================================ */

CREATE OR REPLACE PACKAGE BODY apps.xxcust_om_gl_interface_pkg AS

    -- =========================================================================
    -- CONSTANTS
    -- =========================================================================
    c_gl_source            CONSTANT VARCHAR2(50) := 'XXCUST_OM_COGS';
    c_je_category          CONSTANT VARCHAR2(50) := 'Cost of Goods Sold';
    c_run_prefix           CONSTANT VARCHAR2(10) := 'OMGL-';
    c_actual_flag          CONSTANT VARCHAR2(1) := 'A';
    c_created_by           CONSTANT NUMBER := -1;
    c_chart_of_accounts_id CONSTANT NUMBER := 50428;
    c_currency_code        CONSTANT VARCHAR2(15) := 'INR';
    
    c_status_processed     CONSTANT VARCHAR2(20) := 'PROCESSED';
    c_status_rejected      CONSTANT VARCHAR2(20) := 'REJECTED';
    c_status_error         CONSTANT VARCHAR2(20) := 'ERROR';

    -- =========================================================================
    -- PRIVATE: Write to output log
    -- =========================================================================
    PROCEDURE log_msg (p_msg IN VARCHAR2) IS
    BEGIN
        dbms_output.put_line(to_char(sysdate, 'DD-MON-YYYY HH24:MI:SS') || ' | ' || p_msg);
    END log_msg;

    -- =========================================================================
    -- Get ledger ID (set_of_books_id)
    -- =========================================================================
    FUNCTION get_sob_id RETURN NUMBER IS
        l_sob_id NUMBER;
    BEGIN
        SELECT MIN(set_of_books_id)
        INTO   l_sob_id
        FROM   apps.ar_system_parameters_all
        WHERE  set_of_books_id > 0;
        RETURN l_sob_id;
    EXCEPTION
        WHEN OTHERS THEN RETURN 2021; -- Default for BAMUL
    END get_sob_id;

    -- =========================================================================
    -- Get GL period name for a given date
    -- =========================================================================
    FUNCTION get_period_name (p_accounting_date IN DATE) RETURN VARCHAR2 IS
        l_period_name VARCHAR2(15);
    BEGIN
        SELECT gps.period_name
        INTO   l_period_name
        FROM   apps.gl_period_statuses gps
        JOIN   apps.ar_system_parameters_all asp ON asp.set_of_books_id = gps.set_of_books_id
        WHERE  gps.start_date <= p_accounting_date
        AND    gps.end_date >= p_accounting_date
        AND    (gps.closing_status = 'O' OR gps.closing_status = 'F')
        AND    gps.adjustment_period_flag = 'N'
        AND    asp.set_of_books_id > 0
        AND    ROWNUM = 1;
        RETURN l_period_name;
    EXCEPTION
        WHEN OTHERS THEN RETURN NULL;
    END get_period_name;

    -- =========================================================================
    -- Get division segment from organization mapping
    -- =========================================================================
    FUNCTION get_division_segment (p_org_id IN NUMBER) RETURN VARCHAR2 IS
        l_division VARCHAR2(25);
    BEGIN
        SELECT division_code
        INTO   l_division
        FROM   apps.mapping_division_segment
        WHERE  organization_id = p_org_id;
        RETURN l_division;
    EXCEPTION
        WHEN OTHERS THEN RETURN '00';  -- Default
    END get_division_segment;

    -- =========================================================================
    -- Get department segment from cost center mapping
    -- =========================================================================
    FUNCTION get_department_segment (p_cost_center IN VARCHAR2) RETURN VARCHAR2 IS
        l_department VARCHAR2(25);
    BEGIN
        SELECT new_department_flex_value
        INTO   l_department
        FROM   apps.mapping_department_segment
        WHERE  old_department_flex_value = p_cost_center;
        RETURN l_department;
    EXCEPTION
        WHEN OTHERS THEN RETURN '00000';  -- Default
    END get_department_segment;

    -- =========================================================================
    -- Get product segment from item mapping
    -- =========================================================================
    FUNCTION get_product_segment (p_item_id IN NUMBER) RETURN VARCHAR2 IS
        l_product VARCHAR2(25);
    BEGIN
        SELECT product_code
        INTO   l_product
        FROM   apps.mapping_product_segment
        WHERE  inventory_item_id = p_item_id;
        RETURN l_product;
    EXCEPTION
        WHEN OTHERS THEN RETURN '00000000';  -- Default
    END get_product_segment;

    -- =========================================================================
    -- Get transaction type segment (GST-based)
    -- =========================================================================
    FUNCTION get_txn_type_segment (p_customer_trx_id IN NUMBER) RETURN VARCHAR2 IS
        l_tax_code VARCHAR2(100);
    BEGIN
        -- Try to get tax code from AR transaction
        -- This is a placeholder - adjust based on your tax setup
        RETURN '0';  -- Default for now
    EXCEPTION
        WHEN OTHERS THEN RETURN '0';
    END get_txn_type_segment;

    -- =========================================================================
    -- Derive CCID from 8-segment combination
    -- =========================================================================
    FUNCTION derive_ccid (
        p_segment3 IN VARCHAR2,  -- Account segment (from cogs_account or material_account)
        p_org_id   IN NUMBER DEFAULT NULL,
        p_item_id  IN NUMBER DEFAULT NULL,
        p_trx_id   IN NUMBER DEFAULT NULL
    ) RETURN NUMBER IS
        l_ccid           NUMBER;
        l_seg1_entity    VARCHAR2(25) := '01';
        l_seg2_division  VARCHAR2(25);
        l_seg3_account   VARCHAR2(25) := p_segment3;
        l_seg4_dept      VARCHAR2(25);
        l_seg5_product   VARCHAR2(25);
        l_seg6_txn       VARCHAR2(25);
        l_seg7_future1   VARCHAR2(25) := '000';
        l_seg8_future2   VARCHAR2(25) := '000';
    BEGIN
        -- Derive segments
        l_seg2_division := NVL(get_division_segment(p_org_id), '00');
        l_seg4_dept     := '00000';  -- Default for COGS
        l_seg5_product  := NVL(get_product_segment(p_item_id), '00000000');
        l_seg6_txn      := NVL(get_txn_type_segment(p_trx_id), '0');

        -- Lookup CCID
        SELECT code_combination_id
        INTO   l_ccid
        FROM   apps.gl_code_combinations
        WHERE  chart_of_accounts_id = c_chart_of_accounts_id
        AND    segment1 = l_seg1_entity
        AND    segment2 = l_seg2_division
        AND    segment3 = l_seg3_account
        AND    segment4 = l_seg4_dept
        AND    segment5 = l_seg5_product
        AND    segment6 = l_seg6_txn
        AND    segment7 = l_seg7_future1
        AND    segment8 = l_seg8_future2
        AND    enabled_flag = 'Y'
        AND    ROWNUM = 1;

        RETURN l_ccid;
    EXCEPTION
        WHEN no_data_found THEN
            log_msg('  CCID not found for: ' || l_seg1_entity || '.' || l_seg2_division || '.' ||
                    l_seg3_account || '.' || l_seg4_dept || '.' || l_seg5_product || '.' ||
                    l_seg6_txn || '.' || l_seg7_future1 || '.' || l_seg8_future2);
            RETURN -1;
        WHEN OTHERS THEN
            log_msg('  Error deriving CCID: ' || sqlerrm);
            RETURN -1;
    END derive_ccid;

    -- =========================================================================
    -- MAIN INTERFACE PROCEDURE
    -- =========================================================================
    PROCEDURE run_interface (
        p_errbuf              OUT VARCHAR2,
        p_retcode             OUT NUMBER,
        p_group_id            OUT NUMBER,
        p_ar_batch_source     IN  VARCHAR2 DEFAULT 'BAMUL_OM_IMPORT',
        p_invoice_date_from   IN  VARCHAR2 DEFAULT NULL,
        p_invoice_date_to     IN  VARCHAR2 DEFAULT NULL,
        p_debug_mode          IN  VARCHAR2 DEFAULT 'N'
    ) IS
        -- =====================================================================
        -- CURSOR: AR Invoice Lines from RA_INTERFACE_LINES_ALL
        -- These are the lines just created by OM to AR interface
        -- =====================================================================
        CURSOR c_ar_lines IS
            SELECT
                ril.interface_line_id,
                ril.interface_line_attribute1 AS order_number,
                ril.interface_line_attribute6 AS order_line_id,
                ril.interface_line_attribute15 AS line_number,
                ril.batch_source_name,
                ril.trx_date,
                ril.gl_date,
                ril.quantity,
                ril.unit_selling_price,
                ril.amount,
                ril.currency_code,
                ril.inventory_item_id,
                ril.description,
                ril.org_id,
                -- Get product code from segment5 in distributions table
                rid.segment5 AS product_code
            FROM
                apps.ra_interface_lines_all ril,
                apps.ra_interface_distributions_all rid
            WHERE
                ril.interface_line_id = rid.interface_line_id
                AND ril.batch_source_name = p_ar_batch_source
                AND ril.line_type = 'LINE'
                AND NVL(ril.quantity, 0) > 0
                AND NVL(ril.amount, 0) <> 0
                -- Exclude tax lines
                AND rid.segment5 NOT LIKE '%GST%'
                AND rid.segment5 NOT LIKE '%TAX%'
                AND rid.segment5 IS NOT NULL
                AND rid.segment5 <> '00000000'  -- Exclude invalid products
                -- Date filters
                AND (p_invoice_date_from IS NULL OR TRUNC(ril.trx_date) >= TO_DATE(p_invoice_date_from, 'DD-MON-YYYY'))
                AND (p_invoice_date_to IS NULL OR TRUNC(ril.trx_date) <= TO_DATE(p_invoice_date_to, 'DD-MON-YYYY'))
                -- Not already processed
                AND NOT EXISTS (
                    SELECT 1
                    FROM apps.xx_om_gl_interface_log log
                    WHERE log.ar_invoice_line_id = ril.interface_line_id
                    AND log.interface_status = c_status_processed
                )
            ORDER BY
                ril.trx_date,
                ril.interface_line_attribute1,
                ril.interface_line_attribute15;

        -- Local variables
        l_run_id            VARCHAR2(50);
        l_sob_id            NUMBER;
        l_period_name       VARCHAR2(15);
        l_accounting_date   DATE;
        l_debit_ccid        NUMBER;
        l_credit_ccid       NUMBER;
        l_total_cost        NUMBER;
        l_cnt_processed     NUMBER := 0;
        l_cnt_rejected      NUMBER := 0;
        l_cnt_errored       NUMBER := 0;

        -- COGS details variables
        l_cogs_product_code        VARCHAR2(50);
        l_cogs_description         VARCHAR2(240);
        l_cogs_cost                NUMBER;
        l_cogs_uom                 VARCHAR2(3);
        l_cogs_material_account    VARCHAR2(20);
        l_cogs_cogs_account        VARCHAR2(20);
        l_reference_dr             VARCHAR2(240);
        l_reference_cr             VARCHAR2(240);
        l_order_number             VARCHAR2(30);
        l_sysdate                  DATE;
        l_interface_line_id        NUMBER;
        l_product_code             VARCHAR2(50);
        l_inventory_item_id        NUMBER;
        l_quantity                 NUMBER;
        l_trx_date                 DATE;
        l_line_number              VARCHAR2(30);
        l_group_id                 NUMBER;
        l_currency_code            VARCHAR2(15);
        l_status_processed         VARCHAR2(20);
        l_mat_acct                 VARCHAR2(20);
        l_cogs_acct                VARCHAR2(20);
        l_dr_ccid                  NUMBER;
        l_cr_ccid                  NUMBER;
        l_acct_date                DATE;
        l_prd_name                 VARCHAR2(15);
        l_prod_code                VARCHAR2(50);
        l_item_id                  NUMBER;
        l_qty                      NUMBER;
        l_unit_cost                NUMBER;
        l_tot_cost                 NUMBER;

    BEGIN
        p_retcode := 0;
        l_sysdate := SYSDATE;
        l_currency_code := c_currency_code;
        l_status_processed := c_status_processed;

        -- Initialize
        l_run_id := c_run_prefix || TO_CHAR(l_sysdate, 'YYYYMMDDHH24MISS');
        l_sob_id := get_sob_id();

        -- Get group_id for GL_INTERFACE
        SELECT apps.gl_journal_import_s.NEXTVAL INTO p_group_id FROM dual;
        l_group_id := p_group_id;

        log_msg('=================================================================');
        log_msg('IF-02: OM to GL COGS Interface - Run ID: ' || l_run_id);
        log_msg('Group ID (Journal Import): ' || p_group_id);
        log_msg('Ledger (SOB ID)          : ' || l_sob_id);
        log_msg('AR Batch Source          : ' || p_ar_batch_source);
        log_msg('Invoice Date From        : ' || NVL(p_invoice_date_from, 'NONE'));
        log_msg('Invoice Date To          : ' || NVL(p_invoice_date_to, 'NONE'));
        log_msg('=================================================================');

        -- =====================================================================
        -- MAIN PROCESSING LOOP
        -- =====================================================================
        FOR r IN c_ar_lines LOOP
            BEGIN
                -- Copy cursor values to local variables at start
                l_order_number := r.order_number;
                l_interface_line_id := r.interface_line_id;
                l_product_code := r.product_code;
                l_inventory_item_id := r.inventory_item_id;
                l_quantity := r.quantity;
                l_trx_date := r.trx_date;
                l_line_number := r.line_number;

                -- STEP 1: Lookup COGS details
                BEGIN
                    SELECT product_code, description, cost, unit_of_measurement,
                           material_account, cogs_account
                    INTO   l_cogs_product_code, l_cogs_description, l_cogs_cost,
                           l_cogs_uom, l_cogs_material_account, l_cogs_cogs_account
                    FROM   apps.xx_cogs_details
                    WHERE  product_code = l_product_code
                    AND    enabled_flag = 'Y';
                EXCEPTION
                    WHEN no_data_found THEN
                        INSERT INTO apps.xx_om_gl_interface_log (
                            run_id, ar_invoice_number, ar_invoice_line_id,
                            product_code, quantity, accounting_date,
                            gl_interface_group_id, interface_status, rejection_reason
                        ) VALUES (
                            l_run_id, l_order_number, l_interface_line_id,
                            l_product_code, l_quantity, l_trx_date,
                            l_group_id, c_status_rejected,
                            'Product code ' || l_product_code || ' not found in XX_COGS_DETAILS'
                        );
                        l_cnt_rejected := l_cnt_rejected + 1;
                        log_msg('REJECTED: Order ' || l_order_number || ' Line ' || l_line_number ||
                                ' - Product ' || l_product_code || ' not found');
                        CONTINUE;
                END;

                -- STEP 2: Validate cost
                IF NVL(l_cogs_cost, 0) <= 0 THEN
                    INSERT INTO apps.xx_om_gl_interface_log (
                        run_id, ar_invoice_number, ar_invoice_line_id,
                        product_code, quantity, unit_cost, accounting_date,
                        gl_interface_group_id, interface_status, rejection_reason
                    ) VALUES (
                        l_run_id, l_order_number, l_interface_line_id,
                        l_product_code, l_quantity, l_cogs_cost, l_trx_date,
                        l_group_id, c_status_rejected,
                        'Product ' || l_product_code || ' has zero or negative cost'
                    );
                    l_cnt_rejected := l_cnt_rejected + 1;
                    log_msg('REJECTED: Order ' || l_order_number || ' Line ' || l_line_number ||
                            ' - Zero cost for product ' || l_product_code);
                    CONTINUE;
                END IF;

                -- STEP 3: Calculate total cost
                l_total_cost := ROUND(l_quantity * l_cogs_cost, 2);

                -- STEP 4: Derive accounting date and GL period
                l_accounting_date := NVL(r.gl_date, r.trx_date);
                l_period_name := get_period_name(l_accounting_date);

                IF l_period_name IS NULL THEN
                    -- Override to current month if period not open
                    l_accounting_date := TRUNC(sysdate, 'MM');
                    l_period_name := get_period_name(l_accounting_date);
                    
                    IF l_period_name IS NULL THEN
                        INSERT INTO apps.xx_om_gl_interface_log (
                            run_id, ar_invoice_number, ar_invoice_line_id,
                            product_code, quantity, unit_cost, total_cost,
                            accounting_date, gl_interface_group_id,
                            interface_status, rejection_reason
                        ) VALUES (
                            l_run_id, l_order_number, l_interface_line_id,
                            l_product_code, l_quantity, l_cogs_cost, l_total_cost,
                            l_trx_date, l_group_id, c_status_rejected,
                            'No open GL period found for date ' || TO_CHAR(l_trx_date, 'DD-MON-YYYY')
                        );
                        l_cnt_rejected := l_cnt_rejected + 1;
                        log_msg('REJECTED: Order ' || l_order_number || ' - No open period');
                        CONTINUE;
                    END IF;
                END IF;

                -- STEP 5: Derive CCIDs
                l_debit_ccid := derive_ccid(
                    p_segment3 => l_cogs_cogs_account,
                    p_org_id   => r.org_id,
                    p_item_id  => r.inventory_item_id,
                    p_trx_id   => NULL
                );

                l_credit_ccid := derive_ccid(
                    p_segment3 => l_cogs_material_account,
                    p_org_id   => r.org_id,
                    p_item_id  => r.inventory_item_id,
                    p_trx_id   => NULL
                );

                -- Copy to separate variables for INSERT (avoid column name conflicts)
                l_dr_ccid := l_debit_ccid;
                l_cr_ccid := l_credit_ccid;
                l_mat_acct := l_cogs_material_account;
                l_cogs_acct := l_cogs_cogs_account;
                l_acct_date := l_accounting_date;
                l_prd_name := l_period_name;
                l_prod_code := l_product_code;
                l_item_id := l_inventory_item_id;
                l_qty := l_quantity;
                l_unit_cost := l_cogs_cost;
                l_tot_cost := l_total_cost;

                IF l_debit_ccid = -1 OR l_credit_ccid = -1 THEN
                    EXECUTE IMMEDIATE
                        'INSERT INTO apps.xx_om_gl_interface_log (' ||
                        '    run_id, ar_invoice_number, ar_invoice_line_id,' ||
                        '    product_code, quantity, unit_cost, total_cost,' ||
                        '    material_account, cogs_account, debit_ccid, credit_ccid,' ||
                        '    accounting_date, period_name, gl_interface_group_id,' ||
                        '    interface_status, rejection_reason' ||
                        ') VALUES (' ||
                        '    :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15' ||
                        ')'
                    USING l_run_id, l_order_number, l_interface_line_id,
                          l_prod_code, l_qty, l_unit_cost, l_tot_cost,
                          l_mat_acct, l_cogs_acct, l_dr_ccid, l_cr_ccid,
                          l_acct_date, l_prd_name, l_group_id,
                          c_status_rejected,
                          'CCID not found. DR CCID=' || l_debit_ccid || ', CR CCID=' || l_credit_ccid;
                    l_cnt_rejected := l_cnt_rejected + 1;
                    log_msg('REJECTED: Order ' || l_order_number || ' - CCID not found');
                    CONTINUE;
                END IF;

                -- STEP 6: Build reference descriptions
                l_reference_dr := 'COGS Expense: ' || l_cogs_description || ' | Qty: ' || TO_CHAR(l_quantity);
                l_reference_cr := 'Inventory Reduction: ' || l_cogs_description || ' | Qty: ' || TO_CHAR(l_quantity);

                -- STEP 8: Insert DEBIT line to GL_INTERFACE (COGS Expense)
                INSERT INTO apps.gl_interface (
                    status, set_of_books_id, accounting_date, currency_code,
                    date_created, created_by, actual_flag,
                    user_je_source_name, user_je_category_name, period_name,
                    entered_dr, accounted_dr, code_combination_id,
                    -- Segment values for visibility
                    segment1, segment2, segment3, segment4, segment5, segment6, segment7, segment8,
                    -- Reference fields for traceability
                    reference1, reference2, reference4, reference5, reference6, reference10,
                    -- Attribute fields for additional data
                    attribute1, attribute2, attribute3, attribute4, attribute5,
                    group_id
                ) VALUES (
                    'NEW', l_sob_id, l_acct_date, c_currency_code,
                    l_sysdate, c_created_by, c_actual_flag,
                    c_gl_source, c_je_category, l_prd_name,
                    l_total_cost, l_total_cost, l_dr_ccid,
                    -- Segments
                    '01', '00', l_cogs_acct, '00000', l_prod_code, '0', '000', '000',
                    -- References
                    l_order_number,           -- Order number
                    'COGS-DR',                -- Line type
                    l_reference_dr,           -- Description
                    TO_CHAR(l_qty),           -- Quantity
                    TO_CHAR(l_unit_cost),     -- Unit cost
                    l_run_id,                 -- Run ID
                    -- Attributes
                    l_prod_code,              -- Product code
                    l_cogs_acct,              -- COGS account
                    l_mat_acct,               -- Material account
                    TO_CHAR(l_interface_line_id), -- Interface line ID
                    'DEBIT',                  -- Entry type
                    l_group_id
                );

                -- STEP 9: Insert CREDIT line to GL_INTERFACE (Inventory Reduction)
                INSERT INTO apps.gl_interface (
                    status, set_of_books_id, accounting_date, currency_code,
                    date_created, created_by, actual_flag,
                    user_je_source_name, user_je_category_name, period_name,
                    entered_cr, accounted_cr, code_combination_id,
                    -- Segment values for visibility
                    segment1, segment2, segment3, segment4, segment5, segment6, segment7, segment8,
                    -- Reference fields for traceability
                    reference1, reference2, reference4, reference5, reference6, reference10,
                    -- Attribute fields for additional data
                    attribute1, attribute2, attribute3, attribute4, attribute5,
                    group_id
                ) VALUES (
                    'NEW', l_sob_id, l_acct_date, c_currency_code,
                    l_sysdate, c_created_by, c_actual_flag,
                    c_gl_source, c_je_category, l_prd_name,
                    l_total_cost, l_total_cost, l_cr_ccid,
                    -- Segments
                    '01', '00', l_mat_acct, '00000', l_prod_code, '0', '000', '000',
                    -- References
                    l_order_number,           -- Order number
                    'INV-CR',                 -- Line type
                    l_reference_cr,           -- Description
                    TO_CHAR(l_qty),           -- Quantity
                    TO_CHAR(l_unit_cost),     -- Unit cost
                    l_run_id,                 -- Run ID
                    -- Attributes
                    l_prod_code,              -- Product code
                    l_cogs_acct,              -- COGS account
                    l_mat_acct,               -- Material account
                    TO_CHAR(l_interface_line_id), -- Interface line ID
                    'CREDIT',                 -- Entry type
                    l_group_id
                );

                -- STEP 10: Log success
                EXECUTE IMMEDIATE
                    'INSERT INTO apps.xx_om_gl_interface_log (' ||
                    '    run_id, ar_invoice_number, ar_invoice_line_id,' ||
                    '    product_code, inventory_item_id, quantity, unit_cost, total_cost,' ||
                    '    material_account, cogs_account, debit_ccid, credit_ccid,' ||
                    '    accounting_date, period_name, currency_code,' ||
                    '    gl_interface_group_id, interface_status' ||
                    ') VALUES (' ||
                    '    :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17' ||
                    ')'
                USING l_run_id, l_order_number, l_interface_line_id,
                      l_prod_code, l_item_id, l_qty, l_unit_cost, l_tot_cost,
                      l_mat_acct, l_cogs_acct, l_dr_ccid, l_cr_ccid,
                      l_acct_date, l_prd_name, l_currency_code,
                      l_group_id, l_status_processed;

                l_cnt_processed := l_cnt_processed + 1;

                IF p_debug_mode = 'Y' THEN
                    log_msg('PROCESSED: Order ' || l_order_number || ' Line ' || l_line_number ||
                            ' | Product: ' || l_prod_code || ' | Cost: ' || l_tot_cost);
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    EXECUTE IMMEDIATE
                        'INSERT INTO apps.xx_om_gl_interface_log (' ||
                        '    run_id, ar_invoice_number, ar_invoice_line_id,' ||
                        '    product_code, accounting_date, gl_interface_group_id,' ||
                        '    interface_status, rejection_reason' ||
                        ') VALUES (' ||
                        '    :1, :2, :3, :4, :5, :6, :7, :8' ||
                        ')'
                    USING l_run_id, l_order_number, l_interface_line_id,
                          l_prod_code, l_trx_date, l_group_id,
                          c_status_error, 'Unexpected error: ' || sqlerrm;
                    l_cnt_errored := l_cnt_errored + 1;
                    log_msg('ERROR: Order ' || l_order_number || ' - ' || sqlerrm);
            END;
        END LOOP;

        COMMIT;

        log_msg('=================================================================');
        log_msg('IF-02 Complete - Run ID: ' || l_run_id);
        log_msg('  Lines Processed: ' || l_cnt_processed);
        log_msg('  Lines Rejected : ' || l_cnt_rejected);
        log_msg('  Errors         : ' || l_cnt_errored);
        log_msg('-----------------------------------------------------------------');
        log_msg('Next Step: Run Journal Import in GL');
        log_msg('  Source: ' || c_gl_source);
        log_msg('  Group ID: ' || p_group_id);
        log_msg('=================================================================');

        IF l_cnt_rejected > 0 OR l_cnt_errored > 0 THEN
            p_retcode := 1;
            p_errbuf := 'IF-02 completed with ' || l_cnt_rejected ||
                        ' rejections and ' || l_cnt_errored || ' errors';
        ELSE
            p_errbuf := 'IF-02 completed successfully. Group ID: ' || p_group_id;
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            p_retcode := 2;
            p_errbuf := 'IF-02 FATAL ERROR: ' || sqlerrm;
            log_msg('FATAL ERROR: ' || sqlerrm);
    END run_interface;

    -- =========================================================================
    -- PURGE LOG
    -- =========================================================================
    PROCEDURE purge_log (p_days_to_keep IN NUMBER DEFAULT 90) IS
        l_count NUMBER;
    BEGIN
        DELETE FROM apps.xx_om_gl_interface_log
        WHERE  creation_date < sysdate - p_days_to_keep
        AND    interface_status = c_status_processed;

        l_count := SQL%ROWCOUNT;
        COMMIT;
        log_msg('Purge complete: ' || l_count || ' log records deleted.');
    END purge_log;

END xxcust_om_gl_interface_pkg;
/

