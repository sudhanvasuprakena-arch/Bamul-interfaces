/* ============================================================================
   SCRIPT:      migrate_supplier.sql
   PURPOSE:     Complete Supplier Data Migration from Legacy Oracle EBS R12.2
                to New Oracle EBS R12.2 instance via DB link.

   MIGRATES:
     Step 0  - Diagnostics & Pre-checks
     Step 1  - AP_SUPPLIERS              (Vendor Master)
     Step 2  - AP_SUPPLIER_SITES_ALL     (Vendor Sites)
     Step 3  - AP_SUPPLIER_CONTACTS      (Vendor Contacts)
     Step 4  - IBY_EXT_BANK_ACCOUNTS     (Supplier Bank Accounts)
     Step 5  - IBY_PMT_INSTR_USES_ALL   (Payment Instrument Assignments)
     Step 6  - IBY_EXTERNAL_PAYEES_ALL   (External Payee Records)
     Step 7  - Post-Migration Validation

   PRE-REQ:
     - DB link LEGACY_INSTANCE must exist on the new instance
     - DB link user must be APPS or have SELECT grants on required tables
     - Run as APPS or user with INSERT privileges on target tables

   PLATFORM:    Oracle EBS R12.2 (Legacy) --> Oracle EBS R12.2 (New Instance)
   ============================================================================ */

SET SERVEROUTPUT ON SIZE UNLIMITED;
SET TIMING ON;
SET FEEDBACK ON;

-- Define DB link name
DEFINE legacy_db_link = 'LEGACY_INSTANCE'

/* ============================================================================
   STEP 0: DIAGNOSTICS
   ============================================================================ */

PROMPT =====================================================================
PROMPT  STEP 0: PRE-MIGRATION DIAGNOSTICS
PROMPT =====================================================================

-- 0A: Verify DB link connectivity
PROMPT === 0A: DB Link details ===
SELECT db_link, username, host
FROM   user_db_links
WHERE  db_link LIKE '%LEGACY%';

-- 0B: Legacy record counts
PROMPT === 0B: Legacy instance record counts ===
SELECT 'AP_SUPPLIERS'            AS table_name, COUNT(*) AS row_count FROM apps.ap_suppliers@LEGACY_INSTANCE
UNION ALL
SELECT 'AP_SUPPLIER_SITES_ALL',                 COUNT(*)             FROM apps.ap_supplier_sites_all@LEGACY_INSTANCE
UNION ALL
SELECT 'AP_SUPPLIER_CONTACTS',                  COUNT(*)             FROM apps.ap_supplier_contacts@LEGACY_INSTANCE
UNION ALL
SELECT 'IBY_EXT_BANK_ACCOUNTS',                 COUNT(*)             FROM apps.iby_ext_bank_accounts@LEGACY_INSTANCE
UNION ALL
SELECT 'IBY_PMT_INSTR_USES_ALL',                COUNT(*)             FROM apps.iby_pmt_instr_uses_all@LEGACY_INSTANCE
UNION ALL
SELECT 'IBY_EXTERNAL_PAYEES_ALL',                COUNT(*)            FROM apps.iby_external_payees_all@LEGACY_INSTANCE;

-- 0C: New instance current counts
PROMPT === 0C: New instance current counts ===
SELECT 'AP_SUPPLIERS'            AS table_name, COUNT(*) AS row_count FROM ap_suppliers
UNION ALL
SELECT 'AP_SUPPLIER_SITES_ALL',                 COUNT(*)             FROM ap.ap_supplier_sites_all
UNION ALL
SELECT 'AP_SUPPLIER_CONTACTS',                  COUNT(*)             FROM ap_supplier_contacts
UNION ALL
SELECT 'IBY_EXT_BANK_ACCOUNTS',                 COUNT(*)             FROM iby_ext_bank_accounts
UNION ALL
SELECT 'IBY_PMT_INSTR_USES_ALL',                COUNT(*)             FROM iby_pmt_instr_uses_all
UNION ALL
SELECT 'IBY_EXTERNAL_PAYEES_ALL',                COUNT(*)            FROM iby_external_payees_all;

-- 0D: Count of suppliers in-scope (those with PO receipts)
PROMPT === 0D: In-scope suppliers (with PO receipts) ===
SELECT COUNT(DISTINCT ph.vendor_id) AS vendors_with_receipts,
       COUNT(DISTINCT ph.vendor_site_id) AS sites_with_receipts
FROM   apps.po_headers_all@LEGACY_INSTANCE       ph
JOIN   apps.rcv_transactions@LEGACY_INSTANCE      rt ON rt.po_header_id = ph.po_header_id
WHERE  rt.transaction_type IN ('RECEIVE', 'RETURN TO VENDOR')
AND    rt.source_document_code = 'PO';


/* ============================================================================
   STEP 1: MIGRATE AP_SUPPLIERS (Vendor Master)
   ============================================================================ */

PROMPT =====================================================================
PROMPT  STEP 1: MIGRATING AP_SUPPLIERS
PROMPT =====================================================================

INSERT INTO ap_suppliers (
    vendor_id,
    vendor_name,
    vendor_name_alt,
    segment1,
    summary_flag,
    vendor_type_lookup_code,
    enabled_flag,
    start_date_active,
    end_date_active,
    -- Tax / regulatory
    num_1099,
    type_1099,
    vat_registration_num,
    tax_verification_date,
    tax_reporting_name,
    standard_industry_class,
    -- Payment defaults
    payment_method_lookup_code,
    payment_priority,
    terms_id,
    terms_date_basis,
    always_take_disc_flag,
    pay_date_basis_lookup_code,
    pay_group_lookup_code,
    invoice_currency_code,
    payment_currency_code,
    -- Holds
    hold_all_payments_flag,
    hold_unmatched_invoices_flag,
    hold_future_payments_flag,
    -- Matching / invoicing
    match_option,
    auto_tax_calc_flag,
    auto_tax_calc_override,
    invoice_amount_limit,
    -- Withholding
    withholding_status_lookup_code,
    withholding_start_date,
    -- Diversity / classification
    minority_group_lookup_code,
    women_owned_flag,
    small_business_flag,
    one_time_flag,
    -- Relationships
    customer_num,
    parent_vendor_id,
    -- DFF
    attribute_category,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    -- Audit
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login
)
SELECT
    aps.vendor_id,
    aps.vendor_name,
    aps.vendor_name_alt,
    aps.segment1,
    NVL(aps.summary_flag, 'N'),
    aps.vendor_type_lookup_code,
    aps.enabled_flag,
    aps.start_date_active,
    aps.end_date_active,
    -- Tax / regulatory
    aps.num_1099,
    aps.type_1099,
    aps.vat_registration_num,
    aps.tax_verification_date,
    aps.tax_reporting_name,
    aps.standard_industry_class,
    -- Payment defaults
    aps.payment_method_lookup_code,
    aps.payment_priority,
    aps.terms_id,
    aps.terms_date_basis,
    aps.always_take_disc_flag,
    aps.pay_date_basis_lookup_code,
    aps.pay_group_lookup_code,
    aps.invoice_currency_code,
    aps.payment_currency_code,
    -- Holds
    aps.hold_all_payments_flag,
    aps.hold_unmatched_invoices_flag,
    aps.hold_future_payments_flag,
    -- Matching / invoicing
    aps.match_option,
    aps.auto_tax_calc_flag,
    aps.auto_tax_calc_override,
    aps.invoice_amount_limit,
    -- Withholding
    aps.withholding_status_lookup_code,
    aps.withholding_start_date,
    -- Diversity / classification
    aps.minority_group_lookup_code,
    aps.women_owned_flag,
    aps.small_business_flag,
    aps.one_time_flag,
    -- Relationships
    aps.customer_num,
    aps.parent_vendor_id,
    -- DFF
    aps.attribute_category,
    aps.attribute1,
    aps.attribute2,
    aps.attribute3,
    aps.attribute4,
    aps.attribute5,
    aps.attribute6,
    aps.attribute7,
    aps.attribute8,
    aps.attribute9,
    aps.attribute10,
    aps.attribute11,
    aps.attribute12,
    aps.attribute13,
    aps.attribute14,
    aps.attribute15,
    -- Audit
    -1,
    SYSDATE,
    -1,
    SYSDATE,
    -1
FROM
    apps.ap_suppliers@LEGACY_INSTANCE aps
WHERE
    -- Scope: suppliers referenced on POs with receipts
    aps.vendor_id IN (
        SELECT DISTINCT ph.vendor_id
        FROM   apps.po_headers_all@LEGACY_INSTANCE    ph
        JOIN   apps.rcv_transactions@LEGACY_INSTANCE   rt ON rt.po_header_id = ph.po_header_id
        WHERE  rt.transaction_type IN ('RECEIVE', 'RETURN TO VENDOR')
        AND    rt.source_document_code = 'PO'
    )
    -- Skip already migrated
    AND NOT EXISTS (
        SELECT 1
        FROM   ap_suppliers new_aps
        WHERE  new_aps.vendor_id = aps.vendor_id
    );

DECLARE
    l_count NUMBER := SQL%ROWCOUNT;
BEGIN
    dbms_output.put_line('Step 1 Complete: ' || l_count || ' suppliers inserted.');
END;
/


/* ============================================================================
   STEP 2: MIGRATE AP_SUPPLIER_SITES_ALL (Vendor Sites)
   ============================================================================ */

PROMPT =====================================================================
PROMPT  STEP 2: MIGRATING AP_SUPPLIER_SITES_ALL
PROMPT =====================================================================

INSERT INTO ap.ap_supplier_sites_all (
    vendor_site_id,
    vendor_id,
    vendor_site_code,
    vendor_site_code_alt,
    org_id,
    -- Site flags
    purchasing_site_flag,
    pay_site_flag,
    rfq_only_site_flag,
    primary_pay_site_flag,
    -- Address
    address_line1,
    address_line2,
    address_line3,
    address_line4,
    city,
    state,
    zip,
    province,
    country,
    address_lines_alt,
    -- Contact info
    phone,
    fax,
    area_code,
    fax_area_code,
    email_address,
    -- Status
    inactive_date,
    -- Payment defaults
    payment_method_lookup_code,
    terms_id,
    terms_date_basis,
    always_take_disc_flag,
    pay_date_basis_lookup_code,
    pay_group_lookup_code,
    invoice_currency_code,
    payment_currency_code,
    payment_priority,
    -- Holds
    hold_all_payments_flag,
    hold_unmatched_invoices_flag,
    hold_future_payments_flag,
    -- Matching / invoicing
    match_option,
    auto_tax_calc_flag,
    auto_tax_calc_override,
    invoice_amount_limit,
    -- Accounting
    accts_pay_code_combination_id,
    prepay_code_combination_id,
    -- Ship/Bill To
    ship_to_location_id,
    bill_to_location_id,
    ship_via_lookup_code,
    freight_terms_lookup_code,
    fob_lookup_code,
    -- Tax
    vat_registration_num,
    tax_reporting_site_flag,
    -- Withholding
    withholding_status_lookup_code,
    withholding_start_date,
    -- DFF
    attribute_category,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    -- Audit
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login
)
SELECT
    apss.vendor_site_id,
    apss.vendor_id,
    apss.vendor_site_code,
    apss.vendor_site_code_alt,
    apss.org_id,
    -- Site flags
    apss.purchasing_site_flag,
    apss.pay_site_flag,
    apss.rfq_only_site_flag,
    apss.primary_pay_site_flag,
    -- Address
    apss.address_line1,
    apss.address_line2,
    apss.address_line3,
    apss.address_line4,
    apss.city,
    apss.state,
    apss.zip,
    apss.province,
    apss.country,
    apss.address_lines_alt,
    -- Contact info
    apss.phone,
    apss.fax,
    apss.area_code,
    apss.fax_area_code,
    apss.email_address,
    -- Status
    apss.inactive_date,
    -- Payment defaults
    apss.payment_method_lookup_code,
    apss.terms_id,
    apss.terms_date_basis,
    apss.always_take_disc_flag,
    apss.pay_date_basis_lookup_code,
    apss.pay_group_lookup_code,
    apss.invoice_currency_code,
    apss.payment_currency_code,
    apss.payment_priority,
    -- Holds
    apss.hold_all_payments_flag,
    apss.hold_unmatched_invoices_flag,
    apss.hold_future_payments_flag,
    -- Matching / invoicing
    apss.match_option,
    apss.auto_tax_calc_flag,
    apss.auto_tax_calc_override,
    apss.invoice_amount_limit,
    -- Accounting
    apss.accts_pay_code_combination_id,
    apss.prepay_code_combination_id,
    -- Ship/Bill To
    apss.ship_to_location_id,
    apss.bill_to_location_id,
    apss.ship_via_lookup_code,
    apss.freight_terms_lookup_code,
    apss.fob_lookup_code,
    -- Tax
    apss.vat_registration_num,
    apss.tax_reporting_site_flag,
    -- Withholding
    apss.withholding_status_lookup_code,
    apss.withholding_start_date,
    -- DFF
    apss.attribute_category,
    apss.attribute1,
    apss.attribute2,
    apss.attribute3,
    apss.attribute4,
    apss.attribute5,
    apss.attribute6,
    apss.attribute7,
    apss.attribute8,
    apss.attribute9,
    apss.attribute10,
    apss.attribute11,
    apss.attribute12,
    apss.attribute13,
    apss.attribute14,
    apss.attribute15,
    -- Audit
    -1,
    SYSDATE,
    -1,
    SYSDATE,
    -1
FROM
    apps.ap_supplier_sites_all@LEGACY_INSTANCE apss
WHERE
    -- Only sites whose vendor exists in new instance
    EXISTS (
        SELECT 1
        FROM   ap_suppliers new_aps
        WHERE  new_aps.vendor_id = apss.vendor_id
    )
    -- Scope: sites referenced on POs with receipts
    AND apss.vendor_site_id IN (
        SELECT DISTINCT ph.vendor_site_id
        FROM   apps.po_headers_all@LEGACY_INSTANCE    ph
        JOIN   apps.rcv_transactions@LEGACY_INSTANCE   rt ON rt.po_header_id = ph.po_header_id
        WHERE  rt.transaction_type IN ('RECEIVE', 'RETURN TO VENDOR')
        AND    rt.source_document_code = 'PO'
    )
    -- Skip already migrated
    AND NOT EXISTS (
        SELECT 1
        FROM   ap.ap_supplier_sites_all new_apss
        WHERE  new_apss.vendor_site_id = apss.vendor_site_id
    );

DECLARE
    l_count NUMBER := SQL%ROWCOUNT;
BEGIN
    dbms_output.put_line('Step 2 Complete: ' || l_count || ' supplier sites inserted.');
END;
/


/* ============================================================================
   STEP 3: MIGRATE AP_SUPPLIER_CONTACTS (Vendor Contacts)
   ============================================================================ */

PROMPT =====================================================================
PROMPT  STEP 3: MIGRATING AP_SUPPLIER_CONTACTS
PROMPT =====================================================================

INSERT INTO ap_supplier_contacts (
    vendor_contact_id,
    vendor_id,
    vendor_site_id,
    org_id,
    -- Name
    first_name,
    middle_name,
    last_name,
    prefix,
    title,
    -- Contact details
    area_code,
    phone,
    fax_area_code,
    fax,
    email_address,
    url,
    -- Status
    inactive_date,
    -- Department / mail stop
    department,
    mail_stop,
    -- DFF
    attribute_category,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    -- Audit
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login
)
SELECT
    apc.vendor_contact_id,
    apc.vendor_id,
    apc.vendor_site_id,
    apc.org_id,
    -- Name
    apc.first_name,
    apc.middle_name,
    apc.last_name,
    apc.prefix,
    apc.title,
    -- Contact details
    apc.area_code,
    apc.phone,
    apc.fax_area_code,
    apc.fax,
    apc.email_address,
    apc.url,
    -- Status
    apc.inactive_date,
    -- Department / mail stop
    apc.department,
    apc.mail_stop,
    -- DFF
    apc.attribute_category,
    apc.attribute1,
    apc.attribute2,
    apc.attribute3,
    apc.attribute4,
    apc.attribute5,
    apc.attribute6,
    apc.attribute7,
    apc.attribute8,
    apc.attribute9,
    apc.attribute10,
    apc.attribute11,
    apc.attribute12,
    apc.attribute13,
    apc.attribute14,
    apc.attribute15,
    -- Audit
    -1,
    SYSDATE,
    -1,
    SYSDATE,
    -1
FROM
    apps.ap_supplier_contacts@LEGACY_INSTANCE apc
WHERE
    -- Only contacts for migrated vendors
    EXISTS (
        SELECT 1
        FROM   ap_suppliers new_aps
        WHERE  new_aps.vendor_id = apc.vendor_id
    )
    -- Skip already migrated
    AND NOT EXISTS (
        SELECT 1
        FROM   ap_supplier_contacts new_apc
        WHERE  new_apc.vendor_contact_id = apc.vendor_contact_id
    );

DECLARE
    l_count NUMBER := SQL%ROWCOUNT;
BEGIN
    dbms_output.put_line('Step 3 Complete: ' || l_count || ' supplier contacts inserted.');
END;
/


/* ============================================================================
   STEP 4: MIGRATE IBY_EXTERNAL_PAYEES_ALL (External Payee Records)
   These link suppliers/sites to payment instruments in Oracle Payments.
   Must be migrated BEFORE bank account assignments (Step 6).
   ============================================================================ */

PROMPT =====================================================================
PROMPT  STEP 4: MIGRATING IBY_EXTERNAL_PAYEES_ALL
PROMPT =====================================================================

INSERT INTO iby_external_payees_all (
    ext_payee_id,
    payee_party_id,
    payment_function,
    org_id,
    org_type,
    supplier_site_id,
    default_payment_method_code,
    exclusive_payment_flag,
    -- Audit
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    object_version_number
)
SELECT
    iep.ext_payee_id,
    iep.payee_party_id,
    iep.payment_function,
    iep.org_id,
    iep.org_type,
    iep.supplier_site_id,
    iep.default_payment_method_code,
    iep.exclusive_payment_flag,
    -- Audit
    -1,
    SYSDATE,
    -1,
    SYSDATE,
    -1,
    1
FROM
    apps.iby_external_payees_all@LEGACY_INSTANCE iep
WHERE
    -- Only payees for migrated supplier sites
    iep.supplier_site_id IN (
        SELECT vendor_site_id
        FROM   ap.ap_supplier_sites_all
    )
    AND iep.payment_function = 'PAYABLES_DISB'
    -- Skip already migrated
    AND NOT EXISTS (
        SELECT 1
        FROM   iby_external_payees_all new_iep
        WHERE  new_iep.ext_payee_id = iep.ext_payee_id
    );

DECLARE
    l_count NUMBER := SQL%ROWCOUNT;
BEGIN
    dbms_output.put_line('Step 4 Complete: ' || l_count || ' external payee records inserted.');
END;
/


/* ============================================================================
   STEP 5: MIGRATE IBY_EXT_BANK_ACCOUNTS (Supplier Bank Accounts)
   ============================================================================ */

PROMPT =====================================================================
PROMPT  STEP 5: MIGRATING IBY_EXT_BANK_ACCOUNTS
PROMPT =====================================================================

INSERT INTO iby_ext_bank_accounts (
    ext_bank_account_id,
    bank_id,
    bank_name,
    branch_id,
    branch_name,
    bank_account_name,
    bank_account_num,
    bank_account_type,
    iban,
    currency_code,
    country_code,
    start_date,
    end_date,
    -- Owner
    acct_owner_party_id,
    -- Status
    status,
    -- DFF
    attribute_category,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    -- Audit
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    object_version_number
)
SELECT
    ieba.ext_bank_account_id,
    ieba.bank_id,
    ieba.bank_name,
    ieba.branch_id,
    ieba.branch_name,
    ieba.bank_account_name,
    ieba.bank_account_num,
    ieba.bank_account_type,
    ieba.iban,
    ieba.currency_code,
    ieba.country_code,
    ieba.start_date,
    ieba.end_date,
    -- Owner
    ieba.acct_owner_party_id,
    -- Status
    NVL(ieba.status, 'A'),
    -- DFF
    ieba.attribute_category,
    ieba.attribute1,
    ieba.attribute2,
    ieba.attribute3,
    ieba.attribute4,
    ieba.attribute5,
    -- Audit
    -1,
    SYSDATE,
    -1,
    SYSDATE,
    -1,
    1
FROM
    apps.iby_ext_bank_accounts@LEGACY_INSTANCE ieba
WHERE
    -- Only bank accounts that are assigned to migrated payees
    ieba.ext_bank_account_id IN (
        SELECT ipiu.instrument_id
        FROM   apps.iby_pmt_instr_uses_all@LEGACY_INSTANCE ipiu
        WHERE  ipiu.instrument_type = 'BANKACCOUNT'
        AND    ipiu.ext_pmt_party_id IN (
            SELECT iep.ext_payee_id
            FROM   apps.iby_external_payees_all@LEGACY_INSTANCE iep
            WHERE  iep.supplier_site_id IN (
                SELECT vendor_site_id
                FROM   ap.ap_supplier_sites_all
            )
            AND iep.payment_function = 'PAYABLES_DISB'
        )
    )
    -- Skip already migrated
    AND NOT EXISTS (
        SELECT 1
        FROM   iby_ext_bank_accounts new_ieba
        WHERE  new_ieba.ext_bank_account_id = ieba.ext_bank_account_id
    );

DECLARE
    l_count NUMBER := SQL%ROWCOUNT;
BEGIN
    dbms_output.put_line('Step 5 Complete: ' || l_count || ' bank accounts inserted.');
END;
/


/* ============================================================================
   STEP 6: MIGRATE IBY_PMT_INSTR_USES_ALL (Payment Instrument Assignments)
   Links bank accounts to supplier payee records.
   ============================================================================ */

PROMPT =====================================================================
PROMPT  STEP 6: MIGRATING IBY_PMT_INSTR_USES_ALL
PROMPT =====================================================================

INSERT INTO iby_pmt_instr_uses_all (
    instrument_payment_use_id,
    ext_pmt_party_id,
    instrument_type,
    instrument_id,
    payment_function,
    payment_flow,
    order_of_preference,
    start_date,
    end_date,
    -- Audit
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    object_version_number
)
SELECT
    ipiu.instrument_payment_use_id,
    ipiu.ext_pmt_party_id,
    ipiu.instrument_type,
    ipiu.instrument_id,
    ipiu.payment_function,
    ipiu.payment_flow,
    ipiu.order_of_preference,
    ipiu.start_date,
    ipiu.end_date,
    -- Audit
    -1,
    SYSDATE,
    -1,
    SYSDATE,
    -1,
    1
FROM
    apps.iby_pmt_instr_uses_all@LEGACY_INSTANCE ipiu
WHERE
    -- Only assignments for migrated payees
    ipiu.ext_pmt_party_id IN (
        SELECT ext_payee_id
        FROM   iby_external_payees_all
        WHERE  supplier_site_id IN (
            SELECT vendor_site_id
            FROM   ap.ap_supplier_sites_all
        )
        AND payment_function = 'PAYABLES_DISB'
    )
    -- Only for migrated bank accounts
    AND ipiu.instrument_type = 'BANKACCOUNT'
    AND EXISTS (
        SELECT 1
        FROM   iby_ext_bank_accounts new_ieba
        WHERE  new_ieba.ext_bank_account_id = ipiu.instrument_id
    )
    -- Skip already migrated
    AND NOT EXISTS (
        SELECT 1
        FROM   iby_pmt_instr_uses_all new_ipiu
        WHERE  new_ipiu.instrument_payment_use_id = ipiu.instrument_payment_use_id
    );

DECLARE
    l_count NUMBER := SQL%ROWCOUNT;
BEGIN
    dbms_output.put_line('Step 6 Complete: ' || l_count || ' payment instrument assignments inserted.');
END;
/


/* ============================================================================
   STEP 7: POST-MIGRATION VALIDATION
   ============================================================================ */

PROMPT =====================================================================
PROMPT  STEP 7: POST-MIGRATION VALIDATION
PROMPT =====================================================================

-- 7A: Missing suppliers (should return 0 rows)
PROMPT === 7A: Missing Suppliers (expect 0 rows) ===
SELECT DISTINCT
    ph.vendor_id,
    aps.vendor_name,
    aps.segment1 AS supplier_number
FROM
    apps.po_headers_all@LEGACY_INSTANCE       ph
    JOIN apps.rcv_transactions@LEGACY_INSTANCE rt  ON rt.po_header_id = ph.po_header_id
    JOIN apps.ap_suppliers@LEGACY_INSTANCE     aps ON aps.vendor_id   = ph.vendor_id
WHERE
    rt.transaction_type IN ('RECEIVE', 'RETURN TO VENDOR')
    AND rt.source_document_code = 'PO'
    AND NOT EXISTS (
        SELECT 1
        FROM   ap_suppliers new_aps
        WHERE  new_aps.vendor_id = ph.vendor_id
    );

-- 7B: Missing supplier sites (should return 0 rows)
PROMPT === 7B: Missing Supplier Sites (expect 0 rows) ===
SELECT DISTINCT
    ph.vendor_site_id,
    apss.vendor_site_code,
    aps.vendor_name
FROM
    apps.po_headers_all@LEGACY_INSTANCE              ph
    JOIN apps.rcv_transactions@LEGACY_INSTANCE        rt   ON rt.po_header_id     = ph.po_header_id
    JOIN apps.ap_suppliers@LEGACY_INSTANCE            aps  ON aps.vendor_id        = ph.vendor_id
    JOIN apps.ap_supplier_sites_all@LEGACY_INSTANCE   apss ON apss.vendor_site_id  = ph.vendor_site_id
WHERE
    rt.transaction_type IN ('RECEIVE', 'RETURN TO VENDOR')
    AND rt.source_document_code = 'PO'
    AND NOT EXISTS (
        SELECT 1
        FROM   ap.ap_supplier_sites_all new_apss
        WHERE  new_apss.vendor_site_id = ph.vendor_site_id
    );

-- 7C: Missing contacts for migrated suppliers
PROMPT === 7C: Missing Contacts (expect 0 rows) ===
SELECT
    apc.vendor_contact_id,
    apc.first_name,
    apc.last_name,
    aps.vendor_name
FROM
    apps.ap_supplier_contacts@LEGACY_INSTANCE apc
    JOIN apps.ap_suppliers@LEGACY_INSTANCE     aps ON aps.vendor_id = apc.vendor_id
WHERE
    EXISTS (
        SELECT 1
        FROM   ap_suppliers new_aps
        WHERE  new_aps.vendor_id = apc.vendor_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM   ap_supplier_contacts new_apc
        WHERE  new_apc.vendor_contact_id = apc.vendor_contact_id
    );

-- 7D: Migration summary counts
PROMPT === 7D: Migration Summary ===
SELECT 'AP_SUPPLIERS'             AS table_name, COUNT(*) AS new_count FROM ap_suppliers           WHERE created_by = -1
UNION ALL
SELECT 'AP_SUPPLIER_SITES_ALL',                  COUNT(*)              FROM ap.ap_supplier_sites_all WHERE created_by = -1
UNION ALL
SELECT 'AP_SUPPLIER_CONTACTS',                   COUNT(*)              FROM ap_supplier_contacts     WHERE created_by = -1
UNION ALL
SELECT 'IBY_EXTERNAL_PAYEES_ALL',                COUNT(*)              FROM iby_external_payees_all  WHERE created_by = -1
UNION ALL
SELECT 'IBY_EXT_BANK_ACCOUNTS',                  COUNT(*)              FROM iby_ext_bank_accounts    WHERE created_by = -1
UNION ALL
SELECT 'IBY_PMT_INSTR_USES_ALL',                 COUNT(*)              FROM iby_pmt_instr_uses_all   WHERE created_by = -1;

-- 7E: Suppliers with sites but no bank account assignments
PROMPT === 7E: Suppliers without bank accounts (informational) ===
SELECT
    aps.vendor_id,
    aps.vendor_name,
    apss.vendor_site_code,
    apss.org_id
FROM
    ap_suppliers aps
    JOIN ap.ap_supplier_sites_all apss ON apss.vendor_id = aps.vendor_id
WHERE
    aps.created_by = -1
    AND NOT EXISTS (
        SELECT 1
        FROM   iby_external_payees_all  iep
        JOIN   iby_pmt_instr_uses_all   ipiu ON ipiu.ext_pmt_party_id = iep.ext_payee_id
        WHERE  iep.supplier_site_id = apss.vendor_site_id
        AND    ipiu.instrument_type = 'BANKACCOUNT'
    );


/* ============================================================================
   COMMIT (uncomment after reviewing validation output)
   ============================================================================ */

-- COMMIT;
PROMPT === COMMIT is commented out. Uncomment after verifying all validation checks pass. ===


/* ============================================================================
   ROLLBACK (emergency use only - before COMMIT)
   ============================================================================ */

/*
ROLLBACK;

-- If already committed, delete migrated records by audit stamp:
DELETE FROM iby_pmt_instr_uses_all  WHERE created_by = -1 AND TRUNC(creation_date) = TRUNC(SYSDATE);
DELETE FROM iby_ext_bank_accounts   WHERE created_by = -1 AND TRUNC(creation_date) = TRUNC(SYSDATE);
DELETE FROM iby_external_payees_all WHERE created_by = -1 AND TRUNC(creation_date) = TRUNC(SYSDATE);
DELETE FROM ap_supplier_contacts    WHERE created_by = -1 AND TRUNC(creation_date) = TRUNC(SYSDATE);
DELETE FROM ap.ap_supplier_sites_all WHERE created_by = -1 AND TRUNC(creation_date) = TRUNC(SYSDATE);
DELETE FROM ap_suppliers            WHERE created_by = -1 AND TRUNC(creation_date) = TRUNC(SYSDATE);
COMMIT;
*/
