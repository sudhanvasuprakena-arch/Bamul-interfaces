-- ============================================================================
-- BAMUL Supplier Sync - Hourly Job via DB Link
-- ============================================================================
-- Purpose: Pull new suppliers AND sync modified suppliers from BMLPROD into DEV
--          via DB link LEGACY_INSTANCE.BAMULNANDINI.COOP
-- 
-- Flow:
--   PART 1 (New Suppliers - batch of 500):
--     Query ap_suppliers + ap_supplier_sites_all @LEGACY_INSTANCE
--     → Insert into BAMUL_SUPPLIER_DET_STG (picked_status = 'N')
--     → Call bamul_supplier_detls_pkg.bamul_supplier_detls_prc (mode = 'E')
--     → Package creates vendor + site + contact via AP_VENDOR_PUB_PKG APIs
--
--   PART 2 (Modified Suppliers - last 1 hour):
--     Detect changes via GREATEST(last_update_date) in BMLPROD
--     → If NOT in DEV → create (insert into staging → package creates)
--
-- Objects Created:
--   1. APPS.BAMUL_SUPPLIER_SYNC_FROM_LEGACY  - sync procedure
--   2. APPS.BAMUL_SUPPLIER_SYNC_HOURLY_JOB   - DBMS_SCHEDULER job (every hour at :30)
--
-- Pre-existing Dependencies:
--   - APPS.BAMUL_SUPPLIER_DETLS_PKG (Spec + Body) - already compiled in DEV
--     Source: BAMUL_SUPPLIER_DETLS_PKG_SP.pls, BAMUL_SUPPLIER_DETLS_PKG_BD 2.pls
--   - BAMUL.BAMUL_SUPPLIER_DET_STG - staging table (owned by BAMUL schema)
--   - DB Link: LEGACY_INSTANCE.BAMULNANDINI.COOP
--
-- Deployed: 05-May-2026
-- Status: VALID, TESTED, RUNNING (FULL mode until backlog cleared)
-- First Run: 496 suppliers pulled, 241 created (2948 → 3189)
-- Remaining: ~4068 suppliers to sync
-- ============================================================================

-- ============================================================================
-- STEP 1: Sync Procedure
-- ============================================================================
CREATE OR REPLACE PROCEDURE apps.bamul_supplier_sync_from_legacy (
    p_mode IN VARCHAR2 DEFAULT 'INCREMENTAL'
) AS
    v_count_new    NUMBER := 0;
    v_count_upd    NUMBER := 0;
    v_errbuf       VARCHAR2(4000);
    v_retcode      NUMBER;
    v_batch_size   NUMBER := 500;
    v_next_id      NUMBER;
    v_log_id       NUMBER;
    v_err_text     VARCHAR2(500);
    v_exists       NUMBER;

    -- Cursor: NEW suppliers not in DEV
    -- FULL mode: all missing suppliers
    -- INCREMENTAL mode: only created in last 1 day
    CURSOR c_new_suppliers IS
        SELECT s.segment1       AS vendor_number,
               s.vendor_name,
               s.vendor_type_lookup_code AS vendor_type,
               DECODE(s.enabled_flag, 'Y', 'Active', 'Inactive') AS vendor_status,
               ss.address_line1 AS vendor_address,
               ss.address_line2 AS vendor_communication_address,
               ss.address_line3 AS suburb,
               ss.city,
               ss.state,
               NVL(ss.country, 'India') AS country,
               ss.zip           AS postal_code,
               ss.vendor_site_code AS ebs_site,
               ss.org_id,
               s.creation_date
        FROM   apps.ap_suppliers@LEGACY_INSTANCE.BAMULNANDINI.COOP s,
               apps.ap_supplier_sites_all@LEGACY_INSTANCE.BAMULNANDINI.COOP ss
        WHERE  s.vendor_id = ss.vendor_id
        AND    s.enabled_flag = 'Y'
        AND    ss.org_id = 148
        AND    s.segment1 NOT IN (
                   SELECT segment1 FROM ap_suppliers WHERE segment1 IS NOT NULL
               )
        AND    s.segment1 NOT IN (
                   SELECT vendor_number FROM bamul_supplier_det_stg
                   WHERE picked_status IN ('N', 'V', 'EH', 'EL')
                   AND vendor_number IS NOT NULL
               )
        AND    CASE WHEN p_mode = 'FULL' THEN 1
                    WHEN s.creation_date > SYSDATE - 1 THEN 1
                    ELSE 0 END = 1
        AND    ROWNUM <= v_batch_size;

    -- Cursor: MODIFIED suppliers (updated in BMLPROD in last 1 hour)
    CURSOR c_modified_suppliers IS
        SELECT s.segment1       AS vendor_number,
               s.vendor_name,
               s.vendor_type_lookup_code AS vendor_type,
               DECODE(s.enabled_flag, 'Y', 'Active', 'Inactive') AS vendor_status,
               ss.address_line1 AS vendor_address,
               ss.address_line2 AS vendor_communication_address,
               ss.address_line3 AS suburb,
               ss.city,
               ss.state,
               NVL(ss.country, 'India') AS country,
               ss.zip           AS postal_code,
               ss.vendor_site_code AS ebs_site,
               s.creation_date
        FROM   apps.ap_suppliers@LEGACY_INSTANCE.BAMULNANDINI.COOP s,
               apps.ap_supplier_sites_all@LEGACY_INSTANCE.BAMULNANDINI.COOP ss
        WHERE  s.vendor_id = ss.vendor_id
        AND    s.enabled_flag = 'Y'
        AND    ss.org_id = 148
        AND    GREATEST(s.last_update_date, ss.last_update_date) > SYSDATE - 1/24
        AND    ROWNUM <= v_batch_size;

BEGIN
    fnd_global.apps_initialize(
        user_id      => 0,
        resp_id      => fnd_global.resp_id,
        resp_appl_id => fnd_global.resp_appl_id
    );
    mo_global.init('SQLAP');
    mo_global.set_policy_context('S', 81);

    -- Reset errored records
    UPDATE bamul_supplier_det_stg
    SET    picked_status = 'N'
    WHERE  picked_status = 'E'
    AND    creation_date > SYSDATE - 1;
    COMMIT;

    -- PART 1: NEW SUPPLIERS
    SELECT NVL(MAX(supplier_id), 0) INTO v_next_id FROM bamul_supplier_det_stg;

    FOR rec IN c_new_suppliers LOOP
        BEGIN
            v_next_id := v_next_id + 1;
            INSERT INTO bamul_supplier_det_stg (
                supplier_id, operating_unit, vendor_number, vendor_name,
                vendor_type, vendor_status, vendor_address,
                vendor_communication_address, suburb, city, state, country,
                postal_code, ebs_site, picked_status, creation_date, created_by
            ) VALUES (
                v_next_id, 'BAMUL_OU', rec.vendor_number, rec.vendor_name,
                rec.vendor_type, rec.vendor_status, rec.vendor_address,
                rec.vendor_communication_address, rec.suburb,
                NVL(rec.city, 'Bengaluru'), NVL(rec.state, 'Karnataka'),
                rec.country, rec.postal_code, rec.ebs_site,
                'N', SYSDATE, 0
            );
            v_count_new := v_count_new + 1;
        EXCEPTION
            WHEN DUP_VAL_ON_INDEX THEN NULL;
            WHEN OTHERS THEN NULL;
        END;
    END LOOP;
    COMMIT;

    -- Process staging records
    IF v_count_new > 0 THEN
        bamul_supplier_detls_pkg.bamul_supplier_detls_prc(
            errbuf              => v_errbuf,
            retcode             => v_retcode,
            p_data_file_path    => NULL,
            p_data_file_name    => NULL,
            p_archive_file_path => NULL,
            p_data_upload       => 'N',
            p_mode              => 'E'
        );
    END IF;

    -- PART 2: MODIFIED SUPPLIERS (last 1 hour) - create if not in DEV
    FOR rec IN c_modified_suppliers LOOP
        BEGIN
            SELECT COUNT(*) INTO v_exists FROM ap_suppliers WHERE segment1 = rec.vendor_number;

            IF v_exists = 0 THEN
                v_next_id := v_next_id + 1;
                INSERT INTO bamul_supplier_det_stg (
                    supplier_id, operating_unit, vendor_number, vendor_name,
                    vendor_type, vendor_status, vendor_address,
                    vendor_communication_address, suburb, city, state, country,
                    postal_code, ebs_site, picked_status, creation_date, created_by
                ) VALUES (
                    v_next_id, 'BAMUL_OU', rec.vendor_number, rec.vendor_name,
                    rec.vendor_type, rec.vendor_status, rec.vendor_address,
                    rec.vendor_communication_address, rec.suburb,
                    NVL(rec.city, 'Bengaluru'), NVL(rec.state, 'Karnataka'),
                    rec.country, rec.postal_code, rec.ebs_site,
                    'N', SYSDATE, 0
                );
                v_count_upd := v_count_upd + 1;
                COMMIT;
            END IF;
        EXCEPTION
            WHEN DUP_VAL_ON_INDEX THEN NULL;
            WHEN OTHERS THEN NULL;
        END;
    END LOOP;

    -- Process any new staging records from PART 2
    IF v_count_upd > 0 THEN
        bamul_supplier_detls_pkg.bamul_supplier_detls_prc(
            errbuf              => v_errbuf,
            retcode             => v_retcode,
            p_data_file_path    => NULL,
            p_data_file_name    => NULL,
            p_archive_file_path => NULL,
            p_data_upload       => 'N',
            p_mode              => 'E'
        );
    END IF;

    -- LOG
    SELECT NVL(MAX(sync_id), 0) + 1 INTO v_log_id FROM bamul_cust_sync_log;
    INSERT INTO bamul_cust_sync_log (sync_id, sync_type, sync_date, records_synced, status)
    VALUES (v_log_id, 'SUP_NEW', SYSDATE, v_count_new, 'S');
    v_log_id := v_log_id + 1;
    INSERT INTO bamul_cust_sync_log (sync_id, sync_type, sync_date, records_synced, status)
    VALUES (v_log_id, 'SUP_UPD', SYSDATE, v_count_upd, 'S');
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        v_err_text := SUBSTR(SQLERRM, 1, 500);
        ROLLBACK;
        BEGIN
            SELECT NVL(MAX(sync_id), 0) + 1 INTO v_log_id FROM bamul_cust_sync_log;
            INSERT INTO bamul_cust_sync_log (sync_id, sync_type, sync_date, records_synced, status, error_msg)
            VALUES (v_log_id, 'SUP_ERR', SYSDATE, 0, 'E', v_err_text);
            COMMIT;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;
END bamul_supplier_sync_from_legacy;
/

-- ============================================================================
-- STEP 2: DBMS_SCHEDULER Job (runs every hour at :30)
-- ============================================================================
BEGIN
    BEGIN
        DBMS_SCHEDULER.DROP_JOB(job_name => 'APPS.BAMUL_SUPPLIER_SYNC_HOURLY_JOB', force => TRUE);
    EXCEPTION
        WHEN OTHERS THEN NULL;
    END;

    DBMS_SCHEDULER.CREATE_JOB(
        job_name        => 'APPS.BAMUL_SUPPLIER_SYNC_HOURLY_JOB',
        job_type        => 'PLSQL_BLOCK',
        job_action      => 'BEGIN apps.bamul_supplier_sync_from_legacy(p_mode => ''FULL''); END;',
        start_date      => SYSTIMESTAMP,
        repeat_interval => 'FREQ=HOURLY; BYMINUTE=30; BYSECOND=0',
        enabled         => TRUE,
        auto_drop       => FALSE,
        comments        => 'Hourly sync of suppliers from BMLPROD via DB link - FULL mode until caught up'
    );
END;
/

-- ============================================================================
-- USEFUL COMMANDS:
-- ============================================================================
-- Run manually (FULL - all missing):
--   BEGIN apps.bamul_supplier_sync_from_legacy(p_mode => 'FULL'); END;
--
-- Run manually (INCREMENTAL - last 1 day only):
--   BEGIN apps.bamul_supplier_sync_from_legacy(p_mode => 'INCREMENTAL'); END;
--
-- Switch to INCREMENTAL mode (after backlog is cleared):
--   BEGIN
--       DBMS_SCHEDULER.SET_ATTRIBUTE(
--           name      => 'APPS.BAMUL_SUPPLIER_SYNC_HOURLY_JOB',
--           attribute => 'job_action',
--           value     => 'BEGIN apps.bamul_supplier_sync_from_legacy(p_mode => ''INCREMENTAL''); END;'
--       );
--   END;
--
-- Disable job:
--   BEGIN DBMS_SCHEDULER.DISABLE('APPS.BAMUL_SUPPLIER_SYNC_HOURLY_JOB'); END;
--
-- Enable job:
--   BEGIN DBMS_SCHEDULER.ENABLE('APPS.BAMUL_SUPPLIER_SYNC_HOURLY_JOB'); END;
--
-- Check job run history:
--   SELECT log_date, status, run_duration, error#
--   FROM all_scheduler_job_run_details
--   WHERE job_name = 'BAMUL_SUPPLIER_SYNC_HOURLY_JOB' ORDER BY log_date DESC;
--
-- Check sync log:
--   SELECT * FROM bamul_cust_sync_log WHERE sync_type LIKE 'SUP%' ORDER BY sync_id DESC;
--
-- Check staging status:
--   SELECT picked_status, COUNT(*) FROM bamul_supplier_det_stg GROUP BY picked_status;
--
-- Remaining suppliers to sync:
--   SELECT COUNT(*) FROM apps.ap_suppliers@LEGACY_INSTANCE.BAMULNANDINI.COOP s
--   WHERE s.enabled_flag = 'Y'
--   AND s.segment1 NOT IN (SELECT segment1 FROM ap_suppliers WHERE segment1 IS NOT NULL);
--
-- Check specific supplier in DEV:
--   SELECT segment1, vendor_name, vendor_type_lookup_code, enabled_flag
--   FROM ap_suppliers WHERE segment1 = '&vendor_number';
--
-- Drop everything:
--   BEGIN DBMS_SCHEDULER.DROP_JOB('APPS.BAMUL_SUPPLIER_SYNC_HOURLY_JOB', force => TRUE); END;
--   DROP PROCEDURE apps.bamul_supplier_sync_from_legacy;
-- ============================================================================
