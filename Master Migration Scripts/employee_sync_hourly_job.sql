-- ============================================================================
-- BAMUL Employee Sync - Hourly Job via DB Link
-- ============================================================================
-- Purpose: Pull new employees AND sync modified employees from BMLPROD into DEV
--          via DB link LEGACY_INSTANCE.BAMULNANDINI.COOP
-- 
-- Flow:
--   PART 1 (New Employees - batch of 500):
--     Query per_all_people_f + per_addresses + per_phones @LEGACY_INSTANCE
--     → Insert into BAMUL_EMPLOYEE_DET_STG (picked_status = 'N')
--     → Call bamul_employee_detls_pkg.bamul_employee_detls_prc (mode = 'E')
--     → Package creates employee + address + phone via HR APIs
--
--   PART 2 (Modified Employees - last 1 hour):
--     Detect changes via last_update_date in BMLPROD per_all_people_f
--     → If NOT in DEV → create (insert into staging → package creates)
--
-- Objects Created:
--   1. APPS.BAMUL_EMP_SYNC_FROM_LEGACY   - sync procedure
--   2. APPS.BAMUL_EMP_SYNC_HOURLY_JOB    - DBMS_SCHEDULER job (every hour at :00)
--
-- Pre-existing Dependencies:
--   - APPS.BAMUL_EMPLOYEE_DETLS_PKG (Spec + Body) - already compiled in DEV
--     Source: BAMUL_EMPLOYEE_DETLS_PKG_SP.pls, BAMUL_EMPLOYEE_DETLS_PKG_BD.pls
--   - APPS.BAMUL_EMPLOYEE_DET_STG - staging table
--   - DB Link: LEGACY_INSTANCE.BAMULNANDINI.COOP
--
-- APIs Used by Package:
--   - HR_EMPLOYEE_API.CREATE_EMPLOYEE (creates person record)
--   - HR_PERSON_ADDRESS_API.CREATE_PERSON_ADDRESS (creates address)
--   - HR_PHONE_API.CREATE_PHONE (creates phone contact)
--
-- Deployed: 05-May-2026
-- Status: VALID, DEPLOYED, RUNNING (INCREMENTAL mode)
-- ============================================================================

-- ============================================================================
-- STEP 1: Sync Procedure
-- ============================================================================
CREATE OR REPLACE PROCEDURE apps.bamul_emp_sync_from_legacy (
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

    -- Cursor: NEW employees from BMLPROD not in DEV
    -- FULL mode: all missing employees
    -- INCREMENTAL mode: only created in last 1 day
    CURSOR c_new_employees IS
        SELECT p.employee_number,
               p.last_name,
               p.first_name,
               p.title,
               DECODE(p.sex, 'M', 'Male', 'F', 'Female', p.sex) AS gender,
               p.date_of_birth,
               FLOOR(MONTHS_BETWEEN(SYSDATE, p.date_of_birth)/12) AS age,
               DECODE(p.marital_status, 'M', 'Married', 'S', 'Single', 'D', 'Divorced', 'W', 'Widowed', p.marital_status) AS marital_status,
               p.nationality,
               pa.address_line1,
               pa.address_line2,
               pa.address_line3,
               NVL(pa.town_or_city, 'BENGALURU') AS city,
               pa.postal_code AS zip_code,
               NVL(pa.country, 'IN') AS country,
               ph.phone_number AS contact_number,
               p.email_address,
               p.start_date AS employee_start_date
        FROM   apps.per_all_people_f@LEGACY_INSTANCE.BAMULNANDINI.COOP p,
               apps.per_addresses@LEGACY_INSTANCE.BAMULNANDINI.COOP pa,
               apps.per_phones@LEGACY_INSTANCE.BAMULNANDINI.COOP ph
        WHERE  p.current_employee_flag = 'Y'
        AND    p.employee_number IS NOT NULL
        AND    TRUNC(SYSDATE) BETWEEN p.effective_start_date AND p.effective_end_date
        AND    pa.person_id(+) = p.person_id
        AND    pa.primary_flag(+) = 'Y'
        AND    NVL(pa.date_to(+), SYSDATE) >= SYSDATE
        AND    ph.parent_id(+) = p.person_id
        AND    ph.parent_table(+) = 'PER_ALL_PEOPLE_F'
        AND    p.employee_number NOT IN (
                   SELECT employee_number FROM per_all_people_f
                   WHERE employee_number IS NOT NULL
               )
        AND    p.employee_number NOT IN (
                   SELECT emp_number FROM bamul_employee_det_stg
                   WHERE picked_status IN ('N', 'V', 'EH', 'EAD')
                   AND emp_number IS NOT NULL
               )
        AND    CASE WHEN p_mode = 'FULL' THEN 1
                    WHEN p.creation_date > SYSDATE - 1 THEN 1
                    ELSE 0 END = 1
        AND    ROWNUM <= v_batch_size;

    -- Cursor: MODIFIED employees (updated in BMLPROD in last 1 hour)
    CURSOR c_modified_employees IS
        SELECT p.employee_number,
               p.last_name,
               p.first_name,
               p.title,
               DECODE(p.sex, 'M', 'Male', 'F', 'Female', p.sex) AS gender,
               p.date_of_birth,
               FLOOR(MONTHS_BETWEEN(SYSDATE, p.date_of_birth)/12) AS age,
               DECODE(p.marital_status, 'M', 'Married', 'S', 'Single', 'D', 'Divorced', 'W', 'Widowed', p.marital_status) AS marital_status,
               p.nationality,
               pa.address_line1,
               pa.address_line2,
               pa.address_line3,
               NVL(pa.town_or_city, 'BENGALURU') AS city,
               pa.postal_code AS zip_code,
               NVL(pa.country, 'IN') AS country,
               ph.phone_number AS contact_number,
               p.email_address,
               p.start_date AS employee_start_date
        FROM   apps.per_all_people_f@LEGACY_INSTANCE.BAMULNANDINI.COOP p,
               apps.per_addresses@LEGACY_INSTANCE.BAMULNANDINI.COOP pa,
               apps.per_phones@LEGACY_INSTANCE.BAMULNANDINI.COOP ph
        WHERE  p.current_employee_flag = 'Y'
        AND    p.employee_number IS NOT NULL
        AND    TRUNC(SYSDATE) BETWEEN p.effective_start_date AND p.effective_end_date
        AND    pa.person_id(+) = p.person_id
        AND    pa.primary_flag(+) = 'Y'
        AND    NVL(pa.date_to(+), SYSDATE) >= SYSDATE
        AND    ph.parent_id(+) = p.person_id
        AND    ph.parent_table(+) = 'PER_ALL_PEOPLE_F'
        AND    p.last_update_date > SYSDATE - 1/24
        AND    ROWNUM <= v_batch_size;

BEGIN
    fnd_global.apps_initialize(
        user_id      => 0,
        resp_id      => fnd_global.resp_id,
        resp_appl_id => fnd_global.resp_appl_id
    );

    -- Reset errored/validated records
    UPDATE bamul_employee_det_stg
    SET    picked_status = 'N'
    WHERE  picked_status IN ('E', 'V')
    AND    creation_date > SYSDATE - 1;
    COMMIT;

    -- PART 1: NEW EMPLOYEES
    SELECT NVL(MAX(employee_stg_id), 0) INTO v_next_id FROM bamul_employee_det_stg;

    FOR rec IN c_new_employees LOOP
        BEGIN
            v_next_id := v_next_id + 1;
            INSERT INTO bamul_employee_det_stg (
                employee_stg_id, emp_number, last_name, first_name, title,
                gender, date_of_birth, age, marital_status, nationality,
                address_line_1, address_line_2, address_line_3,
                city, zip_code, country, contact_number, email_address,
                employee_start_date, picked_status, creation_date, created_by
            ) VALUES (
                v_next_id, rec.employee_number, rec.last_name, rec.first_name, rec.title,
                rec.gender, rec.date_of_birth, rec.age, rec.marital_status, rec.nationality,
                rec.address_line1, rec.address_line2, rec.address_line3,
                rec.city, rec.zip_code, rec.country, rec.contact_number, rec.email_address,
                rec.employee_start_date, 'N', SYSDATE, 0
            );
            v_count_new := v_count_new + 1;
        EXCEPTION
            WHEN DUP_VAL_ON_INDEX THEN NULL;
            WHEN OTHERS THEN NULL;
        END;
    END LOOP;
    COMMIT;

    -- Process staging records
    bamul_employee_detls_pkg.bamul_employee_detls_prc(
        errbuf              => v_errbuf,
        retcode             => v_retcode,
        p_data_file_path    => NULL,
        p_data_file_name    => NULL,
        p_archive_file_path => NULL,
        p_data_upload       => 'N',
        p_mode              => 'E'
    );

    -- PART 2: MODIFIED EMPLOYEES (last 1 hour) - create if not in DEV
    FOR rec IN c_modified_employees LOOP
        BEGIN
            SELECT COUNT(*) INTO v_exists
            FROM per_all_people_f
            WHERE employee_number = rec.employee_number
            AND ROWNUM = 1;

            IF v_exists = 0 THEN
                v_next_id := v_next_id + 1;
                INSERT INTO bamul_employee_det_stg (
                    employee_stg_id, emp_number, last_name, first_name, title,
                    gender, date_of_birth, age, marital_status, nationality,
                    address_line_1, address_line_2, address_line_3,
                    city, zip_code, country, contact_number, email_address,
                    employee_start_date, picked_status, creation_date, created_by
                ) VALUES (
                    v_next_id, rec.employee_number, rec.last_name, rec.first_name, rec.title,
                    rec.gender, rec.date_of_birth, rec.age, rec.marital_status, rec.nationality,
                    rec.address_line1, rec.address_line2, rec.address_line3,
                    rec.city, rec.zip_code, rec.country, rec.contact_number, rec.email_address,
                    rec.employee_start_date, 'N', SYSDATE, 0
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
        bamul_employee_detls_pkg.bamul_employee_detls_prc(
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
    VALUES (v_log_id, 'EMP_NEW', SYSDATE, v_count_new, 'S');
    v_log_id := v_log_id + 1;
    INSERT INTO bamul_cust_sync_log (sync_id, sync_type, sync_date, records_synced, status)
    VALUES (v_log_id, 'EMP_UPD', SYSDATE, v_count_upd, 'S');
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        v_err_text := SUBSTR(SQLERRM, 1, 500);
        ROLLBACK;
        BEGIN
            SELECT NVL(MAX(sync_id), 0) + 1 INTO v_log_id FROM bamul_cust_sync_log;
            INSERT INTO bamul_cust_sync_log (sync_id, sync_type, sync_date, records_synced, status, error_msg)
            VALUES (v_log_id, 'EMP_ERR', SYSDATE, 0, 'E', v_err_text);
            COMMIT;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;
END bamul_emp_sync_from_legacy;
/

-- ============================================================================
-- STEP 2: DBMS_SCHEDULER Job (runs every hour at :00)
-- ============================================================================
BEGIN
    BEGIN
        DBMS_SCHEDULER.DROP_JOB(job_name => 'APPS.BAMUL_EMP_SYNC_HOURLY_JOB', force => TRUE);
    EXCEPTION
        WHEN OTHERS THEN NULL;
    END;

    DBMS_SCHEDULER.CREATE_JOB(
        job_name        => 'APPS.BAMUL_EMP_SYNC_HOURLY_JOB',
        job_type        => 'PLSQL_BLOCK',
        job_action      => 'BEGIN apps.bamul_emp_sync_from_legacy(p_mode => ''INCREMENTAL''); END;',
        start_date      => SYSTIMESTAMP,
        repeat_interval => 'FREQ=HOURLY; BYMINUTE=0; BYSECOND=0',
        enabled         => TRUE,
        auto_drop       => FALSE,
        comments        => 'Hourly sync of employees from BMLPROD via DB link - new (last 1 day) + modified (last 1 hour)'
    );
END;
/

-- ============================================================================
-- USEFUL COMMANDS:
-- ============================================================================
-- Run manually (INCREMENTAL - last 1 day new + last 1 hour modified):
--   BEGIN apps.bamul_emp_sync_from_legacy(p_mode => 'INCREMENTAL'); END;
--
-- Run manually (FULL - all missing employees):
--   BEGIN apps.bamul_emp_sync_from_legacy(p_mode => 'FULL'); END;
--
-- Disable job:
--   BEGIN DBMS_SCHEDULER.DISABLE('APPS.BAMUL_EMP_SYNC_HOURLY_JOB'); END;
--
-- Enable job:
--   BEGIN DBMS_SCHEDULER.ENABLE('APPS.BAMUL_EMP_SYNC_HOURLY_JOB'); END;
--
-- Check job run history:
--   SELECT log_date, status, run_duration, error#
--   FROM all_scheduler_job_run_details
--   WHERE job_name = 'BAMUL_EMP_SYNC_HOURLY_JOB' ORDER BY log_date DESC;
--
-- Check sync log:
--   SELECT * FROM bamul_cust_sync_log WHERE sync_type LIKE 'EMP%' ORDER BY sync_id DESC;
--
-- Check staging status:
--   SELECT picked_status, COUNT(*) FROM bamul_employee_det_stg GROUP BY picked_status;
--
-- Remaining employees to sync:
--   SELECT COUNT(DISTINCT p.employee_number)
--   FROM apps.per_all_people_f@LEGACY_INSTANCE.BAMULNANDINI.COOP p
--   WHERE p.current_employee_flag = 'Y' AND p.employee_number IS NOT NULL
--   AND TRUNC(SYSDATE) BETWEEN p.effective_start_date AND p.effective_end_date
--   AND p.employee_number NOT IN (
--       SELECT employee_number FROM per_all_people_f WHERE employee_number IS NOT NULL);
--
-- Check specific employee in DEV:
--   SELECT employee_number, full_name, start_date
--   FROM per_all_people_f WHERE employee_number = '&emp_number'
--   AND TRUNC(SYSDATE) BETWEEN effective_start_date AND effective_end_date;
--
-- Check specific employee in BMLPROD (from DEV via DB link):
--   SELECT employee_number, full_name, start_date
--   FROM apps.per_all_people_f@LEGACY_INSTANCE.BAMULNANDINI.COOP
--   WHERE employee_number = '&emp_number'
--   AND TRUNC(SYSDATE) BETWEEN effective_start_date AND effective_end_date;
--
-- See all 3 sync jobs:
--   SELECT job_name, enabled, state, repeat_interval, next_run_date, run_count
--   FROM all_scheduler_jobs
--   WHERE job_name IN ('BAMUL_CUST_SYNC_HOURLY_JOB','BAMUL_SUPPLIER_SYNC_HOURLY_JOB','BAMUL_EMP_SYNC_HOURLY_JOB')
--   AND owner = 'APPS';
--
-- Drop everything:
--   BEGIN DBMS_SCHEDULER.DROP_JOB('APPS.BAMUL_EMP_SYNC_HOURLY_JOB', force => TRUE); END;
--   DROP PROCEDURE apps.bamul_emp_sync_from_legacy;
-- ============================================================================
