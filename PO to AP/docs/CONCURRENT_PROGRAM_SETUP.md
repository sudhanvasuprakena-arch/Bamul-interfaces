# PO-AP Interface Concurrent Program Setup Guide

## Overview

This document describes how to register the PO-AP Receipt Interface as an Oracle EBS concurrent program so it can be scheduled and run through the Concurrent Manager, replacing or complementing the Node.js scheduler (`run_po_ap_interface.js`).

The concurrent program runs:

| Process | Description |
|---------|-------------|
| **Process A** | Receipt-to-Invoice — creates AP Standard Invoices from PO receipts (with RTV netting) |
| **Process B** | RTV-to-Credit-Memo — creates AP Credit Memos for post-invoice Return To Vendor transactions |

Process B is skipped if Process A returns a fatal error (retcode = 2).

---

## Prerequisites

- `XXCUST_PO_AP_INTERFACE_PKG` package (spec + body) is compiled and valid in the new EBS instance.
- `XXCUST_PO_AP_INTERFACE_LOG` and `COA MAPPING` tables exist.
- Database link `LEGACY_INSTANCE` is configured and grants are in place.
- The APPS schema user has EXECUTE privilege on the package.

---

## Step 1: Create the Wrapper Procedure

The Concurrent Manager requires a procedure with the standard `(errbuf OUT, retcode OUT, ...)` signature. Deploy this under the `APPS` schema (or your custom schema with appropriate synonyms/grants).

```sql
CREATE OR REPLACE PROCEDURE XXCUST_PO_AP_CONC_PROG (
    errbuf              OUT VARCHAR2,
    retcode             OUT NUMBER,
    p_operating_unit    IN  VARCHAR2 DEFAULT NULL,
    p_date_from         IN  VARCHAR2 DEFAULT NULL,   -- DD-MON-YYYY
    p_date_to           IN  VARCHAR2 DEFAULT NULL,   -- DD-MON-YYYY
    p_po_number         IN  VARCHAR2 DEFAULT NULL,
    p_debug_mode        IN  VARCHAR2 DEFAULT 'N'
) IS
    l_errbuf_a   VARCHAR2(2000);
    l_retcode_a  NUMBER;
    l_errbuf_b   VARCHAR2(2000);
    l_retcode_b  NUMBER;
BEGIN
    -- ============================================================
    -- Process A: Receipt-to-Invoice
    -- ============================================================
    FND_FILE.PUT_LINE(FND_FILE.LOG, '=== Starting Process A: Receipt-to-Invoice ===');

    XXCUST_PO_AP_INTERFACE_PKG.run_receipt_interface(
        p_errbuf            => l_errbuf_a,
        p_retcode           => l_retcode_a,
        p_operating_unit    => p_operating_unit,
        p_receipt_date_from => p_date_from,
        p_receipt_date_to   => p_date_to,
        p_po_number         => p_po_number,
        p_debug_mode        => p_debug_mode
    );

    FND_FILE.PUT_LINE(FND_FILE.LOG, 'Process A Return Code: ' || l_retcode_a);
    FND_FILE.PUT_LINE(FND_FILE.LOG, 'Process A Message: ' || l_errbuf_a);

    -- Fatal failure in Process A — skip Process B
    IF l_retcode_a = 2 THEN
        errbuf  := 'Process A failed: ' || l_errbuf_a;
        retcode := 2;
        RETURN;
    END IF;

    -- ============================================================
    -- Process B: RTV-to-Credit-Memo
    -- ============================================================
    FND_FILE.PUT_LINE(FND_FILE.LOG, '=== Starting Process B: RTV-to-Credit-Memo ===');

    XXCUST_PO_AP_INTERFACE_PKG.run_rtv_interface(
        p_errbuf         => l_errbuf_b,
        p_retcode        => l_retcode_b,
        p_operating_unit => p_operating_unit,
        p_rtv_date_from  => p_date_from,
        p_rtv_date_to    => p_date_to,
        p_po_number      => p_po_number,
        p_debug_mode     => p_debug_mode
    );

    FND_FILE.PUT_LINE(FND_FILE.LOG, 'Process B Return Code: ' || l_retcode_b);
    FND_FILE.PUT_LINE(FND_FILE.LOG, 'Process B Message: ' || l_errbuf_b);

    -- ============================================================
    -- Final status — worst of A and B
    -- ============================================================
    retcode := GREATEST(NVL(l_retcode_a, 0), NVL(l_retcode_b, 0));

    IF retcode = 0 THEN
        errbuf := 'Both processes completed successfully.';
    ELSIF retcode = 1 THEN
        errbuf := 'Completed with warnings. A: ' || l_errbuf_a || ' | B: ' || l_errbuf_b;
    ELSE
        errbuf := 'Errors encountered. A: ' || l_errbuf_a || ' | B: ' || l_errbuf_b;
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        errbuf  := 'Unexpected error: ' || SQLERRM;
        retcode := 2;
        FND_FILE.PUT_LINE(FND_FILE.LOG, errbuf);
END XXCUST_PO_AP_CONC_PROG;
/
```

---

## Step 2: Register the Executable

> **Important:** Call `FND_GLOBAL.APPS_INITIALIZE` before any `FND_PROGRAM` API call.

```sql
BEGIN
    FND_GLOBAL.APPS_INITIALIZE(
        user_id      => 0,       -- SYSADMIN
        resp_id      => 20420,   -- System Administrator
        resp_appl_id => 1        -- SYSADMIN application
    );

    FND_PROGRAM.EXECUTABLE(
        executable          => 'XXCUST_PO_AP_EXEC',
        application         => 'SQLAP',
        short_name          => 'XXCUST_PO_AP_EXEC',
        description         => 'PO-AP Interface Executable',
        execution_method    => 'PL/SQL Stored Procedure',
        execution_file_name => 'XXCUST_PO_AP_CONC_PROG'
    );
    COMMIT;
END;
/
```

**Manual alternative (System Administrator UI):**

1. Navigate to **System Administrator → Concurrent → Program → Executable**
2. Fill in:

| Field | Value |
|-------|-------|
| Executable | `XXCUST_PO_AP_EXEC` |
| Short Name | `XXCUST_PO_AP_EXEC` |
| Application | Payables (`SQLAP`) |
| Execution Method | PL/SQL Stored Procedure |
| Execution File Name | `XXCUST_PO_AP_CONC_PROG` |

---

## Step 3: Register the Concurrent Program

> **Critical:** You must pass `use_in_srs => 'Y'` so users can submit from the Submit Request form and so parameters can be registered. The default is `'N'`.

```sql
BEGIN
    FND_GLOBAL.APPS_INITIALIZE(0, 20420, 1);

    FND_PROGRAM.REGISTER(
        program                => 'PO to AP Receipt Interface',
        application            => 'SQLAP',
        enabled                => 'Y',
        short_name             => 'XXCUST_PO_AP_IFACE',
        description            => 'Receipt-to-Invoice and RTV-to-Credit-Memo Interface',
        executable_short_name  => 'XXCUST_PO_AP_EXEC',
        executable_application => 'SQLAP',
        use_in_srs             => 'Y',
        output_type            => 'TEXT'
    );
    COMMIT;
END;
/
```

**Manual alternative:**

1. Navigate to **System Administrator → Concurrent → Program → Define**
2. Fill in:

| Field | Value |
|-------|-------|
| Program | PO to AP Receipt Interface |
| Short Name | `XXCUST_PO_AP_IFACE` |
| Application | Payables |
| Executable Name | `XXCUST_PO_AP_EXEC` |
| Output Format | Text |

---

## Step 4: Register Parameters

> **Notes on `FND_PROGRAM.PARAMETER`:**
> - `token` must be **NULL** for PL/SQL Stored Procedure programs (only used for Oracle Reports / Java programs).
> - `display_size`, `description_size`, `concatenated_description_size` are **mandatory** (no defaults).
> - Use value set `'YES_NO'` (not `'FND_YES_NO'` — it does not exist in this instance).
> - Use `'60 Characters'` for free-text parameters.

```sql
BEGIN
    FND_GLOBAL.APPS_INITIALIZE(0, 20420, 1);

    -- Param 1: Operating Unit
    FND_PROGRAM.PARAMETER(
        program_short_name            => 'XXCUST_PO_AP_IFACE',
        application                   => 'SQLAP',
        sequence                      => 10,
        parameter                     => 'Operating Unit',
        description                   => 'Operating Unit (NULL = all)',
        enabled                       => 'Y',
        value_set                     => '60 Characters',
        default_type                  => NULL,
        default_value                 => NULL,
        required                      => 'N',
        enable_security               => 'N',
        range                         => NULL,
        display                       => 'Y',
        display_size                  => 30,
        description_size              => 50,
        concatenated_description_size => 25,
        prompt                        => 'Operating Unit',
        token                         => NULL
    );

    -- Param 2: Date From
    FND_PROGRAM.PARAMETER(
        program_short_name            => 'XXCUST_PO_AP_IFACE',
        application                   => 'SQLAP',
        sequence                      => 20,
        parameter                     => 'Date From',
        description                   => 'Receipt/RTV date from (DD-MON-YYYY)',
        enabled                       => 'Y',
        value_set                     => 'FND_STANDARD_DATE',
        default_type                  => NULL,
        default_value                 => NULL,
        required                      => 'N',
        enable_security               => 'N',
        range                         => NULL,
        display                       => 'Y',
        display_size                  => 20,
        description_size              => 50,
        concatenated_description_size => 25,
        prompt                        => 'Date From',
        token                         => NULL
    );

    -- Param 3: Date To
    FND_PROGRAM.PARAMETER(
        program_short_name            => 'XXCUST_PO_AP_IFACE',
        application                   => 'SQLAP',
        sequence                      => 30,
        parameter                     => 'Date To',
        description                   => 'Receipt/RTV date to (DD-MON-YYYY)',
        enabled                       => 'Y',
        value_set                     => 'FND_STANDARD_DATE',
        default_type                  => NULL,
        default_value                 => NULL,
        required                      => 'N',
        enable_security               => 'N',
        range                         => NULL,
        display                       => 'Y',
        display_size                  => 20,
        description_size              => 50,
        concatenated_description_size => 25,
        prompt                        => 'Date To',
        token                         => NULL
    );

    -- Param 4: PO Number
    FND_PROGRAM.PARAMETER(
        program_short_name            => 'XXCUST_PO_AP_IFACE',
        application                   => 'SQLAP',
        sequence                      => 40,
        parameter                     => 'PO Number',
        description                   => 'Specific PO Number (NULL = all)',
        enabled                       => 'Y',
        value_set                     => '60 Characters',
        default_type                  => NULL,
        default_value                 => NULL,
        required                      => 'N',
        enable_security               => 'N',
        range                         => NULL,
        display                       => 'Y',
        display_size                  => 30,
        description_size              => 50,
        concatenated_description_size => 25,
        prompt                        => 'PO Number',
        token                         => NULL
    );

    -- Param 5: Debug Mode
    FND_PROGRAM.PARAMETER(
        program_short_name            => 'XXCUST_PO_AP_IFACE',
        application                   => 'SQLAP',
        sequence                      => 50,
        parameter                     => 'Debug Mode',
        description                   => 'Y or N - Enable verbose logging',
        enabled                       => 'Y',
        value_set                     => 'YES_NO',
        default_type                  => 'Constant',
        default_value                 => 'N',
        required                      => 'N',
        enable_security               => 'N',
        range                         => NULL,
        display                       => 'Y',
        display_size                  => 5,
        description_size              => 50,
        concatenated_description_size => 25,
        prompt                        => 'Debug Mode',
        token                         => NULL
    );

    COMMIT;
END;
/
```

**Parameter summary:**

| Seq | Parameter | Value Set | Required | Default |
|-----|-----------|-----------|----------|---------|
| 10 | Operating Unit | `60 Characters` | No | NULL (all OUs) |
| 20 | Date From | `FND_STANDARD_DATE` | No | NULL |
| 30 | Date To | `FND_STANDARD_DATE` | No | NULL |
| 40 | PO Number | `60 Characters` | No | NULL (all POs) |
| 50 | Debug Mode | `YES_NO` | No | N |

---

## Step 5: Assign to a Request Group

Users can only run concurrent programs that belong to a request group attached to their responsibility.

```sql
BEGIN
    FND_GLOBAL.APPS_INITIALIZE(0, 20420, 1);

    FND_PROGRAM.ADD_TO_GROUP(
        program_short_name  => 'XXCUST_PO_AP_IFACE',
        program_application => 'SQLAP',
        request_group       => 'All Reports',
        group_application   => 'SQLAP'
    );
    COMMIT;
END;
/
```

---

## Step 6: Running the Program

### Via the UI

1. Log in with a responsibility that has access to the request group (e.g., Payables Manager).
2. Navigate to **View → Requests → Submit a New Request**.
3. Select **PO to AP Receipt Interface**.
4. Enter parameters:
   - **Operating Unit**: leave blank for all, or enter a specific OU.
   - **Date From / Date To**: date range for receipts/RTVs. Leave blank for all unprocessed.
   - **PO Number**: leave blank for all POs, or enter a specific PO.
   - **Debug Mode**: `N` for normal, `Y` for verbose logging.
5. Click **Submit**.
6. Monitor in the Requests window. Check the **Log** tab for `FND_FILE` output.

### Via PL/SQL (ad-hoc or from another program)

```sql
DECLARE
    l_request_id NUMBER;
BEGIN
    FND_GLOBAL.APPS_INITIALIZE(
        user_id      => 1234,      -- your FND user ID
        resp_id      => 50555,     -- your responsibility ID
        resp_appl_id => 200        -- SQLAP application ID
    );

    l_request_id := FND_REQUEST.SUBMIT_REQUEST(
        application => 'SQLAP',
        program     => 'XXCUST_PO_AP_IFACE',
        description => 'Nightly PO-AP Interface Run',
        start_time  => NULL,       -- NULL = immediate; or 'YYYY/MM/DD HH24:MI:SS'
        sub_request => FALSE,
        argument1   => NULL,       -- p_operating_unit
        argument2   => NULL,       -- p_date_from
        argument3   => NULL,       -- p_date_to
        argument4   => NULL,       -- p_po_number
        argument5   => 'N'         -- p_debug_mode
    );

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Submitted Request ID: ' || l_request_id);
END;
/
```

### Scheduling (Nightly)

When submitting via UI or `FND_REQUEST.SUBMIT_REQUEST`, use the **Schedule** button / `start_time` parameter to set a recurring schedule (e.g., daily at 02:00 AM).

Alternatively, the existing Node.js scheduler + cron approach (`run_po_ap_interface.js` + `crontab`) can continue to be used for environments where the Concurrent Manager is not preferred.

---

## Return Codes

| Code | Meaning | Behavior |
|------|---------|----------|
| 0 | Success | Both Process A and B completed without issues |
| 1 | Warning | One or both processes had rejections/warnings (partial success) |
| 2 | Error | Fatal error — check the concurrent request log for details |

---

## Troubleshooting

| Symptom | Check |
|---------|-------|
| Program not visible in Submit Request | Verify it is added to the correct request group and the responsibility has access |
| `ORA-06550` compilation error | Ensure `XXCUST_PO_AP_CONC_PROG` procedure and `XXCUST_PO_AP_INTERFACE_PKG` are valid (`SELECT status FROM user_objects WHERE object_name = ...`) |
| Process A returns retcode 2 | Check the concurrent log — common causes: DB link down, missing supplier mappings, invalid COA mappings |
| No invoices created after run | Run reconciliation Query 4A from `PO_AP.sql` with the run_id from the log output |
| Credit memos not generated | Ensure Process A ran first for the same receipts — Process B only creates credit memos for RTVs that occurred after the original invoice was created |

---

## Related Files

| File | Purpose |
|------|---------|
| `PO_AP.sql` | Package spec/body + DDL + reconciliation queries |
| `script/run_po_ap_interface.js` | Node.js scheduler (alternative to Concurrent Manager) |
| `script/config.json` | Connection and runtime config for the Node.js scheduler |
| `script/crontab` | Cron schedule for the Node.js scheduler |
