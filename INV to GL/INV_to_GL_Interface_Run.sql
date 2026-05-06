
DECLARE
    l_errbuf    VARCHAR2(2000);
    l_retcode   NUMBER;
    l_group_id  NUMBER;
BEGIN
    XXCUST_INV_GL_INTERFACE_PKG.run_interface(
        p_errbuf           => l_errbuf,
        p_retcode          => l_retcode,
        p_group_id         => l_group_id,
        p_organization_id  => NULL,           -- Processes all orgs
        p_txn_date_from    => '01-DEC-2025',  -- Change this to a date where you have legacy data!
        p_txn_date_to      => '31-DEC-2025',  -- Change this to match your test data!
        p_txn_type_ids     => NULL,           -- Processes all transaction types
        p_debug_mode       => 'Y'             -- Turns on detailed logging
    );
    DBMS_OUTPUT.PUT_LINE('Return Code : ' || l_retcode);
    DBMS_OUTPUT.PUT_LINE('Message     : ' || l_errbuf);
    DBMS_OUTPUT.PUT_LINE('Group ID    : ' || l_group_id);
END;
/
