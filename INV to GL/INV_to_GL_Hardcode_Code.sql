INSERT INTO apps.gl_code_combinations (
   code_combination_id,
   chart_of_accounts_id,
   segment1, segment2, segment3, segment4,
   segment5, segment6, segment7, segment8,
   enabled_flag,
   summary_flag,
   account_type,
   detail_posting_allowed_flag,
   detail_budgeting_allowed_flag,
   last_update_date,
   last_updated_by
)
SELECT
   apps.gl_code_combinations_s.nextval,
   50428,
   '01','01','513001','00000',
   '00000000','1','000','000',
   'Y','N','E',
   'Y','Y',
   SYSDATE, -1
FROM dual;
COMMIT;
