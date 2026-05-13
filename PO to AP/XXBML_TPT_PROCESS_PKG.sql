create or replace PACKAGE BODY      XXBML_TPT_PROCESS_PKG IS
/*
-- ======================================================================================================
--
-- NAME  : XXBML_TPT_PROCESS_PKG.pkb
--
-- Description:
-- Package for Creating AP invoices for Transportation Route Types
--
-- MODIFICATION HISTORY
--
-- Date         Version  Author              Comments
-- -----------  -------  -----------------   -----------------------------------------------------------
-- 04-FEB-2022  1.0      Aditya Bhallam      Initial Creation
-- 05-MAY-2026  1.1      Sachin A S          Added logic to insert invoice lines in AP interface and also push the same to new EBS via DB link
*/

--declare global variables start
g_pkg                   varchar2(50) := 'XXBML_TPT_PROCESS_PKG';
g_conc_request_id       number       := fnd_global.conc_request_id;
g_from_date             date;
g_to_date               date;
g_reporting_month       varchar2(10);
g_reporting_period      varchar2(10);
g_route_type            varchar2(50);
g_route_shift           varchar2(50);
g_route_number          varchar2(50);
g_vendor_id             number;
g_generate_invoice      varchar2(5);
g_prorate_flag          varchar2(5):='N';      
--declare global variables end

procedure log(p_prc in varchar2,p_msg in varchar2)
is pragma autonomous_transaction;
begin
    dbms_output.put_line(g_pkg || '.' || p_prc || ':   ' || p_msg);
    FND_FILE.put_line(FND_FILE.log,g_pkg || '.' || p_prc || ':   ' || p_msg);

    insert into xxbml.xxbml_trp_process_log
           (id
           ,error_context
           ,error_message
           ,creation_date)
    values (xxbml.xxbml_trp_process_log_seq.nextval
           ,g_pkg || '.' || p_prc
           ,p_msg
           ,sysdate
           );
    commit;

exception
    when others then
       null;
end log;

procedure output(p_msg in varchar2)
is
begin
    fnd_file.put_line(fnd_file.output,p_msg);
exception
    when others then
       null;
end output;


PROCEDURE call_api(x_return_status       out varchar2
                  ,x_return_msg          out varchar2)
AS
l_prc                     varchar2(50) := 'call_api';
l_step                    varchar2(100);
l_return_msg              varchar2(4000);
l_skip                    exception;
l_inv_source              varchar2(50) := 'TRANSPORT'; --'MANUAL INVOICE ENTRY';

--
l_user_id                 number := 3493;   --BML_JOB_MGR
l_resp_id                 number := 50740;  --BAMUL Inventory Administration
l_resp_appl_id            number := 7000;   --Inventory
l_org_id                  number := 148;    --Bengaluru Operating Unit
l_request_id              number;
--
l_boolean                 boolean;
l_phase                   varchar2 (200);
l_status                  varchar2 (200);
l_dev_phase               varchar2 (200);
l_dev_status              varchar2 (200);
l_message                 varchar2 (200);

begin
x_return_status      := 'S';
l_step               := 'START: ';
log(l_prc,l_step || l_return_msg);


    mo_global.init ('SQLAP');
    --fnd_global.apps_initialize (user_id      => l_user_id
    --                           ,resp_id      => l_resp_id
    --                           ,resp_appl_id => l_resp_appl_id);
    --mo_global.set_policy_context ('S', l_org_id);

    l_request_id :=
      fnd_request.submit_request (application   => 'SQLAP'
                                 ,program       => 'APXIIMPT'
                                 ,description   => ''
                                 ,start_time    => NULL
                                 ,sub_request   => FALSE
                                 ,argument1     => l_org_id
                                 ,argument2     => l_inv_source --'MANUAL INVOICE ENTRY'
                                 ,argument3     => NULL
                                 ,argument4     => NULL
                                 ,argument5     => NULL
                                 ,argument6     => NULL
                                 ,argument7     => NULL
                                 ,argument8     => 'N'
                                 ,argument9     => 'Y');
    COMMIT;

    IF l_request_id > 0 THEN
       l_boolean   := fnd_concurrent.wait_for_request (l_request_id
                                                      ,20
                                                      ,0
                                                      ,l_phase
                                                      ,l_status
                                                      ,l_dev_phase
                                                      ,l_dev_status
                                                      ,l_message);
    END IF;

    log(l_prc,'call_api status = ' || l_status);


    IF (l_status = 'Normal') THEN
      log (l_prc,'Invoice Created Successfully, Please see the output of Payables OPEN Invoice Import program request id :' || l_request_id);
    ELSE
      l_return_msg := 'Payable Open Ivoice Pogram failed you can see the log from the application for the following reqiest id :' || l_request_id;
      RAISE l_skip;
    END IF;

  x_return_msg := 'Processed Successfully.';
  log(l_prc, ' END');
exception
    when l_skip then
       x_return_status := 'E';
       x_return_msg    := l_return_msg;
       log(l_prc, x_return_msg);
    when others then
       x_return_status := 'E';
       x_return_msg    := 'END ERROR: PROC:' || l_prc || ': ' || substr(sqlerrm,1,250);
       log(l_prc, x_return_msg);
END call_api;

PROCEDURE ap_invoices_interface_insert(p_request_id        in number
                                      ,x_return_status    out varchar2
                                      ,x_return_msg       out varchar2)
IS
l_prc                      varchar2(50) := 'ap_invoices_interface_insert';
l_step                     varchar2(100);
l_return_status            varchar2(1);
l_return_msg               varchar2(4000);
l_skip                     exception;
--
l_invoice_amount           number;
--l_inv_source               varchar2(50) := 'MANUAL INVOICE ENTRY'; --'TRANSPORT_BILLING';
l_inv_source               varchar2(50) :='TRANSPORT'; --Added by Subodh on 29-NOV-2022 
l_invoice_id               number;
l_line_num                 number := 0;
l_rowcount                 number := 0;
--
-- New EBS interface variables (Process C)
l_new_ebs_invoice_id       number;
l_new_ebs_ret_status       varchar2(1);
l_new_ebs_ret_msg          varchar2(4000);
l_vendor_number            varchar2(30);
l_vendor_site_code         varchar2(30);
l_legacy_seg2              varchar2(25);
l_legacy_seg4              varchar2(25);

BEGIN
x_return_status  := 'S';
l_step           := 'START: ';
log(l_prc,l_step);

  --
  begin
    l_step       := 'B4_INV_HDR_REC_LOOP: ';
    l_invoice_id := null;

      IF   g_route_type in ('TCD','PTC') THEN  --PTC added on 12-JUN-2024 by Subodh
         for inv_hdr_rec in (select (select vendor_id
                                  from ap_suppliers ap
                                 where upper(ap.vendor_name) = upper(tr.transporter_name)
                                   and rownum=1)  vendor_id
                              ,sum(payment_amount) payment_amount
                              ,(select vendor_site_id
                                  from ap_suppliers          ap
                                      ,ap_supplier_sites_all aps
                                 where ap.vendor_id    = aps.vendor_id
                                   and upper(ap.vendor_name) = upper(tr.transporter_name)
                                   and rownum=1
                                   and aps.inactive_date is null --Added by Subodh on 04-JAN-2023
                                   and aps.PAY_GROUP_LOOKUP_CODE like g_route_type||'%' --Added by Subodh on 03-APR-2024 as Same supplier can be attached to different route types
                                   ) vendor_site_id
                              ,'INR'           invoice_currency_code
                              ,10003           terms_id
                              ,'Transport Billing created automatically' description
                              ,l_inv_source    inv_source
                              ,148             org_id
                              ,'NEFT'          payment_method_code
                              ,'STANDARD'      invoice_type_lookup_code
                              ,tr.item_desc
                              ,tr.item_id
                              ,tr.dist_code_combination_id
                              --,tr.route_number
                              ,tr.parent_route_number
                             -- ,tr.route_shift
                              ,tr.transporter_name --Added by Subodh on 06-JAN-2023
                          from (select zt.*
                                      ,(select mtl.description
                                          from mtl_system_items_b mtl
                                              ,po_lines_all       pol
                                         where pol.po_header_id      = zt.ebs_bpa_po_hdr_id
                                           and mtl.inventory_item_id = pol.item_id
                                           and rownum=1) item_desc
                                      ,(select mtl.inventory_item_id
                                          from mtl_system_items_b mtl
                                              ,po_lines_all       pol
                                         where pol.po_header_id      = zt.ebs_bpa_po_hdr_id
                                           and mtl.inventory_item_id = pol.item_id
                                           and rownum=1) item_id
                                      ,(select mtl.expense_account
                                          from mtl_system_items_b mtl
                                              ,po_lines_all       pol
                                         where pol.po_header_id      = zt.ebs_bpa_po_hdr_id
                                           and mtl.inventory_item_id = pol.item_id
                                           and rownum=1) dist_code_combination_id
                                  from tr_trip_hdr zt) tr
                         where tr.calc_request_id         = p_request_id
                           and tr.calc_status             = 'CALCULATED'
                           and tr.ebs_ap_invoice_id       = -9
                           --and tr.trip_trx_id in (251787,252714)  --remove after testing
                           and tr.payment_amount is not null
                         group by transporter_name,item_desc,item_id,dist_code_combination_id,parent_route_number
                        -- ,route_shift
                         )
                   --  END IF;
    loop
      l_step       := 'IN_INV_HDR_REC_LOOP: ';
      l_rowcount       := l_rowcount + 1;
      l_invoice_amount := 0;
      l_line_num       := 0;
      l_invoice_id     := ap_invoices_interface_s.nextval;
      begin
        --insert invoice headers start
        INSERT INTO ap_invoices_interface
              (invoice_id
              ,invoice_num
              ,vendor_id
              ,vendor_site_id
              ,invoice_amount
              ,invoice_currency_code
              ,invoice_date
              ,terms_date
              ,terms_id
              ,description
              ,SOURCE
              ,org_id
              ,payment_method_code
              ,invoice_type_lookup_code
              ,attribute2)
       VALUES (l_invoice_id                              --invoice_id
              --,'TPT-' || l_invoice_id                     --invoice_num
              ,g_route_type||'-'|| l_invoice_id                     --invoice_num --Added by Subodh on 29-NOV-2022 as Per Leela's advice
              ,inv_hdr_rec.vendor_id                     --vendor_id
              ,inv_hdr_rec.vendor_site_id                --vendor_site_id
              ,inv_hdr_rec.payment_amount                --invoice_amount
              ,inv_hdr_rec.invoice_currency_code         --invoice_currency_code
              ,sysdate                                   --invoice_date
              ,sysdate                                   --terms_date
              ,inv_hdr_rec.terms_id                      --terms_id
              ,inv_hdr_rec.description                   --description
              ,inv_hdr_rec.inv_source                    --source
              ,inv_hdr_rec.org_id                        --org_id
              ,inv_hdr_rec.payment_method_code           --payment_method_code
              ,inv_hdr_rec.invoice_type_lookup_code      --invoice_type_lookup_code
              ,g_reporting_month || ' ' || g_reporting_period
              );
        --insert invoice headers end

        -- *** NEW EBS: Push invoice header via DB link (Process C) ***
        begin
            -- Get vendor_number (segment1) for mapping in new EBS
            select ap.segment1 into l_vendor_number
              from ap_suppliers ap where ap.vendor_id = inv_hdr_rec.vendor_id and rownum=1;
            -- Get vendor_site_code for mapping in new EBS
            begin
                select aps.vendor_site_code into l_vendor_site_code
                  from ap_supplier_sites_all aps where aps.vendor_site_id = inv_hdr_rec.vendor_site_id and rownum=1;
            exception when others then l_vendor_site_code := null;
            end;

            xxcust_po_ap_interface_pkg.insert_transport_invoice@NEW_EBS_LINK(
                p_invoice_id                => l_invoice_id
               ,p_invoice_num               => g_route_type||'-'||l_invoice_id
               ,p_vendor_id                 => inv_hdr_rec.vendor_id
               ,p_vendor_site_id            => inv_hdr_rec.vendor_site_id
               ,p_invoice_amount            => inv_hdr_rec.payment_amount
               ,p_invoice_currency_code     => inv_hdr_rec.invoice_currency_code
               ,p_invoice_date              => sysdate
               ,p_terms_date                => sysdate
               ,p_terms_id                  => inv_hdr_rec.terms_id
               ,p_description               => inv_hdr_rec.description
               ,p_source                    => inv_hdr_rec.inv_source
               ,p_org_id                    => inv_hdr_rec.org_id
               ,p_payment_method_code       => inv_hdr_rec.payment_method_code
               ,p_invoice_type_lookup_code  => inv_hdr_rec.invoice_type_lookup_code
               ,p_attribute2                => g_reporting_month || ' ' || g_reporting_period
               ,p_vendor_number             => l_vendor_number
               ,p_vendor_site_code          => l_vendor_site_code
               ,x_new_invoice_id            => l_new_ebs_invoice_id
               ,x_return_status             => l_new_ebs_ret_status
               ,x_return_msg                => l_new_ebs_ret_msg
            );
            if nvl(l_new_ebs_ret_status,'E') = 'E' then
               log(l_prc,'NEW EBS header insert failed: ' || l_new_ebs_ret_msg);
            else
               log(l_prc,'NEW EBS header insert OK: new_invoice_id=' || l_new_ebs_invoice_id);
            end if;
        exception when others then
            log(l_prc,'NEW EBS header call error: ' || substr(sqlerrm,1,250));
        end;
        -- *** END NEW EBS header ***

        --insert invoice lines start
        l_line_num := 0;
        l_step     := 'B4_INV_LINES_REC_LOOP: ';
        for inv_lines_rec in (select 1 z from dual
                              /*
                              select sum(tr.payment_amount) total_payment_amount
                                    ,'MISCELLANEOUS'     line_type_lookup_code
                                    ,'IMPORTED' line_source
                                from tr_trip_hdr tr
                               where tr.calc_request_id         = p_request_id
                                 and tr.calc_status             = 'CALCULATED'
                                 and tr.ebs_ap_invoice_id       = -9
                                 and tr.trip_trx_id in (251787,252714)  --remove after testing
                                 and tr.transporter_id          = inv_hdr_rec.vendor_id
                                 and tr.payment_amount is not null
                              */
                              )
        loop
            l_line_num       := l_line_num + 1;
            --l_invoice_amount := l_invoice_amount + inv_hdr_rec.payment_amount;
            insert into ap_invoice_lines_interface
                  (invoice_id
                  ,line_number
                  ,line_type_lookup_code
                  ,amount
                  ,description
                  ,inventory_item_id
                  ,dist_code_combination_id
                  ,reference_1
                  ,reference_2)
          values  (l_invoice_id
                  ,l_line_num
                  ,'ITEM' --inv_lines_rec.line_type_lookup_code
                  ,inv_hdr_rec.payment_amount
                  ,inv_hdr_rec.item_desc
                  ,inv_hdr_rec.item_id
                  ,inv_hdr_rec.dist_code_combination_id
                  ,inv_hdr_rec.parent_route_number 
                  --|| ' | ' || inv_hdr_rec.route_shift
                  ,to_char(g_from_date,'DD-MON-RRRR') || ' to ' || to_char(g_to_date,'DD-MON-RRRR')
                  );

            -- *** NEW EBS: Push invoice line via DB link (Process C) ***
            if l_new_ebs_invoice_id is not null then
            begin
                -- Get legacy segments locally to avoid DB link loop
                begin
                    select gcc.segment2, gcc.segment4
                    into   l_legacy_seg2, l_legacy_seg4
                    from   apps.gl_code_combinations gcc
                    where  gcc.code_combination_id = inv_hdr_rec.dist_code_combination_id;
                exception when others then
                    l_legacy_seg2 := null;
                    l_legacy_seg4 := null;
                end;
                xxcust_po_ap_interface_pkg.insert_transport_inv_line@NEW_EBS_LINK(
                    p_new_invoice_id            => l_new_ebs_invoice_id
                   ,p_line_number               => l_line_num
                   ,p_line_type_lookup_code     => 'ITEM'
                   ,p_amount                    => inv_hdr_rec.payment_amount
                   ,p_description               => inv_hdr_rec.item_desc
                   ,p_inventory_item_id         => inv_hdr_rec.item_id
                   ,p_dist_code_combination_id  => inv_hdr_rec.dist_code_combination_id
                   ,p_reference_1               => inv_hdr_rec.parent_route_number
                   ,p_reference_2               => to_char(g_from_date,'DD-MON-RRRR') || ' to ' || to_char(g_to_date,'DD-MON-RRRR')
                   ,p_legacy_segment2           => l_legacy_seg2
                   ,p_legacy_segment4           => l_legacy_seg4
                   ,x_return_status             => l_new_ebs_ret_status
                   ,x_return_msg                => l_new_ebs_ret_msg
                );
                if nvl(l_new_ebs_ret_status,'E') = 'E' then
                   log(l_prc,'NEW EBS line insert failed: ' || l_new_ebs_ret_msg);
                end if;
            exception when others then
                log(l_prc,'NEW EBS line call error: ' || substr(sqlerrm,1,250));
            end;
            end if;
            -- *** END NEW EBS line ***

        end loop;  --for inv_lines_rec
        --insert invoice lines end

      exception when others then
        rollback;
        l_return_msg    := 'Error at ' || l_step || substr(sqlerrm,1,250);
        log(l_prc,l_return_msg);
      end;

      update tr_trip_hdr tr
         set ebs_ap_invoice_id  = l_invoice_id
            --,ebs_ap_invoice_num = 'DM-' || l_invoice_id
            ,ebs_ap_invoice_num = g_route_type||'-'|| l_invoice_id --Added by Subodh on 29-NOV-2022 as Per Leela's advice
            ,ap_invoice_reporting_period = g_reporting_month || ' ' || g_reporting_period
       where tr.calc_request_id          = p_request_id
         and tr.calc_status              = 'CALCULATED'
         and tr.ebs_ap_invoice_id        = -9
         and tr.transporter_name =inv_hdr_rec.transporter_name --Added by Subodh on 06-JAN-2023
         and tr.parent_route_number= inv_hdr_rec.parent_route_number --Added by Subodh on 14-MAR-2024 
         --and tr.trip_trx_id in (251787,252714)  --remove after testing
         and tr.payment_amount is not null;

    end loop;  --for inv_hdr_rec
    
    ELSE 
    
     for inv_hdr_rec in (select (select vendor_id
                                  from ap_suppliers ap
                                 where upper(ap.vendor_name) = upper(tr.transporter_name)
                                   and rownum=1)  vendor_id
                              ,sum(payment_amount) payment_amount
                              ,(select vendor_site_id
                                  from ap_suppliers          ap
                                      ,ap_supplier_sites_all aps
                                 where ap.vendor_id    = aps.vendor_id
                                   and upper(ap.vendor_name) = upper(tr.transporter_name)
                                   and rownum=1
                                   and aps.inactive_date is null --Added by Subodh on 04-JAN-2023
                                   and aps.PAY_GROUP_LOOKUP_CODE like g_route_type||'%' --Added by Subodh on 03-APR-2024 as Same supplier can be attached to different route types
                                   ) vendor_site_id
                              ,'INR'           invoice_currency_code
                              ,10003           terms_id
                              ,'Transport Billing created automatically' description
                              ,l_inv_source    inv_source
                              ,148             org_id
                              ,'NEFT'          payment_method_code
                              ,'STANDARD'      invoice_type_lookup_code
                              ,tr.item_desc
                              ,tr.item_id
                              ,tr.dist_code_combination_id
                              ,tr.route_number
                              ,tr.route_shift
                              ,tr.transporter_name --Added by Subodh on 06-JAN-2023
                          from (select zt.*
                                      ,(select mtl.description
                                          from mtl_system_items_b mtl
                                              ,po_lines_all       pol
                                         where pol.po_header_id      = zt.ebs_bpa_po_hdr_id
                                           and mtl.inventory_item_id = pol.item_id
                                           and rownum=1) item_desc
                                      ,(select mtl.inventory_item_id
                                          from mtl_system_items_b mtl
                                              ,po_lines_all       pol
                                         where pol.po_header_id      = zt.ebs_bpa_po_hdr_id
                                           and mtl.inventory_item_id = pol.item_id
                                           and rownum=1) item_id
                                      ,(select mtl.expense_account
                                          from mtl_system_items_b mtl
                                              ,po_lines_all       pol
                                         where pol.po_header_id      = zt.ebs_bpa_po_hdr_id
                                           and mtl.inventory_item_id = pol.item_id
                                           and rownum=1) dist_code_combination_id
                                  from tr_trip_hdr zt) tr
                         where tr.calc_request_id         = p_request_id
                           and tr.calc_status             = 'CALCULATED'
                           and tr.ebs_ap_invoice_id       = -9
                           --and tr.trip_trx_id in (251787,252714)  --remove after testing
                           and tr.payment_amount is not null
                         group by transporter_name,item_desc,item_id,dist_code_combination_id,route_number,route_shift)
                   --  END IF;
    loop
      l_step       := 'IN_INV_HDR_REC_LOOP: ';
      l_rowcount       := l_rowcount + 1;
      l_invoice_amount := 0;
      l_line_num       := 0;
      l_invoice_id     := ap_invoices_interface_s.nextval;
      begin
        --insert invoice headers start
        INSERT INTO ap_invoices_interface
              (invoice_id
              ,invoice_num
              ,vendor_id
              ,vendor_site_id
              ,invoice_amount
              ,invoice_currency_code
              ,invoice_date
              ,terms_date
              ,terms_id
              ,description
              ,SOURCE
              ,org_id
              ,payment_method_code
              ,invoice_type_lookup_code
              ,attribute2)
       VALUES (l_invoice_id                              --invoice_id
              --,'TPT-' || l_invoice_id                     --invoice_num
              ,g_route_type||'-'|| l_invoice_id                     --invoice_num --Added by Subodh on 29-NOV-2022 as Per Leela's advice
              ,inv_hdr_rec.vendor_id                     --vendor_id
              ,inv_hdr_rec.vendor_site_id                --vendor_site_id
              ,inv_hdr_rec.payment_amount                --invoice_amount
              ,inv_hdr_rec.invoice_currency_code         --invoice_currency_code
              ,sysdate                                   --invoice_date
              ,sysdate                                   --terms_date
              ,inv_hdr_rec.terms_id                      --terms_id
              ,inv_hdr_rec.description                   --description
              ,inv_hdr_rec.inv_source                    --source
              ,inv_hdr_rec.org_id                        --org_id
              ,inv_hdr_rec.payment_method_code           --payment_method_code
              ,inv_hdr_rec.invoice_type_lookup_code      --invoice_type_lookup_code
              ,g_reporting_month || ' ' || g_reporting_period
              );
        --insert invoice headers end

        -- *** NEW EBS: Push invoice header via DB link (Process C) ***
        begin
            -- Get vendor_number (segment1) for mapping in new EBS
            select ap.segment1 into l_vendor_number
              from ap_suppliers ap where ap.vendor_id = inv_hdr_rec.vendor_id and rownum=1;
            -- Get vendor_site_code for mapping in new EBS
            begin
                select aps.vendor_site_code into l_vendor_site_code
                  from ap_supplier_sites_all aps where aps.vendor_site_id = inv_hdr_rec.vendor_site_id and rownum=1;
            exception when others then l_vendor_site_code := null;
            end;

            xxcust_po_ap_interface_pkg.insert_transport_invoice@NEW_EBS_LINK(
                p_invoice_id                => l_invoice_id
               ,p_invoice_num               => g_route_type||'-'||l_invoice_id
               ,p_vendor_id                 => inv_hdr_rec.vendor_id
               ,p_vendor_site_id            => inv_hdr_rec.vendor_site_id
               ,p_invoice_amount            => inv_hdr_rec.payment_amount
               ,p_invoice_currency_code     => inv_hdr_rec.invoice_currency_code
               ,p_invoice_date              => sysdate
               ,p_terms_date                => sysdate
               ,p_terms_id                  => inv_hdr_rec.terms_id
               ,p_description               => inv_hdr_rec.description
               ,p_source                    => inv_hdr_rec.inv_source
               ,p_org_id                    => inv_hdr_rec.org_id
               ,p_payment_method_code       => inv_hdr_rec.payment_method_code
               ,p_invoice_type_lookup_code  => inv_hdr_rec.invoice_type_lookup_code
               ,p_attribute2                => g_reporting_month || ' ' || g_reporting_period
               ,p_vendor_number             => l_vendor_number
               ,p_vendor_site_code          => l_vendor_site_code
               ,x_new_invoice_id            => l_new_ebs_invoice_id
               ,x_return_status             => l_new_ebs_ret_status
               ,x_return_msg                => l_new_ebs_ret_msg
            );
            if nvl(l_new_ebs_ret_status,'E') = 'E' then
               log(l_prc,'NEW EBS header insert failed: ' || l_new_ebs_ret_msg);
            else
               log(l_prc,'NEW EBS header insert OK: new_invoice_id=' || l_new_ebs_invoice_id);
            end if;
        exception when others then
            log(l_prc,'NEW EBS header call error: ' || substr(sqlerrm,1,250));
        end;
        -- *** END NEW EBS header ***

        --insert invoice lines start
        l_line_num := 0;
        l_step     := 'B4_INV_LINES_REC_LOOP: ';
        for inv_lines_rec in (select 1 z from dual
                              /*
                              select sum(tr.payment_amount) total_payment_amount
                                    ,'MISCELLANEOUS'     line_type_lookup_code
                                    ,'IMPORTED' line_source
                                from tr_trip_hdr tr
                               where tr.calc_request_id         = p_request_id
                                 and tr.calc_status             = 'CALCULATED'
                                 and tr.ebs_ap_invoice_id       = -9
                                 and tr.trip_trx_id in (251787,252714)  --remove after testing
                                 and tr.transporter_id          = inv_hdr_rec.vendor_id
                                 and tr.payment_amount is not null
                              */
                              )
        loop
            l_line_num       := l_line_num + 1;
            --l_invoice_amount := l_invoice_amount + inv_hdr_rec.payment_amount;
            insert into ap_invoice_lines_interface
                  (invoice_id
                  ,line_number
                  ,line_type_lookup_code
                  ,amount
                  ,description
                  ,inventory_item_id
                  ,dist_code_combination_id
                  ,reference_1
                  ,reference_2)
          values  (l_invoice_id
                  ,l_line_num
                  ,'ITEM' --inv_lines_rec.line_type_lookup_code
                  ,inv_hdr_rec.payment_amount
                  ,inv_hdr_rec.item_desc
                  ,inv_hdr_rec.item_id
                  ,inv_hdr_rec.dist_code_combination_id
                  ,inv_hdr_rec.route_number || ' | ' || inv_hdr_rec.route_shift
                  ,to_char(g_from_date,'DD-MON-RRRR') || ' to ' || to_char(g_to_date,'DD-MON-RRRR')
                  );

            -- *** NEW EBS: Push invoice line via DB link (Process C) ***
            if l_new_ebs_invoice_id is not null then
            begin
                -- Get legacy segments locally to avoid DB link loop
                begin
                    select gcc.segment2, gcc.segment4
                    into   l_legacy_seg2, l_legacy_seg4
                    from   apps.gl_code_combinations gcc
                    where  gcc.code_combination_id = inv_hdr_rec.dist_code_combination_id;
                exception when others then
                    l_legacy_seg2 := null;
                    l_legacy_seg4 := null;
                end;
                xxcust_po_ap_interface_pkg.insert_transport_inv_line@NEW_EBS_LINK(
                    p_new_invoice_id            => l_new_ebs_invoice_id
                   ,p_line_number               => l_line_num
                   ,p_line_type_lookup_code     => 'ITEM'
                   ,p_amount                    => inv_hdr_rec.payment_amount
                   ,p_description               => inv_hdr_rec.item_desc
                   ,p_inventory_item_id         => inv_hdr_rec.item_id
                   ,p_dist_code_combination_id  => inv_hdr_rec.dist_code_combination_id
                   ,p_reference_1               => inv_hdr_rec.route_number || ' | ' || inv_hdr_rec.route_shift
                   ,p_reference_2               => to_char(g_from_date,'DD-MON-RRRR') || ' to ' || to_char(g_to_date,'DD-MON-RRRR')
                   ,p_legacy_segment2           => l_legacy_seg2
                   ,p_legacy_segment4           => l_legacy_seg4
                   ,x_return_status             => l_new_ebs_ret_status
                   ,x_return_msg                => l_new_ebs_ret_msg
                );
                if nvl(l_new_ebs_ret_status,'E') = 'E' then
                   log(l_prc,'NEW EBS line insert failed: ' || l_new_ebs_ret_msg);
                end if;
            exception when others then
                log(l_prc,'NEW EBS line call error: ' || substr(sqlerrm,1,250));
            end;
            end if;
            -- *** END NEW EBS line ***

        end loop;  --for inv_lines_rec
        --insert invoice lines end

      exception when others then
        rollback;
        l_return_msg    := 'Error at ' || l_step || substr(sqlerrm,1,250);
        log(l_prc,l_return_msg);
      end;

      update tr_trip_hdr tr
         set ebs_ap_invoice_id  = l_invoice_id
            --,ebs_ap_invoice_num = 'DM-' || l_invoice_id
            ,ebs_ap_invoice_num = g_route_type||'-'|| l_invoice_id --Added by Subodh on 29-NOV-2022 as Per Leela's advice
            ,ap_invoice_reporting_period = g_reporting_month || ' ' || g_reporting_period
       where tr.calc_request_id          = p_request_id
         and tr.calc_status              = 'CALCULATED'
         and tr.ebs_ap_invoice_id        = -9
         and tr.transporter_name =inv_hdr_rec.transporter_name --Added by Subodh on 06-JAN-2023
         and tr.route_number= inv_hdr_rec.route_number --Added by Subodh on 14-MAR-2024 
         --and tr.trip_trx_id in (251787,252714)  --remove after testing
         and tr.payment_amount is not null;

    end loop;  --for inv_hdr_rec
    END IF;  --g_route_type TCD
    
    
    if l_rowcount = 0 then
       l_return_msg := 'No valid data to create invoice.';
       raise l_skip;
    end if;
    
    -- Call run_ap_import on new EBS to create invoices from interface tables
    -- Failure here should not fail the overall procedure
    begin
        xxcust_po_ap_interface_pkg.run_ap_import@NEW_EBS_LINK(
            p_source        => 'TRANSPORT'
           ,x_request_id    => l_new_ebs_invoice_id
           ,x_return_status => l_new_ebs_ret_status
           ,x_return_msg    => l_new_ebs_ret_msg
        );
        if nvl(l_new_ebs_ret_status,'E') = 'E' then
           log(l_prc,'NEW EBS run_ap_import failed: ' || l_new_ebs_ret_msg);
        else
           log(l_prc,'NEW EBS run_ap_import OK: request_id=' || l_new_ebs_invoice_id);
        end if;
    exception when others then
        log(l_prc,'NEW EBS run_ap_import error: ' || substr(sqlerrm,1,250));
    end;

    call_api(l_return_status,l_return_msg);
    if nvl(l_return_status,'E') = 'E' then
       raise l_skip;
    end if;

      update tr_trip_hdr tr
         set calc_status = 'PROCESSED'
       where tr.calc_request_id         = p_request_id
         --and tr.trip_trx_id in (251787,252714)  --remove after testing
         and tr.payment_amount is not null;

  exception
    when l_skip then
      raise l_skip;
    when others then
      rollback;
      l_return_msg    := 'Error at ' || l_step || substr(sqlerrm,1,250);
      raise l_skip;
  end;
  --

log(l_prc, ' END');
exception
    when l_skip then
       x_return_status := 'E';
       x_return_msg    := ' END ERROR L_SKIP: ' || l_return_msg;
       log(l_prc,x_return_msg);
    when others then
       x_return_status := 'E';
       x_return_msg    := 'END ERROR: ' || l_step || substr(sqlerrm,1,250);
       log(l_prc, x_return_msg);
end ap_invoices_interface_insert;


procedure write_output
is
l_prc                 varchar2(100) := 'write_output';
l_step                varchar2(100);
l_route_type          varchar2(100);
l_data                varchar2(32767);
l_total_amount        number := 0;
begin
l_step := 'START: ';
  log(l_prc,l_step );

  begin
    select text_value into l_route_type from xxbml_tpt_route_types_v where text_key = g_route_type;
  exception when others then
    l_route_type := '';
  end;

  l_data := '<TABLE border=1 style="border-collapse:collapse; font-family:verdana; font-size:90%;">'||
              '<TR>'  ||
                '<TH colspan=22 align="center" BGCOLOR="#E6E6E6"><B> BAMUL TRANSPORT BILLING PROGRAM REPORT </B></TH>'||
              '</TR>' ||
              '<TR>'  ||
                '<TH colspan=22 align="center" BGCOLOR="#E6E6E6"><B> BENGALURU URBAN, BENGALURU RURAL & RAMANAGAR DISTRICT CO-OP MILK PRODUCERS SOCIETIES UNION LTD.,    </B></TH>'||
              '</TR>' ||
              '<TR>'  ||
                '<TH colspan=5  align="left"   BGCOLOR="#E6E6E6"><B> From Date        : '   || to_char(g_from_date,'DD-MON-RR')  || '<br>'
                                                                || ' To Date          : '   || to_char(g_to_date,'DD-MON-RR')    || '<br>'
                                                                || ' Generate Invoice : '   || case when g_generate_invoice = 'Y' then 'Yes' else 'No' end
                      || '    </B></TH>'||
                '<TH colspan=14 align="left"   BGCOLOR="#E6E6E6"><B> Route Type   : ' || l_route_type || '<br>'
                                                                || ' Route Number : ' || g_route_number
                      || '    </B></TH>'||
                '<TH colspan=1  align="right"   BGCOLOR="#E6E6E6"><B> Request ID: '   || g_conc_request_id
                      || '    </B></TH>'||
              '</TR>' ||
              '<TR BGCOLOR="#E6E6E6">'||
                '<TD  align="left"   ><B>Date                       </B></TD>' ||
                '<TD  align="left"   ><B>Route                      </B></TD>' ||
                '<TD  align="left"   ><B>Transporter ID             </B></TD>' || --Added by Subodh on 01-JUN-2022 as requested by Leela
                '<TD  align="left"   ><B>Transporter Name           </B></TD>' || --Added by Subodh on 01-JUN-2022 as requested by Leela
                '<TD  align="center" ><B>Shift                      </B></TD>' ||
                '<TD r align="right"  ><B>BPA                        </B></TD>' ||
                  '<TD  align="right"  ><B>BPA Start Date               </B></TD>' || --Added by Subodh on 08-FEB-2024
                '<TD  align="right"  ><B>BPA End Date               </B></TD>' || --Added by Subodh on 01-JUN-2022 as requested by Leela
                '<TD  align="right"  ><B>VEHICLE MILEAGE            </B></TD>' ||
                  '<TD  align="right"  ><B>DIESEL PRICE (TRIP DATE)   </B></TD>' ||
                '<TD  align="right"  ><B>DIESEL PRICE (BPA START DATE) </B></TD>' ||
                '<TD  align="left"   ><B>PAYMENT TYPE               </B></TD>' ||
                 '<TD align="right"  ><B>TOTAL KMS                  </B></TD>' ||
                '<TD  align="right"  ><B>RATE/KM                       </B></TD>' ||
                '<TD  align="right"  ><B>EXTRA RATE                 </B></TD>'  ||
                '<TD  align="right"  ><B>PAYABLE FUEL PRICE         </B></TD>' ||
                  '<TD  align="right"  ><B>CALCULATED RATE/KM         </B></TD>' ||
                
                '<TD  align="right"  ><B>AMOUNT                     </B></TD>' ||
                '<TD  align="left"   ><B>STATUS                     </B></TD>' ||
                '<TD  align="left"   ><B>ERROR                      </B></TD>' ||
                '<TD  align="left"   ><B>EBS INVOICE NUMBER         </B></TD>' ||
                
              '</TR>' ;
  output(l_data);
l_total_amount :=0;
  for l_rec in (select schedule_date
                      ,tth.route_number
                      ,tth.route_shift
                      ,tth.ebs_bpa_po_number
                      ,nvl(calc_pay_type,
                                        (select unit_meas_lookup_code from po_lines_all pla
                                          where tth.route_number      = pla.vendor_product_num
                                            and tth.EBS_BPA_PO_HDR_ID = pla.po_header_id
                                             and trunc(tth.schedule_date) between trunc(to_date(pla.attribute1,'YYYY/MM/DD HH24:MI:SS')) and trunc(to_date(pla.attribute2,'YYYY/MM/DD HH24:MI:SS')) --Added by Subodh on 06-MAY-2024
                                            )) calc_pay_type
                      ,nvl(calc_rate_per_pay_type,
                                                 (select pla.unit_price from po_lines_all pla
                                                   where tth.route_number      = pla.vendor_product_num
                                                     and tth.EBS_BPA_PO_HDR_ID = pla.po_header_id
                                                      and trunc(tth.schedule_date) between trunc(to_date(pla.attribute1,'YYYY/MM/DD HH24:MI:SS')) and trunc(to_date(pla.attribute2,'YYYY/MM/DD HH24:MI:SS')) --Added by Subodh on 06-MAY-2024
                                                     )) calc_rate_per_pay_type
                      ,calc_extra_rate_per_pay_type
                      ,calc_payable_fuel_price
                      ,payment_amount
                      ,calc_status
                      ,calc_error_msg
                      ,TRANSPORTER_NAME --Added by Subodh on 01-JUN-2022 as requested by Leela
                      ,(select max(segment1) from ap_suppliers asp
                         where 1=1
                         --and tth.TRANSPORTER_ID =asp.VENDOR_ID
                         and upper(tth.TRANSPORTER_NAME) = upper(asp.vendor_name)) TRANSPORTER_ID --Added by Subodh on 01-JUN-2022 as requested by Leela
                     ,(select MAX(fnd_date.canonical_to_date(attribute2)) from po_lines_all pla
                        where tth.route_number      = pla.vendor_product_num
                          and tth.EBS_BPA_PO_HDR_ID = pla.po_header_id
                           and trunc(tth.schedule_date) between trunc(to_date(pla.attribute1,'YYYY/MM/DD HH24:MI:SS')) and trunc(to_date(pla.attribute2,'YYYY/MM/DD HH24:MI:SS')) --Added by Subodh on 06-MAY-2024
                      ) bpa_end_date
                       ,(select MAX(fnd_date.canonical_to_date(attribute1)) from po_lines_all pla
                        where tth.route_number      = pla.vendor_product_num
                          and tth.EBS_BPA_PO_HDR_ID = pla.po_header_id
                           and trunc(tth.schedule_date) between trunc(to_date(pla.attribute1,'YYYY/MM/DD HH24:MI:SS')) and trunc(to_date(pla.attribute2,'YYYY/MM/DD HH24:MI:SS')) --Added by Subodh on 06-MAY-2024
                      ) bpa_start_date
                      ,nvl(tv.mileage,tth.calc_veh_mileage) vehicle_mileage
                       ,(select value
                         from TR_FUEL_RATE_CHART z
                        where z.fuel_type = nvl(tv.fuel_type,tth.calc_veh_fuel_type) --'Diesel'
                          and trunc(z.effective_date) = trunc(tth.schedule_date)
                          and rownum = 1) trip_fuel_rate
                          ,(select value
                         from TR_FUEL_RATE_CHART z,po_lines_all pol
                        where z.fuel_type = nvl(tv.fuel_type,tth.calc_veh_fuel_type) --'Diesel'
                          and trunc(z.effective_date) = trunc(to_date(pol.attribute1,'YYYY/MM/DD HH24:MI:SS'))
                          and  trunc(tth.schedule_date)   between trunc(to_date(pol.attribute1,'YYYY/MM/DD HH24:MI:SS')) and trunc(to_date(pol.attribute2,'YYYY/MM/DD HH24:MI:SS'))
                          and tth.EBS_BPA_PO_HDR_ID = pol.po_header_id
                          and tth.route_number      = pol.vendor_product_num
                          and rownum = 1) po_startdate_fuel_rate					
                     ,tth.rt_dist_in_km                 trip_kilometers
                     ,tth.ebs_ap_invoice_num
                     ,tth.calc_rate_km
                       --,null bpa_end_date --Added by Subodh on 01-JUN-2022 as requested by Leela
                  from bmlcustm2.tr_trip_hdr tth
                   ,bmlcustm2.tr_vehicle  tv
                 where calc_request_id     = g_conc_request_id
                  and tth.vehicle_id      = tv.vehicle_id(+)
                 order by schedule_date , tth.route_shift desc)
  loop
           l_data   := '<TR>' ||
                         '<TD align="left"  >' || l_rec.schedule_date                || '</TD>' ||
                         '<TD align="left"  >' || l_rec.route_number                 || '</TD>' ||
                         '<TD align="left"  >' || l_rec.TRANSPORTER_ID               || '</TD>' || --Added by Subodh on 01-JUN-2022 as requested by Leela
                         '<TD align="left"  >' || l_rec.TRANSPORTER_NAME             || '</TD>' || --Added by Subodh on 01-JUN-2022 as requested by Leela
                         '<TD align="center">' || l_rec.route_shift                  || '</TD>' ||
                         '<TD align="right" >' || l_rec.ebs_bpa_po_number            || '</TD>' ||
                          '<TD align="right" >' || l_rec.bpa_start_date                 || '</TD>' ||
                         '<TD align="right" >' || l_rec.bpa_end_date                 || '</TD>' || --Added by Subodh on 01-JUN-2022 as requested by Leela
                          '<TD align="right" >' || l_rec.vehicle_mileage              || '</TD>' ||
                          '<TD align="right" >' || l_rec.trip_fuel_rate               || '</TD>' ||
                         '<TD align="right" >' || l_rec.po_startdate_fuel_rate       || '</TD>' ||
                         '<TD align="left"  >' || l_rec.calc_pay_type                || '</TD>' ||
                          '<TD align="right" >' || l_rec.trip_kilometers              || '</TD>' ||
                         '<TD align="right" >' || l_rec.calc_rate_per_pay_type       || '</TD>' ||
                         '<TD align="right" >' || l_rec.calc_extra_rate_per_pay_type || '</TD>' ||
                         '<TD align="right" >' || l_rec.calc_payable_fuel_price      || '</TD>' ||
                          '<TD align="right" >' || l_rec.calc_rate_km      || '</TD>' ||
                         '<TD align="right" >' || l_rec.payment_amount               || '</TD>' ||
                         '<TD align="left"  >' || l_rec.calc_status                  || '</TD>' ||
                         '<TD align="left"  >' || l_rec.calc_error_msg               || '</TD>' ||
                         '<TD align="left"  >' || l_rec.ebs_ap_invoice_num           || '</TD>' ||
                       '</TR>';
        output(l_data);
        l_total_amount := l_total_amount + l_rec.payment_amount;
  end loop;
  
  l_data   := '<TR BGCOLOR="#E6E6E6">' ||
                 '<TD colspan=17 align="right"><B>TOTAL AMOUNT:  <B></TD>' ||
                 '<TD colspan=1  align="right"><B>' || l_total_amount      || '<B></TD>' ||
                 '<TD colspan=3  align="left" ><B> <B></TD>' ||
               '</TR>';
  output(l_data);
  

l_data := '</TABLE>';
output(l_data);

l_step := 'END: ';

exception
    when others then
       output('END ERROR: ' || substr(sqlerrm,1,200));
end write_output;


procedure write_output_rate
is
l_prc                 varchar2(100) := 'write_output_rate';
l_step                varchar2(100);
l_route_type          varchar2(100);
l_data                varchar2(32767);
l_total_amount        number := 0;
l_excess              number := 0;
l_t_amount            number := 0;
l_t_toll              number := 0;
l_t_tp_qty            number := 0;
l_t_vc                number := 0;
l_t_excess            number := 0;
l_pro_cost            number := 0;
begin
l_step := 'START: ';
  log(l_prc,l_step );

  begin
    select text_value into l_route_type from xxbml_tpt_route_types_v where text_key = g_route_type;
  exception when others then
    l_route_type := '';
  end;

  l_data := '<TABLE border=1 style="border-collapse:collapse; font-family:verdana; font-size:90%;">'||
              '<TR>'  ||
                '<TH colspan=27 align="center" BGCOLOR="#E6E6E6"><B> BAMUL TRANSPORT BILLING PROGRAM REPORT </B></TH>'||
              '</TR>' ||
              '<TR>'  ||
                '<TH colspan=27 align="center" BGCOLOR="#E6E6E6"><B> BENGALURU URBAN, BENGALURU RURAL & RAMANAGAR DISTRICT CO-OP MILK PRODUCERS SOCIETIES UNION LTD.,    </B></TH>'||
              '</TR>' ||
              '<TR>'  ||
                '<TH colspan=5  align="left"   BGCOLOR="#E6E6E6"><B> From Date        : '   || to_char(g_from_date,'DD-MON-RR')  || '<br>'
                                                                || ' To Date          : '   || to_char(g_to_date,'DD-MON-RR')    || '<br>'
                                                                || ' Generate Invoice : '   || case when g_generate_invoice = 'Y' then 'Yes' else 'No' end
                      || '    </B></TH>'||
                '<TH colspan=14 align="left"   BGCOLOR="#E6E6E6"><B> Route Type   : ' || l_route_type || '<br>'
                                                                || ' Route Number : ' || g_route_number
                      || '    </B></TH>'||
                '<TH colspan=1  align="right"   BGCOLOR="#E6E6E6"><B> Request ID: '   || g_conc_request_id
                      || '    </B></TH>'||
              '</TR>' ||
              '<TR BGCOLOR="#E6E6E6">'||
                '<TD  align="left"   ><B>Date                       </B></TD>' ||
                '<TD  align="left"   ><B>Route                      </B></TD>' ||
                '<TD  align="left"   ><B>Transporter ID             </B></TD>' || --Added by Subodh on 01-JUN-2022 as requested by Leela
                '<TD  align="left"   ><B>Transporter Name           </B></TD>' || --Added by Subodh on 01-JUN-2022 as requested by Leela
                '<TD  align="center" ><B>Shift                      </B></TD>' ||
                '<TD r align="right"  ><B>BPA                        </B></TD>' ||
                  '<TD  align="right"  ><B>BPA Start Date               </B></TD>' || --Added by Subodh on 08-FEB-2024
                '<TD  align="right"  ><B>BPA End Date               </B></TD>' || --Added by Subodh on 01-JUN-2022 as requested by Leela
                '<TD  align="right"  ><B>VEHICLE MILEAGE            </B></TD>' ||
                  '<TD  align="right"  ><B>DIESEL PRICE (TRIP DATE)   </B></TD>' ||
                '<TD  align="right"  ><B>DIESEL PRICE (BPA START DATE) </B></TD>' ||
                '<TD  align="left"   ><B>PAYMENT TYPE               </B></TD>' ||
                 '<TD align="right"  ><B>TOTAL KMS                  </B></TD>' ||
                '<TD  align="right"  ><B>RATE/KM                       </B></TD>' ||
                '<TD  align="right"  ><B>EXTRA RATE(TOLL)                 </B></TD>'  ||
                '<TD  align="right"  ><B>PAYABLE FUEL PRICE         </B></TD>' ||
                  '<TD  align="right"  ><B>CALCULATED RATE/KM         </B></TD>' ||
                  
                  '<TD  align="right"  ><B>COST/LTR         </B></TD>' ||
                   '<TD  align="right"  ><B>TRANSPORTER QUANTITY IN KG        </B></TD>' ||
                   '<TD  align="right"  ><B>FIXED CAPCITY oF VEHICLE        </B></TD>' ||
                   '<TD  align="right"  ><B>EXCESS QUANTITY SUPPLIED        </B></TD>' ||
                   '<TD  align="right"  ><B>PRORATE COST        </B></TD>' ||
                
                '<TD  align="right"  ><B>AMOUNT                     </B></TD>' ||
                '<TD  align="right"  ><B>TOTAL AMOUNT                     </B></TD>' ||
                '<TD  align="left"   ><B>STATUS                     </B></TD>' ||
                '<TD  align="left"   ><B>ERROR                      </B></TD>' ||
                '<TD  align="left"   ><B>EBS INVOICE NUMBER         </B></TD>' ||
                
              '</TR>' ;
  output(l_data);
l_total_amount :=0;
l_t_amount     :=0;
l_t_toll               := 0;
l_t_tp_qty            := 0;
l_t_vc                 := 0;
l_t_excess             := 0;
l_pro_cost            := 0;
  for l_rec in (select schedule_date
                      ,tth.route_number
                      ,tth.route_shift
                      ,tth.ebs_bpa_po_number
                      ,nvl(calc_pay_type,
                                        (select unit_meas_lookup_code from po_lines_all pla
                                          where tth.route_number      = pla.vendor_product_num
                                            and tth.EBS_BPA_PO_HDR_ID = pla.po_header_id
                                             and trunc(tth.schedule_date) between trunc(to_date(pla.attribute1,'YYYY/MM/DD HH24:MI:SS')) and trunc(to_date(pla.attribute2,'YYYY/MM/DD HH24:MI:SS')) --Added by Subodh on 01-APR-2024
                                            )
                                            ) calc_pay_type
                      ,nvl(calc_rate_per_pay_type,
                                                 (select pla.unit_price from po_lines_all pla
                                                   where tth.route_number      = pla.vendor_product_num
                                                     and tth.EBS_BPA_PO_HDR_ID = pla.po_header_id
                                                        and trunc(tth.schedule_date) between trunc(to_date(pla.attribute1,'YYYY/MM/DD HH24:MI:SS')) and trunc(to_date(pla.attribute2,'YYYY/MM/DD HH24:MI:SS')) --Added by Subodh on 01-APR-2024
                                                     )) calc_rate_per_pay_type
                      ,calc_extra_rate_per_pay_type
                      ,calc_payable_fuel_price
                      ,payment_amount
                      ,calc_status
                      ,calc_error_msg
                      ,TRANSPORTER_NAME --Added by Subodh on 01-JUN-2022 as requested by Leela
                      ,(select max(segment1) from ap_suppliers asp
                         where 1=1
                         --and tth.TRANSPORTER_ID =asp.VENDOR_ID
                         and upper(tth.TRANSPORTER_NAME) = upper(asp.vendor_name)) TRANSPORTER_ID --Added by Subodh on 01-JUN-2022 as requested by Leela
                     ,(select MAX(fnd_date.canonical_to_date(attribute2)) from po_lines_all pla
                        where tth.route_number      = pla.vendor_product_num
                          and tth.EBS_BPA_PO_HDR_ID = pla.po_header_id
						  and trunc(tth.schedule_date) between fnd_date.canonical_to_date(attribute1) and fnd_date.canonical_to_date(attribute2) --Added by Subodh on 19-AUG-2025
                      ) bpa_end_date
                       ,(select MAX(fnd_date.canonical_to_date(attribute1)) from po_lines_all pla
                        where tth.route_number      = pla.vendor_product_num
                          and tth.EBS_BPA_PO_HDR_ID = pla.po_header_id
						  and trunc(tth.schedule_date) between fnd_date.canonical_to_date(attribute1) and fnd_date.canonical_to_date(attribute2) --Added by Subodh on 19-AUG-2025
                      ) bpa_start_date
                      ,nvl(tv.mileage,tth.calc_veh_mileage) vehicle_mileage
                       ,(select value
                         from TR_FUEL_RATE_CHART z
                        where z.fuel_type = nvl(tv.fuel_type,tth.calc_veh_fuel_type) --'Diesel'
                          and trunc(z.effective_date) = trunc(tth.schedule_date)
                          and rownum = 1) trip_fuel_rate
                          ,(select value
                         from TR_FUEL_RATE_CHART z,po_lines_all pol
                        where z.fuel_type = nvl(tv.fuel_type,tth.calc_veh_fuel_type) --'Diesel'
                          and trunc(z.effective_date) = trunc(to_date(pol.attribute1,'YYYY/MM/DD HH24:MI:SS'))
                          and  trunc(tth.schedule_date)   between trunc(to_date(pol.attribute1,'YYYY/MM/DD HH24:MI:SS')) and trunc(to_date(pol.attribute2,'YYYY/MM/DD HH24:MI:SS'))
                          and tth.EBS_BPA_PO_HDR_ID = pol.po_header_id
                          and tth.route_number      = pol.vendor_product_num
                          and rownum = 1) po_startdate_fuel_rate					
                     ,tth.rt_dist_in_km                 trip_kilometers
                     ,tth.ebs_ap_invoice_num
                     ,tth.calc_rate_km
                     ,tth.pro_cost
                     ,tth.cost_ltr
                     ,tv.CAPACITY vehicle_CAPACITY
                     ,tth.amount
                    , (select avg(NET_WEIGHT)
                      from  bmlcustm2.rm_shipping_hdr where 1=1 
                        and ROUT_TRX_NUM=tth.trip_trx_id) transport_qty
                       --,null bpa_end_date --Added by Subodh on 01-JUN-2022 as requested by Leela
                  from bmlcustm2.tr_trip_hdr tth
                  -- ,bmlcustm2.tr_vehicle  tv
                  , (select tv1.VEHICLE_ID
               , tr.route_number
               ,tv1.mileage
               ,tv1.capacity
               ,tv1.fuel_type
         from bmlcustm2.tr_route  tr
              ,bmlcustm2.tr_vehicle tv1
             where 1=1
             and tr.VEHICLE_ID = tv1.VEHICLE_ID ) tv
                 where calc_request_id     = g_conc_request_id
                 -- and tth.vehicle_id      = tv.vehicle_id(+)
                  and tth.route_number              = tv.route_number
                 order by schedule_date , tth.route_shift desc)
  loop
                    IF g_prorate_flag='Y' THEN  
                    l_excess :=      NVL(l_rec.transport_qty,0)-NVL(l_rec.vehicle_CAPACITY,0)  ;
                    
                    ELSE
                    l_excess :=0;
                    END IF;
                    
                    
           l_data   := '<TR>' ||
                         '<TD align="left"  >' || l_rec.schedule_date                || '</TD>' ||
                         '<TD align="left"  >' || l_rec.route_number                 || '</TD>' ||
                         '<TD align="left"  >' || l_rec.TRANSPORTER_ID               || '</TD>' || --Added by Subodh on 01-JUN-2022 as requested by Leela
                         '<TD align="left"  >' || l_rec.TRANSPORTER_NAME             || '</TD>' || --Added by Subodh on 01-JUN-2022 as requested by Leela
                         '<TD align="center">' || l_rec.route_shift                  || '</TD>' ||
                         '<TD align="right" >' || l_rec.ebs_bpa_po_number            || '</TD>' ||
                          '<TD align="right" >' || l_rec.bpa_start_date                 || '</TD>' ||
                         '<TD align="right" >' || l_rec.bpa_end_date                 || '</TD>' || --Added by Subodh on 01-JUN-2022 as requested by Leela
                          '<TD align="right" >' || l_rec.vehicle_mileage              || '</TD>' ||
                          '<TD align="right" >' || l_rec.trip_fuel_rate               || '</TD>' ||
                         '<TD align="right" >' || l_rec.po_startdate_fuel_rate       || '</TD>' ||
                         '<TD align="left"  >' || l_rec.calc_pay_type                || '</TD>' ||
                          '<TD align="right" >' || l_rec.trip_kilometers              || '</TD>' ||
                         '<TD align="right" >' || l_rec.calc_rate_per_pay_type       || '</TD>' ||
                         '<TD align="right" >' || l_rec.calc_extra_rate_per_pay_type || '</TD>' ||
                         '<TD align="right" >' || l_rec.calc_payable_fuel_price      || '</TD>' ||
                          '<TD align="right" >' || l_rec.calc_rate_km      || '</TD>' ||
                          
                           '<TD align="right" >' || l_rec.cost_ltr      || '</TD>' ||
                            
                             '<TD align="right" >' || l_rec.transport_qty      || '</TD>' ||
                             '<TD align="right" >' || l_rec.vehicle_CAPACITY      || '</TD>' ||
                              '<TD align="right" >' || l_excess      || '</TD>' ||
                               '<TD align="right" >' || l_rec.pro_cost      || '</TD>' ||
                          
                         '<TD align="right" >' || l_rec.amount                       || '</TD>' ||
                         '<TD align="right" >' || l_rec.payment_amount               || '</TD>' ||
                         '<TD align="left"  >' || l_rec.calc_status                  || '</TD>' ||
                         '<TD align="left"  >' || l_rec.calc_error_msg               || '</TD>' ||
                         '<TD align="left"  >' || l_rec.ebs_ap_invoice_num           || '</TD>' ||
                       '</TR>';
        output(l_data);
        l_total_amount := l_total_amount + NVL(l_rec.payment_amount,0);
        l_t_amount := l_t_amount + NVL(l_rec.amount,0) ;
        l_t_toll   := l_t_toll + NVL(l_rec.calc_extra_rate_per_pay_type,0);
        l_t_tp_qty := l_t_tp_qty + NVL(l_rec.transport_qty,0) ;
        l_t_vc     := l_t_vc +  NVL(l_rec.vehicle_CAPACITY,0) ;
        l_t_excess  := l_t_excess + NVL(l_excess,0);
        l_pro_cost  := l_pro_cost + NVL(l_rec.pro_cost,0);
  end loop;
  
  l_data   := '<TR BGCOLOR="#E6E6E6">' ||
                 '<TD colspan=14 align="right"><B>TOTAL :  <B></TD>' ||
                 '<TD colspan=1  align="right"><B>' || l_t_toll      || '<B></TD>' ||
                 '<TD colspan=3 align="right"><B>TOTAL :  <B></TD>' ||
                 '<TD colspan=1  align="right"><B>' || l_t_tp_qty      || '<B></TD>' ||
                 '<TD colspan=1  align="right"><B>' || l_t_vc      || '<B></TD>' ||
                 '<TD colspan=1  align="right"><B>' || l_t_excess      || '<B></TD>' ||
                 '<TD colspan=1  align="right"><B>' || l_pro_cost      || '<B></TD>' ||
                 '<TD colspan=1  align="right"><B>' || l_t_amount      || '<B></TD>' ||
                  '<TD colspan=1  align="right"><B>' || l_total_amount      || '<B></TD>' ||
                 '<TD colspan=3  align="left" ><B> <B></TD>' ||
               '</TR>';
  output(l_data);
  

l_data := '</TABLE>';
output(l_data);

l_step := 'END: ';

exception
    when others then
       output('END ERROR: ' || substr(sqlerrm,1,200));
end write_output_rate;





procedure write_output_dtc
is
l_prc                 varchar2(100) := 'write_output_dtc';
l_step                varchar2(100);
l_route_type          varchar2(100);
l_data                varchar2(32767);
l_total_amount        number := 0;
begin
l_step := 'START: ';
  log(l_prc,l_step );

  begin
    select text_value into l_route_type from xxbml_tpt_route_types_v where text_key = g_route_type;
  exception when others then
    l_route_type := '';
  end;

  l_data := '<TABLE border=1 style="border-collapse:collapse; font-family:verdana; font-size:90%;">'||
              '<TR>'  ||
                '<TH colspan=20 align="center" BGCOLOR="#E6E6E6"><B> BAMUL TRANSPORT BILLING PROGRAM REPORT </B></TH>'||
              '</TR>' ||
              '<TR>'  ||
                '<TH colspan=20 align="center" BGCOLOR="#E6E6E6"><B> BENGALURU URBAN, BENGALURU RURAL & RAMANAGAR DISTRICT CO-OP MILK PRODUCERS SOCIETIES UNION LTD.,    </B></TH>'||
              '</TR>' ||
              '<TR>'  ||
                '<TH colspan=5  align="left"   BGCOLOR="#E6E6E6"><B> From Date        : '   || to_char(g_from_date,'DD-MON-RR')  || '<br>'
                                                                || ' To Date          : '   || to_char(g_to_date,'DD-MON-RR')    || '<br>'
                                                                || ' Generate Invoice : '   || case when g_generate_invoice = 'Y' then 'Yes' else 'No' end
                      || '    </B></TH>'||
                '<TH colspan=14 align="left"   BGCOLOR="#E6E6E6"><B> Route Type   : ' || l_route_type || '<br>'
                                                                || ' Route Number : ' || g_route_number
                      || '    </B></TH>'||
                '<TH colspan=1  align="right"   BGCOLOR="#E6E6E6"><B> Request ID: '   || g_conc_request_id
                      || '    </B></TH>'||
--g_conc_request_id
              '</TR>' ||
              '<TR BGCOLOR="#E6E6E6">'||
                '<TD rowspan=2 align="left"   ><B>DATE                       </B></TD>' ||
                '<TD rowspan=2 align="left"   ><B>ROUTE                      </B></TD>' ||
                '<TD rowspan=2 align="center" ><B>SHIFT                      </B></TD>' ||
                '<TD colspan=2 align="center" ><B>TRANSPORTER                </B></TD>' || --Added by Subodh on 01-JUN-2022 as requested by Leela
                '<TD rowspan=2 align="right"  ><B>BPA                        </B></TD>' ||
                '<TD rowspan=2 align="right"  ><B>BPA START DATE             </B></TD>' ||
                '<TD rowspan=2 align="right"  ><B>BPA END DATE               </B></TD>' || --Added by Subodh on 01-JUN-2022 as requested by Leela
                --
                '<TD rowspan=2 align="right"  ><B>VEHICLE MILEAGE            </B></TD>' ||
                '<TD rowspan=2 align="right"  ><B>DIESEL PRICE (TRIP DATE)   </B></TD>' ||
                '<TD rowspan=2 align="right"  ><B>DIESEL PRICE (BPA START DATE) </B></TD>' ||
                '<TD rowspan=2 align="right"  ><B>TOTAL KMS                  </B></TD>' ||
                --
                '<TD rowspan=2 align="left"   ><B>PAYMENT TYPE               </B></TD>' ||
                '<TD rowspan=2 align="right"  ><B>RATE                       </B></TD>' ||
                '<TD rowspan=2 align="right"  ><B>EXTRA RATE                 </B></TD>'  ||
                '<TD rowspan=2 align="right"  ><B>PAYABLE FUEL PRICE         </B></TD>' ||
                '<TD rowspan=2 align="right"  ><B>AMOUNT                     </B></TD>' ||
                '<TD rowspan=2 align="left"   ><B>STATUS                     </B></TD>' ||
                '<TD rowspan=2 align="left"   ><B>ERROR                      </B></TD>' ||
                '<TD rowspan=2 align="left"   ><B>EBS INVOICE NUMBER         </B></TD>' ||
              '</TR>' ||
              '<TR BGCOLOR="#E6E6E6">'||
                '<TD     align="left"   ><B>NUMBER                  </B></TD>' ||
                '<TD     align="right"  ><B>NAME                    </B></TD>' ||
              '</TR>';
  output(l_data);

  for l_rec in (select tth.schedule_date
                      ,tth.route_number
                      ,tth.route_shift
                      ,tth.ebs_bpa_po_number
                      ,nvl(tth.calc_pay_type,
                                        (select unit_meas_lookup_code from po_lines_all pla
                                          where tth.parent_route_number      = pla.vendor_product_num
                                            and tth.EBS_BPA_PO_HDR_ID = pla.po_header_id 
                                            and trunc(tth.schedule_date) between trunc(to_date(pla.attribute1,'YYYY/MM/DD HH24:MI:SS')) and trunc(to_date(pla.attribute2,'YYYY/MM/DD HH24:MI:SS')) --Added by Subodh on 25-NOV-2022 
                                            )) calc_pay_type
                      ,nvl(tth.calc_rate_per_pay_type,
                                                 (select pla.unit_price from po_lines_all pla
                                                   where tth.parent_route_number      = pla.vendor_product_num
                                                     and tth.EBS_BPA_PO_HDR_ID = pla.po_header_id 
                                                     and trunc(tth.schedule_date) between trunc(to_date(pla.attribute1,'YYYY/MM/DD HH24:MI:SS')) and trunc(to_date(pla.attribute2,'YYYY/MM/DD HH24:MI:SS'))--Added by Subodh on 25-NOV-2022  
                                                     )) calc_rate_per_pay_type
                      ,tth.calc_extra_rate_per_pay_type
                      ,tth.calc_payable_fuel_price
                      ,tth.payment_amount
                      ,tth.calc_status
                      ,tth.calc_error_msg
                      ,tth.TRANSPORTER_NAME --Added by Subodh on 01-JUN-2022 as requested by Leela
                      ,(select max(segment1) from ap_suppliers asp
                         where 1=1
                         --and tth.TRANSPORTER_ID =asp.VENDOR_ID
                         and upper(tth.TRANSPORTER_NAME) = upper(asp.vendor_name)) TRANSPORTER_ID --Added by Subodh on 01-JUN-2022 as requested by Leela
--                     ,(select MAX(fnd_date.canonical_to_date(attribute1)) from po_lines_all pla
--                        where tth.parent_route_number      = pla.vendor_product_num
--                          and tth.EBS_BPA_PO_HDR_ID = pla.po_header_id
--                      ) bpa_start_date
                       ,(select MAX(fnd_date.canonical_to_date(attribute1)) from po_lines_all pla
                        where tth.parent_route_number      = pla.vendor_product_num
                          and tth.EBS_BPA_PO_HDR_ID = pla.po_header_id
                          and trunc(tth.schedule_date) between trunc(to_date(pla.attribute1,'YYYY/MM/DD HH24:MI:SS')) and trunc(to_date(pla.attribute2,'YYYY/MM/DD HH24:MI:SS'))--Added by Subodh on 25-NOV-2022
                      ) bpa_start_date
                     ,(select MAX(fnd_date.canonical_to_date(attribute2)) from po_lines_all pla
                        where tth.parent_route_number      = pla.vendor_product_num
                          and tth.EBS_BPA_PO_HDR_ID = pla.po_header_id
                          and trunc(tth.schedule_date) between trunc(to_date(pla.attribute1,'YYYY/MM/DD HH24:MI:SS')) and trunc(to_date(pla.attribute2,'YYYY/MM/DD HH24:MI:SS'))--Added by Subodh on 25-NOV-2022
                      ) bpa_end_date
                     ,nvl(tv.mileage,tth.calc_veh_mileage) vehicle_mileage
                    ,(select value
                         from TR_FUEL_RATE_CHART z
                        where z.fuel_type = nvl(tv.fuel_type,tth.calc_veh_fuel_type) --'Diesel'
                          and trunc(z.effective_date) = trunc(tth.schedule_date)
                          and rownum = 1) trip_fuel_rate
                          ,(select value
                         from TR_FUEL_RATE_CHART z,po_lines_all pol
                        where z.fuel_type = nvl(tv.fuel_type,tth.calc_veh_fuel_type) --'Diesel'
                          and trunc(z.effective_date) = trunc(to_date(pol.attribute1,'YYYY/MM/DD HH24:MI:SS'))
                          and  trunc(tth.schedule_date)   between trunc(to_date(pol.attribute1,'YYYY/MM/DD HH24:MI:SS')) and trunc(to_date(pol.attribute2,'YYYY/MM/DD HH24:MI:SS'))
                          and tth.EBS_BPA_PO_HDR_ID = pol.po_header_id
                          and tth.route_number      = pol.vendor_product_num
                          and rownum = 1) po_startdate_fuel_rate
								   
												  
																							   
																				
														
                     ,tth.rt_dist_in_km                 trip_kilometers
                     ,tth.ebs_ap_invoice_num
                  from bmlcustm2.tr_trip_hdr tth
                      ,bmlcustm2.tr_vehicle  tv
                 where calc_request_id     = g_conc_request_id --12016007  
                   and tth.vehicle_id      = tv.vehicle_id(+)
                     ---Added by Subodh on 18-NOV-2022 to pick only Active BPAs
				 and exists
                    (select 1 from po_headers_all pha 
               where tth.ebs_bpa_po_hdr_id = pha.po_header_id 
                 and NVL(pha.CLOSED_CODE,'OPEN'  )= 'OPEN' 
                 and NVL(pha.CLOSED_DATE,trunc(sysdate+1)) >= trunc(sysdate) 
                 and NVL(pha.CANCEL_FLAG,'N')='N' )
                 order by schedule_date)
  loop
           l_data   := '<TR>' ||
                         '<TD align="left"  >' || l_rec.schedule_date                || '</TD>' ||
                         '<TD align="left"  >' || l_rec.route_number                 || '</TD>' ||
                         '<TD align="center">' || l_rec.route_shift                  || '</TD>' ||
                         '<TD align="left"  >' || l_rec.TRANSPORTER_ID               || '</TD>' || --Added by Subodh on 01-JUN-2022 as requested by Leela
                         '<TD align="left"  >' || l_rec.TRANSPORTER_NAME             || '</TD>' || --Added by Subodh on 01-JUN-2022 as requested by Leela
                         '<TD align="right" >' || l_rec.ebs_bpa_po_number            || '</TD>' ||
                         '<TD align="right" >' || l_rec.bpa_start_date               || '</TD>' ||
                         '<TD align="right" >' || l_rec.bpa_end_date                 || '</TD>' || --Added by Subodh on 01-JUN-2022 as requested by Leela
                         --
                         '<TD align="right" >' || l_rec.vehicle_mileage              || '</TD>' ||
                         '<TD align="right" >' || l_rec.trip_fuel_rate               || '</TD>' ||
                         '<TD align="right" >' || l_rec.po_startdate_fuel_rate       || '</TD>' ||
                         '<TD align="right" >' || l_rec.trip_kilometers              || '</TD>' ||
                         --
                         '<TD align="left"  >' || l_rec.calc_pay_type                || '</TD>' ||
                         '<TD align="right" >' || l_rec.calc_rate_per_pay_type       || '</TD>' ||
                         '<TD align="right" >' || l_rec.calc_extra_rate_per_pay_type || '</TD>' ||
                         '<TD align="right" >' || l_rec.calc_payable_fuel_price      || '</TD>' ||
                         '<TD align="right" >' || l_rec.payment_amount               || '</TD>' ||
                         '<TD align="left"  >' || l_rec.calc_status                  || '</TD>' ||
                         '<TD align="left"  >' || l_rec.calc_error_msg               || '</TD>' ||
                         '<TD align="left"  >' || l_rec.ebs_ap_invoice_num           || '</TD>' ||
                       '</TR>';
        output(l_data);
        l_total_amount := l_total_amount + l_rec.payment_amount;
  end loop;

  l_data   := '<TR BGCOLOR="#E6E6E6">' ||
                 '<TD colspan=16 align="right"><B>TOTAL AMOUNT:  <B></TD>' ||
                 '<TD colspan=1  align="right"><B>' || l_total_amount      || '<B></TD>' ||
                 '<TD colspan=3  align="left" ><B> <B></TD>' ||
               '</TR>';
  output(l_data);

l_data := '</TABLE>';
output(l_data);

l_step := 'END: ';

exception
    when others then
       output('END ERROR: ' || substr(sqlerrm,1,200));
end write_output_dtc;





procedure write_output_dtc_adhoc
is
l_prc                 varchar2(100) := 'write_output_dtc_adhoc';
l_step                varchar2(100);
l_route_type          varchar2(100);
l_data                varchar2(32767);
l_total_amount        number := 0;
begin
l_step := 'START: ';
  log(l_prc,l_step );

  begin
    select text_value into l_route_type from xxbml_tpt_route_types_v where text_key = g_route_type;
  exception when others then
    l_route_type := '';
  end;

  l_data := '<TABLE border=1 style="border-collapse:collapse; font-family:verdana; font-size:90%;">'||
              '<TR>'  ||
                '<TH colspan=11 align="center" BGCOLOR="#E6E6E6"><B> BAMUL TRANSPORT BILLING PROGRAM REPORT </B></TH>'||
              '</TR>' ||
              '<TR>'  ||
                '<TH colspan=11 align="center" BGCOLOR="#E6E6E6"><B> BENGALURU URBAN, BENGALURU RURAL & RAMANAGAR DISTRICT CO-OP MILK PRODUCERS SOCIETIES UNION LTD.,    </B></TH>'||
              '</TR>' ||
              '<TR>'  ||
                '<TH colspan=5  align="left"   BGCOLOR="#E6E6E6"><B> From Date        : '   || to_char(g_from_date,'DD-MON-RR')  || '<br>'
                                                                || ' To Date          : '   || to_char(g_to_date,'DD-MON-RR')    || '<br>'
                                                                || ' Generate Invoice : '   || case when g_generate_invoice = 'Y' then 'Yes' else 'No' end
                      || '    </B></TH>'||
                '<TH colspan=5  align="left"   BGCOLOR="#E6E6E6"><B> Route Type   : ' || l_route_type || '<br>'
                                                                || ' Route Number : ' || g_route_number
                      || '    </B></TH>'||
                '<TH colspan=1  align="right"   BGCOLOR="#E6E6E6"><B> Request ID: '   || g_conc_request_id
                      || '    </B></TH>'||
              '</TR>' ||
              '<TR BGCOLOR="#E6E6E6">'||
                '<TD rowspan=2 align="left"   ><B>DATE                       </B></TD>' ||
                '<TD rowspan=2 align="left"   ><B>ROUTE                      </B></TD>' ||
                '<TD rowspan=2 align="center" ><B>SHIFT                      </B></TD>' ||
                '<TD colspan=2 align="center" ><B>TRANSPORTER                </B></TD>' || 
                '<TD rowspan=2 align="right"  ><B>TOTAL CRATES               </B></TD>' ||
                '<TD rowspan=2 align="right"  ><B>TOTAL KILOMETERS           </B></TD>' ||
                '<TD rowspan=2 align="right"  ><B>AMOUNT                     </B></TD>' ||
                '<TD rowspan=2 align="left"   ><B>STATUS                     </B></TD>' ||
                '<TD rowspan=2 align="left"   ><B>ERROR                      </B></TD>' ||
                '<TD rowspan=2 align="left"   ><B>EBS INVOICE NUMBER         </B></TD>' ||
              '</TR>' ||
              '<TR BGCOLOR="#E6E6E6">'||
                '<TD     align="left"   ><B>NUMBER                  </B></TD>' ||
                '<TD     align="right"  ><B>NAME                    </B></TD>' ||
              '</TR>';
  output(l_data);


  for h_rec in (select distinct tth.TRANSPORTER_NAME
                  from bmlcustm2.tr_trip_hdr tth
                      ,bmlcustm2.tr_vehicle  tv
                 where calc_request_id     = g_conc_request_id
                   and tth.vehicle_id      = tv.vehicle_id(+)
                 order by tth.TRANSPORTER_NAME)
  loop
      l_total_amount := 0;
      for l_rec in (select tth.schedule_date
                          ,tth.route_number
                          ,tth.route_shift
                          ,tth.payment_amount
                          ,tth.calc_status
                          ,tth.calc_error_msg
                          ,tth.TRANSPORTER_NAME --Added by Subodh on 01-JUN-2022 as requested by Leela
                          ,(select max(segment1) from ap_suppliers asp
                             where 1=1
                             and upper(tth.TRANSPORTER_NAME) = upper(asp.vendor_name)) TRANSPORTER_ID --Added by Subodh on 01-JUN-2022 as requested by Leela
                         ,tth.rt_dist_in_km  trip_kilometers
                         ,(select sum(total_crates)
                             from bmlcustm2.tr_dispship_hdr
                            where report_date = tth.schedule_date     --'26-FEB-2022'
                              and shift       = tth.route_shift   --'M'
                              and route_no    = tth.route_number --'BTCD773'
                         ) trip_crates
                         ,tth.ebs_ap_invoice_num
                      from bmlcustm2.tr_trip_hdr tth
                          ,bmlcustm2.tr_vehicle  tv
                     where calc_request_id      = g_conc_request_id
                       and tth.vehicle_id       = tv.vehicle_id(+)
                       and tth.transporter_name = h_rec.transporter_name
                         ---Added by Subodh on 18-NOV-2022 to pick only Active BPAs
--				 and exists
--                    (select 1 from po_headers_all pha 
--               where tth.ebs_bpa_po_hdr_id = pha.po_header_id 
--                and NVL(pha.CLOSED_CODE,'OPEN'  )= 'OPEN'
--                 and NVL(pha.CLOSED_DATE,trunc(sysdate+1)) >= trunc(sysdate) 
--                 and NVL(pha.CANCEL_FLAG,'N')='N' )
                     order by schedule_date)
      loop
               l_data   := '<TR>' ||
                             '<TD align="left"  >' || l_rec.schedule_date                || '</TD>' ||
                             '<TD align="left"  >' || l_rec.route_number                 || '</TD>' ||
                             '<TD align="center">' || l_rec.route_shift                  || '</TD>' ||
                             '<TD align="left"  >' || l_rec.TRANSPORTER_ID               || '</TD>' || --Added by Subodh on 01-JUN-2022 as requested by Leela
                             '<TD align="left"  >' || l_rec.TRANSPORTER_NAME             || '</TD>' || --Added by Subodh on 01-JUN-2022 as requested by Leela
                             '<TD align="right" >' || l_rec.trip_crates                  || '</TD>' ||
                             '<TD align="right" >' || l_rec.trip_kilometers              || '</TD>' ||
                             '<TD align="right" >' || l_rec.payment_amount               || '</TD>' ||
                             '<TD align="left"  >' || l_rec.calc_status                  || '</TD>' ||
                             '<TD align="left"  >' || l_rec.calc_error_msg               || '</TD>' ||
                             '<TD align="left"  >' || l_rec.ebs_ap_invoice_num           || '</TD>' ||
                           '</TR>';
            output(l_data);
            l_total_amount := l_total_amount + l_rec.payment_amount;
      end loop;  --for l_rec

      l_data   := '<TR BGCOLOR="#E6E6E6">' ||
                     '<TD colspan=7 align="right"><B>TOTAL AMOUNT:  <B></TD>' ||
                     '<TD colspan=1 align="right"><B>' || l_total_amount      || '<B></TD>' ||
                     '<TD colspan=3 align="left" ><B> <B></TD>' ||
                   '</TR>';
      output(l_data);
  end loop;  --for h_rec


l_data := '</TABLE>';
output(l_data);

l_step := 'END: ';

exception
    when others then
       output('END ERROR: ' || substr(sqlerrm,1,200));
end write_output_dtc_adhoc;


procedure write_output_tcd
is
l_prc                 varchar2(100) := 'write_output_tcd';
l_step                varchar2(100);
l_route_type          varchar2(100);
l_data                varchar2(32767);
l_total_amount        number := 0;
begin
l_step := 'START: ';
  log(l_prc,l_step );

  begin
    select text_value into l_route_type from xxbml_tpt_route_types_v where text_key = g_route_type;
  exception when others then
    l_route_type := '';
  end;

  l_data := '<TABLE border=1 style="border-collapse:collapse; font-family:verdana; font-size:90%;">'||
              '<TR>'  ||
                '<TH colspan=16 align="center" BGCOLOR="#E6E6E6"><B> BAMUL TRANSPORT BILLING PROGRAM REPORT </B></TH>'||
              '</TR>' ||
              '<TR>'  ||
                '<TH colspan=16 align="center" BGCOLOR="#E6E6E6"><B> BENGALURU URBAN, BENGALURU RURAL & RAMANAGAR DISTRICT CO-OP MILK PRODUCERS SOCIETIES UNION LTD.,    </B></TH>'||
              '</TR>' ||
              '<TR>'  ||
                '<TH colspan=5  align="left"   BGCOLOR="#E6E6E6"><B> From Date        : '   || to_char(g_from_date,'DD-MON-RR')  || '<br>'
                                                                || ' To Date          : '   || to_char(g_to_date,'DD-MON-RR')    || '<br>'
                                                                || ' Generate Invoice : '   || case when g_generate_invoice = 'Y' then 'Yes' else 'No' end
                      || '    </B></TH>'||
                '<TH colspan=10 align="left"   BGCOLOR="#E6E6E6"><B> Route Type   : ' || l_route_type || '<br>'
                                                                || ' Route Number : ' || g_route_number
                      || '    </B></TH>'||
                '<TH colspan=1  align="right"   BGCOLOR="#E6E6E6"><B> Request ID: '   || g_conc_request_id
                      || '    </B></TH>'||
              '</TR>' ||
              '<TR BGCOLOR="#E6E6E6">'||
                '<TD rowspan=2 align="left"   ><B>ROUTE                      </B></TD>' ||
                '<TD rowspan=2 align="left"   ><B>DATE                       </B></TD>' ||
                '<TD rowspan=2 align="center" ><B>SHIFT                      </B></TD>' ||
                '<TD colspan=2 align="center" ><B>TRANSPORTER                </B></TD>' ||
                '<TD rowspan=2 align="right"  ><B>BPA                        </B></TD>' ||
                '<TD rowspan=2 align="right"  ><B>BPA START DATE             </B></TD>' ||
                '<TD rowspan=2 align="right"  ><B>BPA END DATE               </B></TD>' ||
                '<TD rowspan=2 align="left"   ><B>PAYMENT TYPE               </B></TD>' ||
                '<TD rowspan=2 align="right"  ><B>KGS/LTR                    </B></TD>' ||
                '<TD rowspan=2 align="right"  ><B>TOTAL QUANTITY             </B></TD>' ||
                '<TD rowspan=2 align="right"  ><B>RATE PER KGS/LTR           </B></TD>' ||
                '<TD rowspan=2 align="right"  ><B>AMOUNT                     </B></TD>' ||
                '<TD rowspan=2 align="left"   ><B>STATUS                     </B></TD>' ||
                '<TD rowspan=2 align="left"   ><B>ERROR                      </B></TD>' ||
                '<TD rowspan=2 align="left"   ><B>EBS INVOICE NUMBER         </B></TD>' ||
              '</TR>' ||
              '<TR BGCOLOR="#E6E6E6">'||
                '<TD     align="left"   ><B>NUMBER                  </B></TD>' ||
                '<TD     align="right"  ><B>NAME                    </B></TD>' ||
              '</TR>';
  output(l_data);

  for l_rec in (select schedule_date
                      ,route_number
                      ,route_shift
                      ,ebs_bpa_po_number
                      ,'Quantity Based' calc_pay_type
                      ,(select pol.unit_price
                          from po_headers_all poh
                              ,po_lines_all   pol
                         where pol.po_header_id       = poh.po_header_id
                           and pol.vendor_product_num = tth.route_number
                           and poh.po_header_id       = tth.ebs_bpa_po_hdr_id
                           and rownum = 1) bpa_rate_per_kgs_ltr
                      ,calc_details
                      --,acctual_total_qty
                      ,acctual_total_qty1 --Added by Subodh on 06-FEB-2024 due to decimal data type issue
                      ,payment_amount
                      ,calc_status
                      ,calc_error_msg
                       ,TRANSPORTER_NAME --Added by Subodh on 01-JUN-2022 as requested by Leela
                       ,(select max(segment1) from ap_suppliers asp
                         where 1=1
                         --and tth.TRANSPORTER_ID =asp.VENDOR_ID
                         and upper(tth.TRANSPORTER_NAME) = upper(asp.vendor_name)) TRANSPORTER_ID 
                     ,(select MAX(fnd_date.canonical_to_date(attribute1)) from po_lines_all pla
                       -- where tth.parent_route_number      = pla.vendor_product_num
                         where tth.route_number      = pla.vendor_product_num --Added by Subodh on 08-JUL-2024
                          and tth.EBS_BPA_PO_HDR_ID = pla.po_header_id
                      ) bpa_start_date
                     ,(select MAX(fnd_date.canonical_to_date(attribute2)) from po_lines_all pla
                       -- where tth.parent_route_number      = pla.vendor_product_num
                        where tth.route_number      = pla.vendor_product_num --Added by Subodh on 08-JUL-2024
                          and tth.EBS_BPA_PO_HDR_ID = pla.po_header_id
                      ) bpa_end_date
                     ,tth.ebs_ap_invoice_num
                  from bmlcustm2.tr_trip_hdr tth
                 where calc_request_id     = g_conc_request_id
                 order by case when route_number = parent_route_number then 1 else 2 end
                         ,route_number,schedule_date,route_shift)
  loop
           l_data   := '<TR>' ||
                         '<TD align="left"  >' || l_rec.route_number                 || '</TD>' ||
                         '<TD align="left"  >' || l_rec.schedule_date                || '</TD>' ||
                         '<TD align="center">' || l_rec.route_shift                  || '</TD>' ||
                         '<TD align="left"  >' || l_rec.TRANSPORTER_ID               || '</TD>' ||
                         '<TD align="left"  >' || l_rec.TRANSPORTER_NAME             || '</TD>' ||
                         '<TD align="right" >' || l_rec.ebs_bpa_po_number            || '</TD>' ||
                         '<TD align="right" >' || l_rec.bpa_start_date               || '</TD>' ||
                         '<TD align="right" >' || l_rec.bpa_end_date                 || '</TD>' ||
                         '<TD align="left"  >' || l_rec.calc_pay_type                || '</TD>' ||
                         '<TD align="right" >' || l_rec.calc_details                 || '</TD>' ||
                         '<TD align="right" >' || l_rec.acctual_total_qty1            || '</TD>' ||
                         '<TD align="right" >' || l_rec.bpa_rate_per_kgs_ltr         || '</TD>' ||
                         '<TD align="right" >' || l_rec.payment_amount               || '</TD>' ||
                         '<TD align="left"  >' || l_rec.calc_status                  || '</TD>' ||
                         '<TD align="left"  >' || l_rec.calc_error_msg               || '</TD>' ||
                         '<TD align="left"  >' || l_rec.ebs_ap_invoice_num           || '</TD>' ||
                       '</TR>';
        output(l_data);
        l_total_amount := l_total_amount + nvl(l_rec.payment_amount,0);
  end loop;

  l_data   := '<TR BGCOLOR="#E6E6E6">' ||
                 '<TD colspan=12 align="right"><B>TOTAL AMOUNT:  <B></TD>' ||
                 '<TD colspan=1  align="right"><B>' || l_total_amount      || '<B></TD>' ||
                 '<TD colspan=3  align="left" ><B> <B></TD>' ||
               '</TR>';
  output(l_data);

l_data := '</TABLE>';
output(l_data);

l_step := 'END: ';

exception
    when others then
       output('END ERROR: ' || substr(sqlerrm,1,200));
end write_output_tcd;


procedure write_output_bmc_itb
is
l_prc                 varchar2(100) := 'write_output_bmc_itb';
l_step                varchar2(100);
l_route_type          varchar2(100);
l_data                varchar2(32767);
begin
l_step := 'START: ';
  log(l_prc,l_step );

  begin
    select text_value into l_route_type from xxbml_tpt_route_types_v where text_key = g_route_type;
  exception when others then
    l_route_type := '';
  end;

  l_data := '<TABLE border=1 style="border-collapse:collapse; font-family:verdana; font-size:90%;">'||
              '<TR>'  ||
                '<TH colspan=6 align="center" BGCOLOR="#E6E6E6"><B> BAMUL TRANSPORT BILLING PROGRAM REPORT </B></TH>'||
              '</TR>' ||
              '<TR>'  ||
                '<TH colspan=6 align="center" BGCOLOR="#E6E6E6"><B> BENGALURU URBAN, BENGALURU RURAL & RAMANAGAR DISTRICT CO-OP MILK PRODUCERS SOCIETIES UNION LTD.,    </B></TH>'||
              '</TR>' ||
              '<TR>'  ||
                '<TH colspan=6 align="left"   BGCOLOR="#E6E6E6"><B> From Date: '   || to_char(g_from_date,'DD-MON-RR') || '<br>'
                                                                || ' To Date: '    || to_char(g_to_date,'DD-MON-RR')    || '<br>'
                                                                || ' Route Type: ' || l_route_type
                      || '    </B></TH>'||
              '</TR>' ||
              '<TR BGCOLOR="#E6E6E6">'||
                '<TD align="left"   ><B>Date                       </B></TD>' ||
                '<TD align="left"   ><B>Route                      </B></TD>' ||
                '<TD align="left"   ><B>Transporter                </B></TD>' ||
                '<TD align="right"  ><B>AMOUNT                     </B></TD>' ||
                '<TD align="left"   ><B>STATUS                     </B></TD>' ||
                '<TD align="left"   ><B>ERROR                      </B></TD>' ||
              '</TR>';
  output(l_data);

  for l_rec in (select schedule_date
                      ,route_number
                      ,to_char(transporter_id) || ' - ' || (select vendor_name from ap_suppliers where segment1=to_char(transporter_id)) transporter_id
                      ,bmc_itb_payment_amount
                      ,bmc_itb_status
                      ,bmc_itb_error_msg
                  from bmlcustm2.tr_trip_hdr tth
                 where bmc_itb_request_id = g_conc_request_id
                 order by schedule_date,route_number)
  loop
           l_data   := '<TR>' ||
                         '<TD align="left"  >' || l_rec.schedule_date                || '</TD>' ||
                         '<TD align="left"  >' || l_rec.route_number                 || '</TD>' ||
                         '<TD align="left"  >' || l_rec.transporter_id               || '</TD>' ||
                         '<TD align="right" >' || l_rec.bmc_itb_payment_amount       || '</TD>' ||
                         '<TD align="left"  >' || l_rec.bmc_itb_status               || '</TD>' ||
                         '<TD align="left"  >' || l_rec.bmc_itb_error_msg            || '</TD>' ||
                       '</TR>';
        output(l_data);
  end loop;

l_data := '</TABLE>';
output(l_data);

l_step := 'END: ';

exception
    when others then
       output('END ERROR: ' || substr(sqlerrm,1,200));
end write_output_bmc_itb;


procedure billing_bmc_itb(x_errbuf            out varchar2
                         ,x_errcode           out number)
as
l_prc                      varchar2(50) := 'billing_bmc_itb';
l_step                     varchar2(100);
l_return_status            varchar2(1);
l_return_msg               varchar2(4000);
l_skip                     exception;
l_vendor_number            number;
--
l_rate_per_km              number;
l_rate_per_kg              number;
l_min_threshold_amt        number;
l_max_threshold_amt        number;
l_shift_count              number;
l_qty_in_kg                number;
l_final_amount             number;
l_bmc_itb_payment_amount   number;

cursor trip_cur
is
select tr.route_type
      ,trunc(tr.schedule_date) trip_date
      ,tr.route_number
      ,tr.route_shift
      ,nvl(tr.rt_dist_in_km,0) trip_in_kilometers
      ,tr.trip_trx_id
 from  bmlcustm2.tr_trip_hdr tr
 where 1=1
   --AND tr.route_status='APPROVED'
   --parameters
   and tr.bmc_itb_request_id      = g_conc_request_id
 order by tr.schedule_date,tr.route_number;


begin
x_errcode        := 0;
l_return_status  := 'S';
l_step           := 'START: ';
log(l_prc,l_step);


--log(l_prc,l_step || 'p_from_date =' || p_from_date || ',p_to_date=' || p_to_date
--                 || ',p_route_type=' || p_route_type || ',p_route_shift=' || p_route_shift
--                 || ',p_route_number=' || p_route_number || ',p_vendor_id=' || p_vendor_id
--                 || ',p_generate_invoice=' || p_generate_invoice);

if g_vendor_id is not null then
   select to_number(segment1) into l_vendor_number from ap_suppliers where vendor_id = g_vendor_id;
end if;


update bmlcustm2.tr_trip_hdr tr
   set bmc_itb_request_id            = g_conc_request_id
      ,bmc_itb_payment_amount        = null
      ,bmc_itb_status                = 'NEW'
      ,bmc_itb_error_msg             = ''
      ,bmc_itb_ap_invoice_id         = -9
 where tr.ROUTE_TYPE                 = 'BMC'
   --AND NVL(tr.ROUTE_SHIFT,'$##$')  = NVL(g_route_Shift,NVL(tr.ROUTE_SHIFT,'$##$'))
   AND NVL(tr.ROUTE_NUMBER,'X')      = NVL(g_route_number,NVL(tr.ROUTE_NUMBER,'X'))
   AND (NVL(tr.transporter_id,-1)  = NVL(l_vendor_number,NVL(tr.transporter_id,-1))
        or
       (NVL(tr.transporter_id,-1)  = NVL(g_vendor_id,NVL(tr.transporter_id,-1))))
   and trunc(tr.schedule_date)       between g_from_date and g_to_date
   and tr.bmc_itb_ap_invoice_id      is null
   and nvl(tr.bmc_itb_status,'NEW')  in ('NEW','ERROR','CALCULATED') ;

log(l_prc,'Rows picked = ' || sql%rowcount);
commit;


  begin
    select to_number(attribute1) rate_per_km
          ,to_number(attribute2) rate_per_kg
          ,to_number(attribute3) min_threshold_amt
          ,to_number(attribute4) max_threshold_amt
      into l_rate_per_km
          ,l_rate_per_kg
          ,l_min_threshold_amt
          ,l_max_threshold_amt
      from fnd_lookup_values_vl
     where lookup_type='XXBML_BMC_INTERNAL_TRANSFER'
       and sysdate between nvl(start_date_active,sysdate) and nvl(end_date_active,sysdate+1);
  exception when others then
    l_return_msg := 'Error while deriving Bamul Internal Transfer Billing Rates: ' || substr(sqlerrm,1,200);
    raise l_skip;
  end;

log(l_prc,l_rate_per_km || '; ' || l_rate_per_kg || '; ' || l_min_threshold_amt || '; ' || l_max_threshold_amt);

  --loop trip_header
  l_step := 'LOOP_TRIP: ';
  for trip_rec in trip_cur()
  loop
    begin
      --
      l_step                        := 'LOOPING TRIP_TRX_ID:' || trip_rec.trip_trx_id || ': ';
      l_return_msg                  := '';
      l_bmc_itb_payment_amount      := 0;
      l_shift_count                 := 0;
      l_qty_in_kg                   := 0;
      l_final_amount                := 0;
      --

      log(l_prc,'trip_rec.trip_in_kilometers= ' || to_char(trip_rec.trip_in_kilometers));
      if trip_rec.trip_in_kilometers = 0 then
         l_return_msg := 'No Estimated Kilometer on Trip.';
         raise l_skip;
      end if;

      l_step := 'START_AMOUNT_CALC: ';
      for bmc_rec in (select shift,sum(nvl(quantity_received,0)) qty_in_kg
                        from bmlcustm2.rm_shipping_hdr
                        where transaction_code = 'BMC'
                          and route_no         = trip_rec.route_number --'BMC-21'
                          and rout_trx_num     = trip_rec.trip_trx_id  --321990
                          and shipped_date     = trip_rec.trip_date    --'18-APR-2022'
                        group by shift)
      loop
          l_shift_count := l_shift_count + 1;
          l_qty_in_kg   := bmc_rec.qty_in_kg;
      end loop;

      --
      if    l_shift_count = 0 then
            l_return_msg := 'No BMC Data.';
            raise l_skip;
      elsif l_shift_count = 1 then
            l_min_threshold_amt := l_min_threshold_amt/2;
            l_max_threshold_amt := l_max_threshold_amt/2;
            l_final_amount      := ( ( (trip_rec.trip_in_kilometers/2) * l_rate_per_km) +
                                     (l_qty_in_kg * l_rate_per_kg) )/2;
      else
            l_final_amount      := ( (trip_rec.trip_in_kilometers * l_rate_per_km) +
                                     (l_qty_in_kg * l_rate_per_kg) )/2;
      end if;

      --
      if    l_final_amount < l_min_threshold_amt then
            l_bmc_itb_payment_amount := l_min_threshold_amt;
      else
            l_bmc_itb_payment_amount := l_max_threshold_amt;
      end if;

      l_step := 'AFTER_TOTAL_TRIP_AMT_CALC: ';

      update tr_trip_hdr
         set bmc_itb_payment_amount  = l_bmc_itb_payment_amount
            ,bmc_itb_status          = 'CALCULATED'
            ,bmc_itb_error_msg       = ''
       where trip_trx_id             = trip_rec.trip_trx_id;

      l_step := 'CALC_AMOUNT_UPDATED: ';
      --
      commit;

    exception
      when l_skip then
        rollback;
        update tr_trip_hdr tr
           set tr.bmc_itb_status    = 'ERROR'
              ,tr.bmc_itb_error_msg = l_return_msg
         where trip_trx_id          = trip_rec.trip_trx_id;
        log(l_prc,'after error update');
      when others then
        rollback;
        l_return_msg := l_step || substr(sqlerrm,1,200);
        update tr_trip_hdr tr
           set tr.bmc_itb_status    = 'ERROR'
              ,tr.bmc_itb_error_msg = l_return_msg
         where trip_trx_id          = trip_rec.trip_trx_id;
    end;

    commit;
    --
  end loop;  --for trip_rec

  l_step := 'AFTER_TRIP_LOOP: ';
  --
  if g_generate_invoice = 'Y' then
     null;
     --create ap invoice here
  else
      update tr_trip_hdr tr
         set tr.bmc_itb_ap_invoice_id = null
            ,tr.bmc_itb_status        = case when tr.bmc_itb_status = 'ERROR' then 'ERROR' else 'NEW' end
       where tr.bmc_itb_request_id    = g_conc_request_id
         and tr.bmc_itb_ap_invoice_id = -9;
  end if;

  l_step := 'B4_WRITING_OUTPUT: ';
  write_output_bmc_itb();

log(l_prc, ' END');
exception
    when l_skip then
         x_errcode := 2;
         x_errbuf  := ' END ERROR L_SKIP: ' || l_return_msg;
         log(l_prc,x_errbuf);
         rollback;
         --l_return_msg := l_step || substr(sqlerrm,1,200);
         update tr_trip_hdr tr
            set bmc_itb_ap_invoice_id = null
               ,tr.bmc_itb_status     = 'ERROR'
               ,tr.bmc_itb_error_msg  = x_errbuf
          where bmc_itb_request_id    = g_conc_request_id;
    when others then
         x_errcode := 2;
         x_errbuf  := 'END ERROR: ' || l_step || substr(sqlerrm,1,250);
         log(l_prc, x_errbuf);
         rollback;
         update tr_trip_hdr tr
            set bmc_itb_ap_invoice_id = null
               ,tr.bmc_itb_status     = 'ERROR'
               ,tr.bmc_itb_error_msg  = x_errbuf
          where bmc_itb_request_id    = g_conc_request_id;
         commit;
end billing_bmc_itb;


procedure billing_tcd(x_errbuf            out varchar2
                     ,x_errcode           out number)
as
l_prc                      varchar2(50) := 'billing_tcd';
l_step                     varchar2(100);
l_return_status            varchar2(1);
l_return_msg               varchar2(4000);
l_skip                     exception;
l_stop                     exception;
l_vendor_number            number;
--
l_ds_hdr_id                number;
l_tcd_count                number;
l_qty_in_kgs               number;
l_qty_in_ltr               number;
l_acctual_total_qty        number;
l_total_trip_amount        number;
l_calc_details             varchar2(4000);
--
l_rowcount                    number := 0;

cursor trip_cur
is
select tr.route_type
      ,trunc(tr.schedule_date) trip_date
      ,tr.route_number
      ,tr.parent_route_number
      ,tr.route_shift
      ,nvl(tr.rt_dist_in_km,0) trip_in_kilometers
      ,(select pol.unit_price
          from po_headers_all poh
              ,po_lines_all   pol
         where pol.po_header_id       = poh.po_header_id
          -- and pol.vendor_product_num = tr.parent_route_number
           and pol.vendor_product_num = tr.route_number -- Added by Subodh on 08-JUL-2024
           and poh.po_header_id       = tr.ebs_bpa_po_hdr_id
           and rownum = 1) bpa_rate_per_kgs_ltr
      ,tr.trip_trx_id
      ,tr.parent_trip_trx_id
 from  bmlcustm2.tr_trip_hdr tr
 where 1=1
   --AND tr.route_status='APPROVED'
   --parameters
   and tr.calc_request_id      = g_conc_request_id
   --Added by Subodh on 30-JAN-2024 to Exclude not dispathed routes
   and exists 
   (select 1
          from bmlcustm2.tr_dispship_hdr
         where report_date = trunc(tr.schedule_date)     --'26-FEB-2022'
           and shift       = tr.route_shift   --'M'
            and NVL(DISPATCH_STATUS,'N') = 'Y'
           and route_no    = tr.route_number)
 ---Added by Subodh on 02-JUL-2024 to pick only Active BPAs
    and exists
  (select 1 from po_headers_all pha 
   where tr.ebs_bpa_po_hdr_id = pha.po_header_id 
 and NVL(pha.CLOSED_CODE,'OPEN'  )= 'OPEN'
   and NVL(pha.CLOSED_DATE,trunc(sysdate+1)) >= trunc(sysdate) 
   and NVL(pha.CANCEL_FLAG,'N')='N' )        
   
   
 order by tr.schedule_date,tr.parent_route_number;


begin
x_errcode        := 0;
l_return_status  := 'S';
l_step           := 'START: ';
log(l_prc,l_step);


--log(l_prc,l_step || 'p_from_date =' || p_from_date || ',p_to_date=' || p_to_date
--                 || ',p_route_type=' || p_route_type || ',p_route_shift=' || p_route_shift
--                 || ',p_route_number=' || p_route_number || ',p_vendor_id=' || p_vendor_id
--                 || ',p_generate_invoice=' || p_generate_invoice);

if g_vendor_id is not null then
   select to_number(segment1) into l_vendor_number from ap_suppliers where vendor_id = g_vendor_id;
end if;

update bmlcustm2.tr_trip_hdr tr
   set calc_request_id            = g_conc_request_id
      ,payment_amount             = case when tr.calc_is_amount_updated = 'Y' then payment_amount 
                                         else null end
      ,calc_payable_fuel_price    = null
      ,calc_status                = 'NEW'
      ,calc_error_msg             = ''
      ,tr.ebs_ap_invoice_id       = -9
 where tr.ROUTE_TYPE              = g_route_type
   AND NVL(tr.ROUTE_SHIFT,'$##$') = NVL(g_route_Shift,NVL(tr.ROUTE_SHIFT,'$##$'))
   AND NVL(tr.parent_route_number,'X') = NVL(g_route_number,NVL(tr.parent_route_number,'X'))
   AND (NVL(tr.transporter_id,-1)  = NVL(l_vendor_number,NVL(tr.transporter_id,-1))
        or
       (NVL(tr.transporter_id,-1)  = NVL(g_vendor_id,NVL(tr.transporter_id,-1))))
   and trunc(tr.schedule_date) between g_from_date and g_to_date
   and nvl(tr.ebs_ap_invoice_id,-9) = -9
   and nvl(tr.calc_status,'NEW') in ('NEW','ERROR','CALCULATED') 
   and exists --Added by Subodh on 01-FEB-2024
   (select 1
          from bmlcustm2.tr_dispship_hdr
         where report_date = trunc(tr.schedule_date)     --'26-FEB-2022'
           and shift       = tr.route_shift   --'M'
             and NVL(DISPATCH_STATUS,'N') = 'Y'
           and route_no    = tr.route_number);

l_rowcount := sql%rowcount;

log(l_prc,'Rows picked = ' || l_rowcount);

if l_rowcount = 0 then
   l_return_msg := 'NO DATA TO PROCESS!!';
   raise l_stop;
end if;


for route_rec in (select distinct poh.po_header_id,tr.trip_trx_id,poh.segment1 po_number
                        --,MAX(fnd_date.canonical_to_date(pol.attribute1)) bpa_start_date
                        --,MAX(fnd_date.canonical_to_date(pol.attribute2)) bpa_end_date
                    from bmlcustm2.tr_trip_hdr tr
                        ,po_headers_all poh
                        ,po_lines_all   pol   
                   where pol.po_header_id       = poh.po_header_id
                    -- and pol.vendor_product_num = tr.parent_route_number
                     and pol.vendor_product_num = tr.route_number --Added by Subodh on 08-JUL-2024
                     and tr.schedule_date between fnd_date.canonical_to_date(pol.attribute1) and fnd_date.canonical_to_date(pol.attribute2) 
                     and tr.calc_request_id     = g_conc_request_id
                     and tr.ebs_bpa_po_hdr_id is null
                       --Added by Subodh on 30-JAN-2024 to Exclude not dispathed routes
   and exists 
   (select 1
          from bmlcustm2.tr_dispship_hdr
         where report_date = trunc(tr.schedule_date)     --'26-FEB-2022'
           and shift       = tr.route_shift   --'M'
           and route_no    = tr.route_number
            and NVL(DISPATCH_STATUS,'N') = 'Y'
           )
     ---Added by Subodh on 02-JUL-2024 to pick only Active BPAs
    and exists
  (select 1 from po_headers_all pha 
   where poh.po_header_id = pha.po_header_id 
 and NVL(pha.CLOSED_CODE,'OPEN'  )= 'OPEN'
   and NVL(pha.CLOSED_DATE,trunc(sysdate+1)) >= trunc(sysdate) 
   and NVL(pha.CANCEL_FLAG,'N')='N' )        
           
           
                     )
loop
    update bmlcustm2.tr_trip_hdr
       set ebs_bpa_po_hdr_id = route_rec.po_header_id
          ,ebs_bpa_po_number = route_rec.po_number
     where trip_trx_id       = route_rec.trip_trx_id;
end loop;


commit;

  --loop trip_header
  l_step := 'LOOP_TRIP: ';
  for trip_rec in trip_cur()
  loop
    begin
      --
      l_step                        := 'LOOPING TRIP_TRX_ID:' || trip_rec.trip_trx_id || ': ';
      l_return_msg                  := '';
      --
      l_ds_hdr_id                   := null;
      l_tcd_count                   := 0;
      l_qty_in_kgs                  := 0;
      l_qty_in_ltr                  := 0;
      l_acctual_total_qty           := 0;
      l_total_trip_amount           := 0;
      l_calc_details                := '';


      if nvl(trip_rec.bpa_rate_per_kgs_ltr,0) = 0 then
         l_return_msg := 'No Rate Per KGS/LTR on BPA/PO.';
         raise l_skip;
      end if;

      l_step := 'START_AMOUNT_CALC: ';
      begin
        select ds_hdr_id
          into l_ds_hdr_id
          from bmlcustm2.tr_dispship_hdr
         where report_date = trip_rec.trip_date     --'26-FEB-2022'
           and shift       = trip_rec.route_shift   --'M'
           and route_no    = trip_rec.route_number; --'BTCD773'
      exception 
        when no_data_found then
          l_ds_hdr_id := null;
        when others then
          l_return_msg := 'Error while deriving tcd data. ' || substr(sqlerrm,1,200);
          raise l_skip;
      end;

      log(l_prc,'Date = ' || trip_rec.trip_date || ', l_ds_hdr_id =' || l_ds_hdr_id);

      if l_ds_hdr_id is null then
         l_return_msg := 'No data found.';
         raise l_skip;
      end if;

      for tcd_rec in (select category_name,converted_uom,round(sum(converted_qty),4) converted_qty
                        from (
                              select case when nvl(category,'-1') like 'MILK _ CURD' and nvl(sub_category,'-1')  like '%CURD%' then 'CURD' else 'MILK' end category_name
                                    ,zl.converted_uom,zl.converted_qty
                                from xxbml_tpt_tcd_dispship_v zl
                               where 1=1
                                 and (    (      nvl(category,'-1')      like 'MILK _ CURD'
                                            and (nvl(sub_category,'-1')  like '%MILK%' or nvl(sub_category,'-1')  like '%CURD%'))
                                       or (      nvl(category,'-1')         = 'MILK PRODUCT'
                                            and (nvl(sub_category,'-1')   like '%DESI%'))
                                     )
                                 and zl.ds_hdr_id=l_ds_hdr_id
                              ) z
                       group by category_name,converted_uom
                     )
      loop
          l_tcd_count := l_tcd_count + 1;
          if    tcd_rec.converted_uom  =  'LTR' then
                l_qty_in_ltr          :=  tcd_rec.converted_qty;
          elsif tcd_rec.converted_uom  =  'KGS' then
                l_qty_in_kgs          :=  tcd_rec.converted_qty;
          end if;

          if l_calc_details is null then
             l_calc_details := tcd_rec.converted_qty || '(' || tcd_rec.converted_uom || ')';
          else
             l_calc_details := l_calc_details || '; ' || tcd_rec.converted_qty || '(' || tcd_rec.converted_uom || ')';
          end if;
      end loop;

      if l_tcd_count = 0 then
         l_return_msg := 'No TCD Lines data.';
         raise l_skip;
      end if;

      l_total_trip_amount := (l_qty_in_ltr * trip_rec.bpa_rate_per_kgs_ltr) +
                             (l_qty_in_kgs * trip_rec.bpa_rate_per_kgs_ltr);

      l_acctual_total_qty := l_qty_in_ltr + l_qty_in_kgs;
      
      --FND_FILE.put_line(FND_FILE.log,'l_qty_in_ltr : l_qty_in_kgs : l_acctual_total_qty : round(l_acctual_total_qty)'|| l_qty_in_ltr ||':'||l_qty_in_kgs||':'||l_acctual_total_qty||':'||round(l_acctual_total_qty,2));

      l_step := 'AFTER_TOTAL_TRIP_AMT_CALC: ';

      update tr_trip_hdr
         set payment_amount          = case when calc_is_amount_updated = 'Y' then payment_amount 
                                         --else l_total_trip_amount end
                                        -- else round(l_total_trip_amount,2) end --Added by Subodh on 26-MAR-2024 as requested
										  else trunc(l_total_trip_amount,2) end  --Added by Subodh on 04-NOV-2025 as requested
            ,acctual_total_qty       = round(l_acctual_total_qty,2)
            --,acctual_total_qty1       = round(l_acctual_total_qty,2)
			 ,acctual_total_qty1       = trunc(l_acctual_total_qty,2) --Added by Subodh on 04-NOV-2025 as requested
            ,calc_status             = 'CALCULATED'
            ,calc_error_msg          = ''
            ,calc_details            = l_calc_details
       where trip_trx_id             = trip_rec.trip_trx_id;

      l_step := 'CALC_AMOUNT_UPDATED: ';
      --
      commit;

    exception
      when l_skip then
        rollback;
        update tr_trip_hdr tr
           set tr.calc_status    = 'ERROR'
              ,tr.calc_error_msg = l_return_msg
         where trip_trx_id       = trip_rec.trip_trx_id;
      when others then
        rollback;
        l_return_msg := l_step || substr(sqlerrm,1,200);
        update tr_trip_hdr tr
           set tr.calc_status    = 'ERROR'
              ,tr.calc_error_msg = l_return_msg
         where trip_trx_id       = trip_rec.trip_trx_id;
    end;
    --
    commit;

  end loop;  --for trip_rec
  --
  if g_generate_invoice = 'Y' then
     ap_invoices_interface_insert(g_conc_request_id
                                 ,l_return_status
                                 ,l_return_msg);
  else
      update tr_trip_hdr tr
         set tr.ebs_ap_invoice_id = null
            ,tr.calc_status       = case when tr.calc_status = 'ERROR' then 'ERROR' else 'NEW' end
       where tr.calc_request_id   = g_conc_request_id
         and tr.ebs_ap_invoice_id = -9;
  end if;

  write_output_tcd();

log(l_prc, ' END');
exception
    when l_stop then
       x_errcode := 2;
       x_errbuf  := ' END ERROR L_STOP: ' || l_return_msg;
       log(l_prc,x_errbuf); 
       output(l_return_msg);      
    when l_skip then
       x_errcode := 2;
       x_errbuf  := ' END ERROR L_SKIP: ' || l_return_msg;
       log(l_prc,x_errbuf);
    when others then
       x_errcode := 2;
       x_errbuf  := 'END ERROR: ' || l_step || substr(sqlerrm,1,250);
       log(l_prc, x_errbuf);
end billing_tcd;


procedure billing_dtc(x_errbuf            out varchar2
                     ,x_errcode           out number)
as
l_prc                      varchar2(50) := 'billing_dtc';
l_step                     varchar2(100);
l_return_status            varchar2(1);
l_return_msg               varchar2(4000);
l_skip                     exception;
l_stop                     exception;
l_vendor_number            number;
--
l_payable_diesel_price        number;
l_payable_diesel_price_per_km number;

l_revised_diesel_price_per_km number;

l_total_trip_kilometers       number;
l_total_trip_amount           number;
--
l_rowcount                    number := 0;

cursor trip_cur --(c_route_type      in varchar2
                --,c_route_Shift     in varchar2
                --,c_route_number    in varchar2
                --,c_vendor_id       in number
                --,c_from_date       in date
                --,c_to_date         in date
                --,c_conc_request_id in number)
is
select tr.route_type
      ,trunc(tr.schedule_date) trip_date
      ,tr.parent_route_number route_number
      ,tr.route_shift
      ,aps.segment1 vendor_number
      ,aps.vendor_name
      ,aps.vendor_id
      ,tr.ebs_bpa_po_number po_number
      ,trunc(to_date(pol.attribute1,'YYYY/MM/DD HH24:MI:SS')) po_date
      ,nvl(tr.calc_pay_type,pol.unit_meas_lookup_code) calc_pay_type
      ,pol.unit_price bpa_rate_per_pay_type
      ,(select value
          from TR_FUEL_RATE_CHART
         where fuel_type = nvl(tv.fuel_type,tr.calc_veh_fuel_type) --'Diesel'
           and trunc(effective_date) = trunc(to_date(pol.attribute1,'YYYY/MM/DD HH24:MI:SS'))
           and trunc(tr.schedule_date) between trunc(to_date(pol.attribute1,'YYYY/MM/DD HH24:MI:SS')) and trunc(to_date(pol.attribute2,'YYYY/MM/DD HH24:MI:SS'))
           --and trunc(effective_date) between trunc(to_date(pol.attribute1,'YYYY/MM/DD HH24:MI:SS')) and trunc(to_date(pol.attribute2,'YYYY/MM/DD HH24:MI:SS'))  --Added by Subodh on 21-NOV-2022 
           and rownum = 1) po_startdate_fuel_rate
      ,(select value
          from TR_FUEL_RATE_CHART
         where fuel_type = nvl(tv.fuel_type,tr.calc_veh_fuel_type) --'Diesel'
           and trunc(effective_date) = trunc(tr.schedule_date)
           and rownum = 1) trip_fuel_rate
     --,(select pol.quantity
     --    from po_lines_all pol
     --   where pol.po_header_id = poh.po_header_id
     --     and rownum = 1) bpa_quantity
     --,(select (select uom_code from MTL_UNITS_OF_MEASURE where unit_of_measure=pol.unit_meas_lookup_code)
     --    from po_lines_all pol
     --   where pol.po_header_id = poh.po_header_id
     --     and rownum = 1) bpa_rate_type
     ,nvl(tr.calc_veh_mileage,tv.mileage) vehicle_mileage
     --,tr.payment_amount                trip_rate
     ,tr.est_total_qty                 trip_est_capacity_qty
     ,nvl(tv.capacity,tr.capacity_qty) vechicle_capacity_qty
     ,tr.rt_dist_in_km                 trip_in_kilometers
     ,nvl(tr.calc_rate_per_pay_type,0) calc_rate_per_pay_type
     ,to_number(case when pol.price_type_lookup_code = 'FIXED' then '0'
                     else to_char(nvl(tr.calc_extra_rate_per_pay_type,0))
                 end) calc_extra_rate_per_pay_type
     ,tr.trip_trx_id
 from  bmlcustm2.tr_trip_hdr tr
      ,po_headers_all        poh
      ,po_lines_all          pol
      ,ap_suppliers          aps
      ,bmlcustm2.tr_vehicle  tv
 where 1=1
   --AND tr.route_status='APPROVED'
   and pol.po_header_id           = poh.po_header_id
   and pol.vendor_product_num     = tr.parent_route_number
   and poh.po_header_id           = tr.ebs_bpa_po_hdr_id
   --and tr.transporter_id          = aps.segment1
   and poh.vendor_id              = aps.vendor_id
   and tr.parent_route_number  not like   'A%'  --do not calculate for adhoc routes, as amomunt is entered manually for each adhoc trip
   and trunc(tr.schedule_date) between trunc(to_date(pol.attribute1,'YYYY/MM/DD HH24:MI:SS'))
                                   and trunc(to_date(pol.attribute2,'YYYY/MM/DD HH24:MI:SS'))
   --and poh.segment1='30404'
   and poh.type_lookup_code       = 'BLANKET'
   and tr.vehicle_id              = tv.vehicle_id(+)
   --parameters
   --AND NVL(tr.ROUTE_TYPE,'$##$')  = NVL(c_route_type, NVL(tr.ROUTE_TYPE, '$##$'))
   --AND NVL(tr.ROUTE_SHIFT,'$##$') = NVL(c_route_Shift,NVL(tr.ROUTE_SHIFT,'$##$'))
   --AND NVL(tr.ROUTE_NUMBER,'X')   = NVL(c_route_number,NVL(tr.ROUTE_NUMBER,'X'))
   --AND NVL(aps.vendor_id,-1)      = NVL(c_vendor_id,NVL(aps.vendor_id,-1))
   --and trunc(tr.schedule_date) between c_from_date and c_to_date
   and tr.calc_request_id         = g_conc_request_id
    ---Added by Subodh on 18-NOV-2022 to pick only Active BPAs
   and exists
  (select 1 from po_headers_all pha 
   where tr.ebs_bpa_po_hdr_id = pha.po_header_id 
 and NVL(pha.CLOSED_CODE,'OPEN'  )= 'OPEN'
   and NVL(pha.CLOSED_DATE,trunc(sysdate+1)) >= trunc(sysdate) 
   and NVL(pha.CANCEL_FLAG,'N')='N' ) 
    --Added by Subodh on 02-JUL-2024 to Exclude not dispathed routes
   and exists 
   (select 1
          from bmlcustm2.tr_dispship_hdr
         where report_date = trunc(tr.schedule_date)     --'26-FEB-2022'
           and shift       = tr.route_shift   --'M'
            and NVL(DISPATCH_STATUS,'N') = 'Y'
           and route_no    = tr.route_number)
   
 order by tr.schedule_date;

--cursor get_km_cur
--is
--select shift,sum(nvl(quantity_received,0)) qty_in_kg
--  from bmlcustm2.rm_shipping_hdr
-- where transaction_code = 'BMC'
--   and route_no         = trip_rec.route_number --'BMC-21'
--   and rout_trx_num     = trip_rec.trip_trx_id  --321990
--   and shipped_date     = trip_rec.trip_date    --'18-APR-2022'
-- group by shift;

begin
x_errcode        := 0;
l_return_status  := 'S';
l_step           := 'START: ';
log(l_prc,l_step);


--log(l_prc,l_step || 'p_from_date =' || p_from_date || ',p_to_date=' || p_to_date
--                 || ',p_route_type=' || p_route_type || ',p_route_shift=' || p_route_shift
--                 || ',p_route_number=' || p_route_number || ',p_vendor_id=' || p_vendor_id
--                 || ',p_generate_invoice=' || p_generate_invoice);

if g_vendor_id is not null then
   select to_number(segment1) into l_vendor_number from ap_suppliers where vendor_id = g_vendor_id;
end if;

update bmlcustm2.tr_trip_hdr tr
   set calc_request_id            = g_conc_request_id
      ,payment_amount             = case when tr.parent_route_number like 'A%' then payment_amount 
                                         when tr.calc_is_amount_updated = 'Y' then payment_amount 
                                         else null end
      ,calc_payable_fuel_price    = null
      ,calc_status                = case when tr.parent_route_number like 'A%' then 'CALCULATED' else 'NEW' end
      ,calc_error_msg             = ''
      ,tr.ebs_ap_invoice_id       = -9
 where tr.ROUTE_TYPE              = g_route_type
   AND NVL(tr.ROUTE_SHIFT,'$##$') = NVL(g_route_Shift,NVL(tr.ROUTE_SHIFT,'$##$'))
   AND NVL(tr.parent_route_number,'X')   = NVL(g_route_number,NVL(tr.parent_route_number,'X'))
   AND (NVL(tr.transporter_id,-1) = NVL(l_vendor_number,NVL(tr.transporter_id,-1))
        or
       (NVL(tr.transporter_id,-1) = NVL(g_vendor_id,NVL(tr.transporter_id,-1))))
   and trunc(tr.schedule_date) between g_from_date and g_to_date
   and nvl(tr.ebs_ap_invoice_id,-9) = -9
   and nvl(tr.calc_status,'NEW') in ('NEW','ERROR','CALCULATED')
   --Added by Subodh on 06-FEB-2024 to Exclude not dispathed routes
   and exists 
   (select 1
          from bmlcustm2.tr_dispship_hdr
         where report_date = trunc(tr.schedule_date)     --'26-FEB-2022'
           and shift       = tr.route_shift   --'M'
           and route_no    = tr.route_number
           and NVL(DISPATCH_STATUS,'N') = 'Y')
   ;

l_rowcount := sql%rowcount;

log(l_prc,'Rows picked = ' || l_rowcount);

if l_rowcount = 0 then
   l_return_msg := 'NO DATA TO PROCESS!!';
   raise l_stop;
end if;

for route_rec in (select distinct poh.po_header_id,tr.trip_trx_id,poh.segment1 po_number
                        --,MAX(fnd_date.canonical_to_date(pol.attribute1)) bpa_start_date
                        --,MAX(fnd_date.canonical_to_date(pol.attribute2)) bpa_end_date
                    from bmlcustm2.tr_trip_hdr tr
                        ,po_headers_all poh
                        ,po_lines_all   pol   
                   where pol.po_header_id       = poh.po_header_id
                     and pol.vendor_product_num = tr.parent_route_number
                     and tr.schedule_date between fnd_date.canonical_to_date(pol.attribute1) and fnd_date.canonical_to_date(pol.attribute2) 
                     and tr.calc_request_id     = g_conc_request_id
                     and tr.ebs_bpa_po_hdr_id is null
                     and poh.type_lookup_code       = 'BLANKET' --Added by Subodh on 30-NOV-2022
                      ---Added by Subodh on 17-NOV-2022 to pick only Active BPAs
   and exists
  (select 1 from po_headers_all pha 
   where poh.po_header_id = pha.po_header_id 
  and NVL(pha.CLOSED_CODE,'OPEN'  )= 'OPEN'
   and NVL(pha.CLOSED_DATE,trunc(sysdate+1)) >= trunc(sysdate) 
   and NVL(pha.CANCEL_FLAG,'N')='N' )
    --Added by Subodh on 02-JUL-2024 to Exclude not dispathed routes
   and exists 
   (select 1
          from bmlcustm2.tr_dispship_hdr
         where report_date = trunc(tr.schedule_date)     --'26-FEB-2022'
           and shift       = tr.route_shift   --'M'
            and NVL(DISPATCH_STATUS,'N') = 'Y'
           and route_no    = tr.route_number)
   
   
                     )
loop
    log(l_prc,'in loop: route_rec.trip_trx_id=' || route_rec.trip_trx_id);
    update bmlcustm2.tr_trip_hdr
       set ebs_bpa_po_hdr_id = route_rec.po_header_id
          ,ebs_bpa_po_number = route_rec.po_number
     where trip_trx_id       = route_rec.trip_trx_id;
end loop;
log(l_prc,'After loop');

commit;

  --loop trip_header
  l_step := 'LOOP_TRIP: ';
  for trip_rec in trip_cur 
                         --(g_route_type,g_route_Shift,g_route_number,g_vendor_id,g_from_date,g_to_date,g_conc_request_id)
  loop
    begin
      --
      l_step                        := 'LOOPING TRIP_TRX_ID:' || trip_rec.trip_trx_id || ': ';
      l_return_msg                  := '';
      --
      l_payable_diesel_price        := 0;
      l_payable_diesel_price_per_km := 0;
      l_revised_diesel_price_per_km := 0;
      l_total_trip_kilometers       := 0;
      l_total_trip_amount           := 0;
      l_return_msg                  := '';

--FND_FILE.put_line (APPS.FND_FILE.LOG,'1');
      --Validate PO NUMBER
      l_step := 'VAL_PO_NUMBER: ';
      if trip_rec.po_number is null then
         l_return_msg := 'H: No BPA/PO Number on Trip.';
         raise l_skip;
      end if;
--FND_FILE.put_line (APPS.FND_FILE.LOG,'2');
      --Validate PO FUEL RATE
      l_step := 'VAL_PO_FUEL_RATE: ';
      if nvl(trip_rec.po_startdate_fuel_rate,0) = 0 then
         l_return_msg := 'H: No Fuel Rate for PO Start Date: ' || to_char(trip_rec.po_date,'DD-MON-RRRR');
         raise l_skip;
      end if;
--FND_FILE.put_line (APPS.FND_FILE.LOG,'3');
      --Validate PO RATE PER KILOMETER
      --l_step := 'VAL_PO_RATE_PER_KM: ';
      --if nvl(trip_rec.bpa_rate_per_pay_type,0) = 0 then
      --   l_return_msg := 'H: No Rate per KM in PO.';
      --   raise l_skip;
      --end if;

      --Validate CALC_PAYMENT_TYPE
      l_step := 'VAL_CALC_PAY_TYPE: ';
      if trip_rec.calc_pay_type is null then
         l_return_msg := 'H: No Payment Type on Trip.';
         raise l_skip;
      end if;
--FND_FILE.put_line (APPS.FND_FILE.LOG,'4');
      --Validate TRIP FUEL RATE
      l_step := 'VAL_TRIP_FUEL_RATE: ';
      if nvl(trip_rec.trip_fuel_rate,0) = 0 then
         l_return_msg := 'L: No Fuel Rate for Trip Date: ' || to_char(trip_rec.trip_date,'DD-MON-RRRR');
         raise l_skip;
      end if;
--FND_FILE.put_line (APPS.FND_FILE.LOG,'5');
      --Validate VEHICLE MIELAGE
      l_step := 'VAL_VEHICLE_MIELAGE: ';
      if nvl(trip_rec.vehicle_mileage,0) = 0 then
         l_return_msg := 'L: No Vehicle Milage.';
         raise l_skip;
      end if;

      --Validate RATE_PER_PAYMENT_TYPE
      l_step := 'VAL_RATE_PER_PAYMENT_TYPE: ';
      if nvl(trip_rec.bpa_rate_per_pay_type,nvl(trip_rec.calc_rate_per_pay_type,0)) = 0 then
         l_return_msg := 'L: No Rate Per Payment Type.';
         raise l_skip;
      end if;

--FND_FILE.put_line (APPS.FND_FILE.LOG,'6');
      l_step := 'START_AMOUNT_CALC: ';
      l_payable_diesel_price        := trip_rec.trip_fuel_rate - trip_rec.po_startdate_fuel_rate;
      log(l_prc,'**l_payable_diesel_price   = ' || l_payable_diesel_price);

      l_payable_diesel_price_per_km := round(((trip_rec.trip_fuel_rate - trip_rec.po_startdate_fuel_rate)/trip_rec.vehicle_mileage),2); --Added rounding by Subodh on 25-MAR-2024 as requested
     -- l_payable_diesel_price_per_km := ((trip_rec.trip_fuel_rate - trip_rec.po_startdate_fuel_rate)/trip_rec.vehicle_mileage); --Removed rounding by Subodh on 30-NOV-2022 as rounding will be done on payable amount as per client
      log(l_prc,'**l_payable_diesel_price_per_km   = ' || l_payable_diesel_price_per_km);

      l_revised_diesel_price_per_km := trip_rec.trip_in_kilometers * l_payable_diesel_price_per_km;
      l_step := 'AFTER_BASIC_CALC: ';
      log(l_prc,'**l_revised_diesel_price_per_km   = ' || l_revised_diesel_price_per_km);

      l_total_trip_amount       :=   nvl(trip_rec.bpa_rate_per_pay_type,nvl(trip_rec.calc_rate_per_pay_type,0))
                                   + nvl(trip_rec.calc_extra_rate_per_pay_type,0)
                                   --+ ROUND(l_revised_diesel_price_per_km,3);--Added by Subodh on 30-NOV-2022
                                    --+ ROUND(l_revised_diesel_price_per_km,2);--Added by Subodh on 26-MAR-2024
									+ TRUNC(l_revised_diesel_price_per_km,2);--Added by Subodh on 05-SEP-2025 as Per client

      l_step := 'AFTER_TOTAL_TRIP_AMT_CALC: ';
      log(l_prc,'**l_total_trip_amount   = ' || l_total_trip_amount);

      --log start
      log(l_prc,'LOG START ******* TRIP_DATE = ' || trip_rec.trip_date || '  *******');
      log(l_prc,'trip_rec.trip_fuel_rate         = ' || trip_rec.trip_fuel_rate);
      log(l_prc,'trip_rec.po_startdate_fuel_rate = ' || trip_rec.po_startdate_fuel_rate);
      log(l_prc,'l_payable_diesel_price          = ' || l_payable_diesel_price);
      --
      log(l_prc,'trip_rec.vehicle_mileage        = ' || trip_rec.vehicle_mileage);
      log(l_prc,'l_payable_diesel_price_per_km   = ' || l_payable_diesel_price_per_km);
      --
      log(l_prc,'trip_rec.bpa_rate_per_pay_type  = ' || trip_rec.bpa_rate_per_pay_type);
      log(l_prc,'l_revised_diesel_price_per_km   = ' || l_revised_diesel_price_per_km);
      --
      log(l_prc,'trip_rec.trip_in_kilometers     = ' || trip_rec.trip_in_kilometers);
      log(l_prc,'l_total_trip_amount             = ' || l_total_trip_amount);
      log(l_prc,'LOG END   ******* TRIP_DATE = ' || trip_rec.trip_date || '  *******');
      --log end

      update tr_trip_hdr
         set payment_amount          = case when parent_route_number like 'A%' then payment_amount 
                                         when calc_is_amount_updated = 'Y' then payment_amount 
                                         else l_total_trip_amount end
           --,calc_payable_fuel_price = l_revised_diesel_price_per_km
            --,calc_payable_fuel_price =ROUND(l_revised_diesel_price_per_km,2) --Added by Subodh on 26-MAR-2024
			,calc_payable_fuel_price =TRUNC(l_revised_diesel_price_per_km,2) --Added by Subodh on 05-SEP-2025 as Per client
            ,calc_status             = 'CALCULATED'
            ,calc_error_msg          = ''
       where trip_trx_id             = trip_rec.trip_trx_id;

      l_step := 'CALC_AMOUNT_UPDATED: ';
      --
      commit;

       FND_FILE.put_line (APPS.FND_FILE.LOG,'8 '||trip_rec.trip_trx_id);

    exception
      when l_skip then
        rollback;
        update tr_trip_hdr tr
           set tr.calc_status    = 'ERROR'
              ,tr.calc_error_msg = l_return_msg
         where trip_trx_id       = trip_rec.trip_trx_id;
      when others then
        rollback;
        l_return_msg := l_step || substr(sqlerrm,1,200);
        update tr_trip_hdr tr
           set tr.calc_status    = 'ERROR'
              ,tr.calc_error_msg = l_return_msg
         where trip_trx_id       = trip_rec.trip_trx_id;
    end;
    --
  end loop;  --for trip_rec
  --
  if g_generate_invoice = 'Y' then
     ap_invoices_interface_insert(g_conc_request_id
                                 ,l_return_status
                                 ,l_return_msg);
  else
      update tr_trip_hdr tr
         set tr.ebs_ap_invoice_id = null
            ,tr.calc_status       = case when tr.calc_status = 'ERROR' then 'ERROR' else 'NEW' end
       where tr.calc_request_id   = g_conc_request_id
         and tr.ebs_ap_invoice_id = -9;
  end if;

  if g_route_number like 'A%' then
     write_output_dtc_adhoc();
  else
     write_output_dtc();
  end if;

log(l_prc, ' END');
exception
    when l_stop then
       x_errcode := 2;
       x_errbuf  := ' END ERROR L_STOP: ' || l_return_msg;
       log(l_prc,x_errbuf); 
       output(l_return_msg);      
    when l_skip then
       x_errcode := 2;
       x_errbuf  := ' END ERROR L_SKIP: ' || l_return_msg;
       log(l_prc,x_errbuf);
    when others then
       x_errcode := 2;
       x_errbuf  := 'END ERROR: ' || l_step || substr(sqlerrm,1,250);
       log(l_prc, x_errbuf);
end billing_dtc;



procedure billing_ptc(x_errbuf            out varchar2
                     ,x_errcode           out number)
as
l_prc                      varchar2(50) := 'billing_ptc';
l_step                     varchar2(100);
l_return_status            varchar2(1);
l_return_msg               varchar2(4000);
l_skip                     exception;
l_stop                     exception;
l_vendor_number            number;
--
l_payable_diesel_price        number;
l_payable_diesel_price_per_km number;

l_revised_diesel_price_per_km number;

l_total_trip_kilometers       number;
l_total_trip_amount           number;
--
l_rowcount                    number := 0;
l_rowcount1                    number := 0;

cursor trip_cur --(c_route_type      in varchar2
                --,c_route_Shift     in varchar2
                --,c_route_number    in varchar2
                --,c_vendor_id       in number
                --,c_from_date       in date
                --,c_to_date         in date
                --,c_conc_request_id in number)
is
select tr.route_type
      ,trunc(tr.schedule_date) trip_date
      ,tr.route_number
      ,tr.route_shift
      ,aps.segment1 vendor_number
      ,aps.vendor_name
      ,aps.vendor_id
      ,tr.ebs_bpa_po_number po_number
      ,trunc(to_date(pol.attribute1,'YYYY/MM/DD HH24:MI:SS')) po_date
      ,nvl(tr.calc_pay_type,pol.unit_meas_lookup_code) calc_pay_type
      ,pol.unit_price bpa_rate_per_pay_type
      ,(select value
          from TR_FUEL_RATE_CHART
         where fuel_type = nvl(tv.fuel_type,tr.calc_veh_fuel_type) --'Diesel'
           and trunc(effective_date) = trunc(to_date(pol.attribute1,'YYYY/MM/DD HH24:MI:SS'))
           and rownum = 1) po_startdate_fuel_rate
      ,(select value
          from TR_FUEL_RATE_CHART
         where fuel_type = nvl(tv.fuel_type,tr.calc_veh_fuel_type) --'Diesel'
           and trunc(effective_date) = trunc(tr.schedule_date)
           and rownum = 1) trip_fuel_rate
     --,(select pol.quantity
     --    from po_lines_all pol
     --   where pol.po_header_id = poh.po_header_id
     --     and rownum = 1) bpa_quantity
     --,(select (select uom_code from MTL_UNITS_OF_MEASURE where unit_of_measure=pol.unit_meas_lookup_code)
     --    from po_lines_all pol
     --   where pol.po_header_id = poh.po_header_id
     --     and rownum = 1) bpa_rate_type
     ,nvl(tv.mileage,tr.calc_veh_mileage) vehicle_mileage
     --,tr.payment_amount                trip_rate
     ,tr.est_total_qty                 trip_est_capacity_qty
     ,nvl(tv.capacity,tr.capacity_qty) vechicle_capacity_qty
     ,tr.rt_dist_in_km                 trip_in_kilometers
     ,nvl(tr.calc_rate_per_pay_type,0) calc_rate_per_pay_type
     ,to_number(case when pol.price_type_lookup_code = 'FIXED' then '0'
                     else to_char(nvl(tr.calc_extra_rate_per_pay_type,0))
                 end) calc_extra_rate_per_pay_type
     ,tr.trip_trx_id
 from  bmlcustm2.tr_trip_hdr tr
      ,po_headers_all        poh
      ,po_lines_all          pol
      ,ap_suppliers          aps
      ,bmlcustm2.tr_vehicle  tv
 where 1=1
   --AND tr.route_status='APPROVED'
   and pol.po_header_id           = poh.po_header_id
   and pol.vendor_product_num     = tr.route_number
   -- and pol.vendor_product_num     = tr.parent_route_number --Added by Subodh on 12-JUN-2024
   and poh.po_header_id           = tr.ebs_bpa_po_hdr_id
   --and tr.transporter_id          = aps.segment1
   and poh.vendor_id              = aps.vendor_id
   --and poh.segment1='30404'
   and poh.type_lookup_code       = 'BLANKET'
   and tr.vehicle_id              = tv.vehicle_id(+)
   --parameters
   --AND NVL(tr.ROUTE_TYPE,'$##$')  = NVL(c_route_type, NVL(tr.ROUTE_TYPE, '$##$'))
   --AND NVL(tr.ROUTE_SHIFT,'$##$') = NVL(c_route_Shift,NVL(tr.ROUTE_SHIFT,'$##$'))
   --AND NVL(tr.ROUTE_NUMBER,'X')   = NVL(c_route_number,NVL(tr.ROUTE_NUMBER,'X'))
   --AND NVL(aps.vendor_id,-1)      = NVL(c_vendor_id,NVL(aps.vendor_id,-1))
   --and trunc(tr.schedule_date) between c_from_date and c_to_date
   and trunc(tr.schedule_date) between trunc(to_date(pol.attribute1,'YYYY/MM/DD HH24:MI:SS'))
                                   and trunc(to_date(pol.attribute2,'YYYY/MM/DD HH24:MI:SS'))
   and tr.calc_request_id         = g_conc_request_id
    ---Added by Subodh on 07-MAR-2024 to pick only Active BPAs
   and exists
  (select 1 from po_headers_all pha 
   where poh.po_header_id = pha.po_header_id 
  and NVL(pha.CLOSED_CODE,'OPEN'  )= 'OPEN'
   and NVL(pha.CLOSED_DATE,trunc(sysdate+1)) >= trunc(sysdate) 
   and NVL(pha.CANCEL_FLAG,'N')='N' )
                     
 order by tr.schedule_date;

--cursor get_km_cur
--is
--select shift,sum(nvl(quantity_received,0)) qty_in_kg
--  from bmlcustm2.rm_shipping_hdr
-- where transaction_code = 'BMC'
--   and route_no         = trip_rec.route_number --'BMC-21'
--   and rout_trx_num     = trip_rec.trip_trx_id  --321990
--   and shipped_date     = trip_rec.trip_date    --'18-APR-2022'
-- group by shift;

begin
x_errcode        := 0;
l_return_status  := 'S';
l_step           := 'START: ';
log(l_prc,l_step);


--log(l_prc,l_step || 'p_from_date =' || p_from_date || ',p_to_date=' || p_to_date
--                 || ',p_route_type=' || p_route_type || ',p_route_shift=' || p_route_shift
--                 || ',p_route_number=' || p_route_number || ',p_vendor_id=' || p_vendor_id
--                 || ',p_generate_invoice=' || p_generate_invoice);

if g_vendor_id is not null then
   select to_number(segment1) into l_vendor_number from ap_suppliers where vendor_id = g_vendor_id;
end if;
log(l_prc,'Before Update');
l_rowcount :=0;
update bmlcustm2.tr_trip_hdr tr
   set calc_request_id            = g_conc_request_id
      ,payment_amount             = null
      ,calc_payable_fuel_price    = null
      ,calc_status                = 'NEW'
      ,calc_error_msg             = ''
      ,tr.ebs_ap_invoice_id       = -9
 where tr.ROUTE_TYPE              = g_route_type
   AND NVL(tr.ROUTE_SHIFT,'$##$') = NVL(g_route_Shift,NVL(tr.ROUTE_SHIFT,'$##$'))
   --AND NVL(tr.ROUTE_NUMBER,'X')   = NVL(g_route_number,NVL(tr.ROUTE_NUMBER,'X'))
      AND NVL(tr.parent_route_number,'X')   = NVL(g_route_number,NVL(tr.parent_route_number,'X')) --Added by Subodh on 06-FEB-2024 
   AND (NVL(tr.transporter_id,-1)  = NVL(l_vendor_number,NVL(tr.transporter_id,-1))
        or
       (NVL(tr.transporter_id,-1)  = NVL(g_vendor_id,NVL(tr.transporter_id,-1))))
   and trunc(tr.schedule_date) between g_from_date and g_to_date
   and nvl(tr.ebs_ap_invoice_id,-9) = -9
   and nvl(tr.calc_status,'NEW') in ('NEW','ERROR','CALCULATED')
   and tr.ROUTE_STATUS='ARRIVED' --Added by Subodh on 05-MAR-2024
     ;
--log(l_prc,'After Update '||sql%rowcount);
l_rowcount := sql%rowcount;
l_rowcount1 := sql%rowcount;

log(l_prc,'Rows picked = ' || l_rowcount1);

if l_rowcount1 = 0 then
   l_return_msg := 'NO DATA TO PROCESS!! ';
   raise l_stop;
end if;

for route_rec in (select distinct poh.po_header_id,tr.trip_trx_id,poh.segment1 po_number
                        --,MAX(fnd_date.canonical_to_date(pol.attribute1)) bpa_start_date
                        --,MAX(fnd_date.canonical_to_date(pol.attribute2)) bpa_end_date
                    from bmlcustm2.tr_trip_hdr tr
                        ,po_headers_all poh
                        ,po_lines_all   pol   
                   where pol.po_header_id       = poh.po_header_id
                     --and pol.vendor_product_num = tr.parent_route_number
                     and pol.vendor_product_num = tr.route_number --Added by Subodh on 06-FEB-2024 
                     -- and pol.vendor_product_num = tr.parent_route_number --Added by Subodh on 12-JUN-2024 
                     and tr.schedule_date between fnd_date.canonical_to_date(pol.attribute1) and fnd_date.canonical_to_date(pol.attribute2) 
                     and tr.calc_request_id     = g_conc_request_id
                     and tr.ebs_bpa_po_hdr_id is null
                      ---Added by Subodh on 07-MAR-2024 to pick only Active BPAs
   and exists
  (select 1 from po_headers_all pha 
   where poh.po_header_id = pha.po_header_id 
  and NVL(pha.CLOSED_CODE,'OPEN'  )= 'OPEN'
   and NVL(pha.CLOSED_DATE,trunc(sysdate+1)) >= trunc(sysdate) 
   and NVL(pha.CANCEL_FLAG,'N')='N' )
                     )
loop
    update bmlcustm2.tr_trip_hdr
       set ebs_bpa_po_hdr_id = route_rec.po_header_id
       ,ebs_bpa_po_number = route_rec.po_number --Added by Subodh on 06-FEB-2024
     where trip_trx_id       = route_rec.trip_trx_id;
end loop;

commit;

  --loop trip_header
  l_step := 'LOOP_TRIP: ';
  for trip_rec in trip_cur --(g_route_type
                           --,g_route_Shift
                           --,g_route_number
                           --,g_vendor_id
                           --,g_from_date
                           --,g_to_date
                           --,g_conc_request_id)
  loop
    begin
   -- FND_FILE.put_line (APPS.FND_FILE.LOG,'Inside trip_cur loop '||trip_rec.trip_trx_id);
      --
      l_step                        := 'LOOPING TRIP_TRX_ID:' || trip_rec.trip_trx_id || ': ';
      l_return_msg                  := '';
      --
      l_payable_diesel_price        := 0;
      l_payable_diesel_price_per_km := 0;
      l_revised_diesel_price_per_km := 0;
      l_total_trip_kilometers       := 0;
      l_total_trip_amount           := 0;
      l_return_msg                  := '';

--FND_FILE.put_line (APPS.FND_FILE.LOG,'1');
      --Validate PO NUMBER
      l_step := 'VAL_PO_NUMBER: ';
      if trip_rec.po_number is null then
         l_return_msg := 'H: No BPA/PO Number on Trip.';
         raise l_skip;
      end if;
--FND_FILE.put_line (APPS.FND_FILE.LOG,'2');
      --Validate PO FUEL RATE
      l_step := 'VAL_PO_FUEL_RATE: ';
      if nvl(trip_rec.po_startdate_fuel_rate,0) = 0 then
         l_return_msg := 'H: No Fuel Rate for PO Start Date: ' || to_char(trip_rec.po_date,'DD-MON-RRRR');
         raise l_skip;
      end if;
--FND_FILE.put_line (APPS.FND_FILE.LOG,'3');
      --Validate PO RATE PER KILOMETER
      --l_step := 'VAL_PO_RATE_PER_KM: ';
      --if nvl(trip_rec.bpa_rate_per_pay_type,0) = 0 then
      --   l_return_msg := 'H: No Rate per KM in PO.';
      --   raise l_skip;
      --end if;

      --Validate CALC_PAYMENT_TYPE
      l_step := 'VAL_CALC_PAY_TYPE: ';
      if trip_rec.calc_pay_type is null then
         l_return_msg := 'H: No Payment Type on Trip.';
         raise l_skip;
      end if;
--FND_FILE.put_line (APPS.FND_FILE.LOG,'4');
      --Validate TRIP FUEL RATE
      l_step := 'VAL_TRIP_FUEL_RATE: ';
      if nvl(trip_rec.trip_fuel_rate,0) = 0 then
         l_return_msg := 'L: No Fuel Rate for Trip Date: ' || to_char(trip_rec.trip_date,'DD-MON-RRRR');
         raise l_skip;
      end if;
--FND_FILE.put_line (APPS.FND_FILE.LOG,'5');
      --Validate VEHICLE MIELAGE
      l_step := 'VAL_VEHICLE_MIELAGE: ';
      if nvl(trip_rec.vehicle_mileage,0) = 0 then
         l_return_msg := 'L: No Vehicle Milage.';
         raise l_skip;
      end if;

      --Validate RATE_PER_PAYMENT_TYPE
      l_step := 'VAL_RATE_PER_PAYMENT_TYPE: ';
      if nvl(trip_rec.bpa_rate_per_pay_type,nvl(trip_rec.calc_rate_per_pay_type,0)) = 0 then
         l_return_msg := 'L: No Rate Per Payment Type.';
         raise l_skip;
      end if;

--FND_FILE.put_line (APPS.FND_FILE.LOG,'6');
      l_step := 'START_AMOUNT_CALC: ';
      l_payable_diesel_price        := trip_rec.trip_fuel_rate - trip_rec.po_startdate_fuel_rate;
      l_payable_diesel_price_per_km := round(((trip_rec.trip_fuel_rate - trip_rec.po_startdate_fuel_rate)/trip_rec.vehicle_mileage),2);

      l_revised_diesel_price_per_km := trip_rec.bpa_rate_per_pay_type + l_payable_diesel_price_per_km;
      l_step := 'AFTER_BASIC_CALC: ';

      if     trip_rec.calc_pay_type = 'TRIP' then

             l_total_trip_amount       :=   nvl(trip_rec.bpa_rate_per_pay_type,nvl(trip_rec.calc_rate_per_pay_type,0))
                                          + nvl(trip_rec.calc_extra_rate_per_pay_type,0);

      --elsif  trip_rec.calc_pay_type in ('KILOMETER','KILOGRAM') then
      elsif  upper(trip_rec.calc_pay_type) like 'KILO%' then

             l_total_trip_kilometers   := trip_rec.trip_in_kilometers;
             l_total_trip_amount       := TRUNC(l_revised_diesel_price_per_km,2) * l_total_trip_kilometers; --Trunc added by Subodh on 04-NOV-2025
      else
             l_total_trip_amount       := TRUNC(l_revised_diesel_price_per_km,2) * trip_rec.trip_in_kilometers; --Trunc added by Subodh on 04-NOV-2025
      end if;

      l_step := 'AFTER_TOTAL_TRIP_AMT_CALC: ';

      --log start
      log(l_prc,'LOG START ******* TRIP_DATE = ' || trip_rec.trip_date || '  *******');
      log(l_prc,'trip_rec.trip_fuel_rate         = ' || trip_rec.trip_fuel_rate);
      log(l_prc,'trip_rec.po_startdate_fuel_rate = ' || trip_rec.po_startdate_fuel_rate);
      log(l_prc,'l_payable_diesel_price          = ' || l_payable_diesel_price);
      --
      log(l_prc,'trip_rec.vehicle_mileage        = ' || trip_rec.vehicle_mileage);
      log(l_prc,'l_payable_diesel_price_per_km   = ' || l_payable_diesel_price_per_km);
      --
      log(l_prc,'trip_rec.bpa_rate_per_pay_type  = ' || trip_rec.bpa_rate_per_pay_type);
      log(l_prc,'l_revised_diesel_price_per_km   = ' || l_revised_diesel_price_per_km);
      --
      log(l_prc,'trip_rec.trip_in_kilometers     = ' || trip_rec.trip_in_kilometers);
      log(l_prc,'l_total_trip_amount             = ' || l_total_trip_amount);
      log(l_prc,'LOG END   ******* TRIP_DATE = ' || trip_rec.trip_date || '  *******');
      --log end

      update tr_trip_hdr
       --  set payment_amount          = round(l_total_trip_amount,2) --Added round by Subodh on 02-APR-2024 as requested by Bamul
		 set payment_amount          = trunc(l_total_trip_amount,2) --Added Trunc by Subodh on 04-NOV-2025 as requested by Bamul
            --,calc_payable_fuel_price = round(l_payable_diesel_price_per_km,2) --Added round by Subodh on 02-APR-2024 as requested by Bamul
			,calc_payable_fuel_price = trunc(l_payable_diesel_price_per_km,2)--Added Trunc by Subodh on 04-NOV-2025 as requested by Bamul
            --,calc_rate_km            = round(l_revised_diesel_price_per_km,2) --Added round by Subodh on 02-APR-2024 as requested by Bamul
			,calc_rate_km            = trunc(l_revised_diesel_price_per_km,2) --Added Trunc by Subodh on 04-NOV-2025 as requested by Bamul
            ,calc_status             = 'CALCULATED'
            ,calc_error_msg          = ''
       where trip_trx_id             = trip_rec.trip_trx_id;

      l_step := 'CALC_AMOUNT_UPDATED: ';
      --
      commit;

       FND_FILE.put_line (APPS.FND_FILE.LOG,'8 '||trip_rec.trip_trx_id);

    exception
      when l_skip then
        rollback;
        update tr_trip_hdr tr
           set tr.calc_status    = 'ERROR'
              ,tr.calc_error_msg = l_return_msg
         where trip_trx_id       = trip_rec.trip_trx_id;
      when others then
        rollback;
        l_return_msg := l_step || substr(sqlerrm,1,200);
        update tr_trip_hdr tr
           set tr.calc_status    = 'ERROR'
              ,tr.calc_error_msg = l_return_msg
         where trip_trx_id       = trip_rec.trip_trx_id;
    end;
    --
  end loop;  --for trip_rec
  commit;
  --
  if g_generate_invoice = 'Y' then
     null;
     --create ap invoice here
      ap_invoices_interface_insert(g_conc_request_id
                                 ,l_return_status
                                 ,l_return_msg);
  else
      update tr_trip_hdr tr
         set tr.ebs_ap_invoice_id = null
            ,tr.calc_status       = case when tr.calc_status = 'ERROR' then 'ERROR' else 'NEW' end
       where tr.calc_request_id   = g_conc_request_id
         and tr.ebs_ap_invoice_id = -9;
  end if;

  write_output();

log(l_prc, ' END');
exception
 when l_stop then
       x_errcode := 2;
       x_errbuf  := ' END ERROR L_STOP: ' || l_return_msg;
       log(l_prc,x_errbuf); 
       output(l_return_msg);  
    when l_skip then
       x_errcode := 2;
       x_errbuf  := ' END ERROR L_SKIP: ' || l_return_msg;
       log(l_prc,x_errbuf);
    when others then
       x_errcode := 2;
       x_errbuf  := 'END ERROR: ' || l_step || substr(sqlerrm,1,250);
       log(l_prc, x_errbuf);
end billing_ptc;



procedure billing_rate_based(x_errbuf            out varchar2
                            ,x_errcode           out number)
as
l_prc                      varchar2(50) := 'billing_rate_based';
l_step                     varchar2(100);
l_return_status            varchar2(1);
l_return_msg               varchar2(4000);
l_skip                     exception;
l_vendor_number            number;
--
l_payable_diesel_price        number;
l_payable_diesel_price_per_km number;

l_revised_diesel_price_per_km number;

l_total_trip_kilometers       number;
l_total_trip_amount           number;
--
l_cost_ltr                    number;
l_pro_cost                    number;
l_sum_vehicle_capacity        number;
l_sum_transport_qty           number;
l_amount                       number;
l_rowcount                    number := 0;
l_rowcount1                    number := 0;
l_stop                        exception;

cursor trip_cur (c_route_type      in varchar2
                ,c_route_Shift     in varchar2
                ,c_route_number    in varchar2
                ,c_vendor_id       in number
                ,c_from_date       in date
                ,c_to_date         in date
                ,c_conc_request_id in number)
is
select tr.route_type
      ,trunc(tr.schedule_date) trip_date
      ,tr.route_number
      ,tr.route_shift
      ,aps.segment1 vendor_number
      ,aps.vendor_name
      ,aps.vendor_id
      ,tr.ebs_bpa_po_number po_number
      ,poh.start_date po_date
      ,nvl(tr.calc_pay_type,pol.unit_meas_lookup_code) calc_pay_type
      ,pol.unit_price bpa_rate_per_pay_type
      ,(select value
          from TR_FUEL_RATE_CHART
         where fuel_type = nvl(tv.fuel_type,tr.calc_veh_fuel_type) --'Diesel'
           and trunc(effective_date) = trunc(to_date(pol.attribute1,'YYYY/MM/DD HH24:MI:SS'))
            and trunc(tr.schedule_date) between trunc(to_date(pol.attribute1,'YYYY/MM/DD HH24:MI:SS')) and trunc(to_date(pol.attribute2,'YYYY/MM/DD HH24:MI:SS'))  --Added by Subodh on 15-APR-2024
           and rownum = 1) po_startdate_fuel_rate
      ,(select value
          from TR_FUEL_RATE_CHART
         where fuel_type = nvl(tv.fuel_type,tr.calc_veh_fuel_type) --'Diesel'
           and trunc(effective_date) = trunc(tr.schedule_date)
           and rownum = 1) trip_fuel_rate
     --,(select pol.quantity
     --    from po_lines_all pol
     --   where pol.po_header_id = poh.po_header_id
     --     and rownum = 1) bpa_quantity
     --,(select (select uom_code from MTL_UNITS_OF_MEASURE where unit_of_measure=pol.unit_meas_lookup_code)
     --    from po_lines_all pol
     --   where pol.po_header_id = poh.po_header_id
     --     and rownum = 1) bpa_rate_type
     ,nvl(tv.mileage,tr.calc_veh_mileage) vehicle_mileage
     --,tr.payment_amount                trip_rate
     ,tr.est_total_qty                 trip_est_capacity_qty
     ,nvl(tv.capacity,tr.capacity_qty) vechicle_capacity_qty
     ,tr.rt_dist_in_km                 trip_in_kilometers
     ,nvl(tr.calc_rate_per_pay_type,0) calc_rate_per_pay_type
     ,to_number(case when pol.price_type_lookup_code = 'FIXED' then '0'
                     else to_char(nvl(tr.calc_extra_rate_per_pay_type,0))
                 end) calc_extra_rate_per_pay_type
     ,tr.trip_trx_id
     ,tv.CAPACITY vehicle_CAPACITY
     , (select avg(NET_WEIGHT)
   from  bmlcustm2.rm_shipping_hdr where 1=1 
   and ROUT_TRX_NUM=tr.trip_trx_id) transport_qty,
   tr.calc_rate_km
 from  bmlcustm2.tr_trip_hdr tr
      ,po_headers_all        poh
      ,po_lines_all          pol
      ,ap_suppliers          aps
      --,bmlcustm2.tr_vehicle  tv
      --Added by Subodh on 21-MAR-2024 to pick Vehicle details from route master
       , (select tv1.VEHICLE_ID
               , tr.route_number
               ,tv1.mileage
               ,tv1.capacity
               ,tv1.fuel_type
         from bmlcustm2.tr_route  tr
              ,bmlcustm2.tr_vehicle tv1
             where 1=1
             and tr.VEHICLE_ID = tv1.VEHICLE_ID ) tv
 where 1=1
   and pol.po_header_id           = poh.po_header_id
   and pol.vendor_product_num     = tr.route_number
   and poh.po_header_id           = tr.ebs_bpa_po_hdr_id
   and poh.vendor_id              = aps.vendor_id
   --and poh.segment1='30404'
   and poh.type_lookup_code       = 'BLANKET'
   and tr.route_number              = tv.route_number --Added by Subodh on 21-MAR-2024 
   and tr.calc_request_id         = c_conc_request_id
   --Added by Subodh on 15-APR-2024
    and trunc(tr.schedule_date) between trunc(to_date(pol.attribute1,'YYYY/MM/DD HH24:MI:SS'))
                                   and trunc(to_date(pol.attribute2,'YYYY/MM/DD HH24:MI:SS'))
   ---Added by Subodh on 07-MAR-2024 to pick only Active BPAs
   and exists
  (select 1 from po_headers_all pha 
   where poh.po_header_id = pha.po_header_id 
  and NVL(pha.CLOSED_CODE,'OPEN'  )= 'OPEN'
   and NVL(pha.CLOSED_DATE,trunc(sysdate+1)) >= trunc(sysdate) 
   and NVL(pha.CANCEL_FLAG,'N')='N' )
                     
 order by tr.schedule_date
 ;

--cursor get_km_cur
--is
--select shift,sum(nvl(quantity_received,0)) qty_in_kg
--  from bmlcustm2.rm_shipping_hdr
-- where transaction_code = 'BMC'
--   and route_no         = trip_rec.route_number --'BMC-21'
--   and rout_trx_num     = trip_rec.trip_trx_id  --321990
--   and shipped_date     = trip_rec.trip_date    --'18-APR-2022'
-- group by shift;

begin
x_errcode        := 0;
l_return_status  := 'S';
l_step           := 'START: ';
log(l_prc,l_step);


--log(l_prc,l_step || 'p_from_date =' || p_from_date || ',p_to_date=' || p_to_date
--                 || ',p_route_type=' || p_route_type || ',p_route_shift=' || p_route_shift
--                 || ',p_route_number=' || p_route_number || ',p_vendor_id=' || p_vendor_id
--                 || ',p_generate_invoice=' || p_generate_invoice);

if g_vendor_id is not null then
   select to_number(segment1) into l_vendor_number from ap_suppliers where vendor_id = g_vendor_id;
end if;

update bmlcustm2.tr_trip_hdr tr
   set calc_request_id            = g_conc_request_id
      ,payment_amount             = null
      ,calc_payable_fuel_price    = null
      ,calc_status                = 'NEW'
      ,calc_error_msg             = ''
      ,tr.ebs_ap_invoice_id       = -9
 where tr.ROUTE_TYPE              = g_route_type
   AND NVL(tr.ROUTE_SHIFT,'$##$') = NVL(g_route_Shift,NVL(tr.ROUTE_SHIFT,'$##$'))
   AND NVL(tr.ROUTE_NUMBER,'X')   = NVL(g_route_number,NVL(tr.ROUTE_NUMBER,'X'))
   AND NVL(tr.transporter_id,-1)  = NVL(l_vendor_number,NVL(tr.transporter_id,-1))
   and trunc(tr.schedule_date) between g_from_date and g_to_date
   and NVL(tr.ebs_ap_invoice_id,-9) =-9
   and nvl(tr.calc_status,'NEW') in ('NEW','ERROR','CALCULATED') ;

--log(l_prc,'Rows picked = ' || sql%rowcount);
--l_rowcount := sql%rowcount;
l_rowcount1 := sql%rowcount;

--log(l_prc,'Rows picked = ' || l_rowcount);
--log(l_prc,'Rows picked = ' || l_rowcount1);
log(l_prc,'Rows picked = ' || sql%rowcount);

if l_rowcount1 = 0 then
   l_return_msg := 'NO DATA TO PROCESS!!';
   raise l_stop;
end if;

begin
l_sum_vehicle_capacity :=0;
l_sum_transport_qty :=0;
--FND_FILE.put_line(FND_FILE.log,'Before CAPACITY ');
select sum(tv.CAPACITY) 
     ,sum((select avg(NET_WEIGHT)
   from  bmlcustm2.rm_shipping_hdr where 1=1 
   and ROUT_TRX_NUM=tr.trip_trx_id)) 
   into l_sum_vehicle_capacity
   ,l_sum_transport_qty
 from  bmlcustm2.tr_trip_hdr tr
      ,po_headers_all        poh
      ,po_lines_all          pol
      ,ap_suppliers          aps
      ,(select tv1.VEHICLE_ID
               , tr.route_number
               ,tv1.mileage
               ,tv1.capacity
               ,tv1.fuel_type
         from bmlcustm2.tr_route  tr
              ,bmlcustm2.tr_vehicle tv1
             where 1=1
             and tr.VEHICLE_ID = tv1.VEHICLE_ID )  tv
 where 1=1
   --AND tr.route_status='APPROVED'
   and pol.po_header_id           = poh.po_header_id
   and pol.vendor_product_num     = tr.route_number
   and poh.po_header_id           = tr.ebs_bpa_po_hdr_id
  -- and tr.transporter_id          = aps.segment1
   and poh.vendor_id              = aps.vendor_id
   --and poh.segment1='30404'
   and poh.type_lookup_code       = 'BLANKET'
  -- and tr.vehicle_id              = tv.vehicle_id(+)
    and tr.route_number              = tv.route_number --Added by Subodh on 22-MAR-2024 
   --parameters
--   AND NVL(tr.ROUTE_TYPE,'$##$')  = NVL(c_route_type, NVL(tr.ROUTE_TYPE, '$##$'))
--   AND NVL(tr.ROUTE_SHIFT,'$##$') = NVL(c_route_Shift,NVL(tr.ROUTE_SHIFT,'$##$'))
--   AND NVL(tr.ROUTE_NUMBER,'X')   = NVL(c_route_number,NVL(tr.ROUTE_NUMBER,'X'))
--   AND NVL(aps.vendor_id,-1)      = NVL(c_vendor_id,NVL(aps.vendor_id,-1))
--   and trunc(tr.schedule_date) between c_from_date and c_to_date
   and tr.calc_request_id         = g_conc_request_id
   ---Added by Subodh on 07-MAR-2024 to pick only Active BPAs
   and exists
  (select 1 from po_headers_all pha 
   where poh.po_header_id = pha.po_header_id 
  and NVL(pha.CLOSED_CODE,'OPEN'  )= 'OPEN'
   and NVL(pha.CLOSED_DATE,trunc(sysdate+1)) >= trunc(sysdate) 
   and NVL(pha.CANCEL_FLAG,'N')='N' );  
   exception when others then 
   l_sum_vehicle_capacity :=0;
l_sum_transport_qty :=0;
--FND_FILE.put_line(FND_FILE.log,'Before CAPACITY EX ');
   end;              


for route_rec in (select distinct poh.po_header_id,tr.trip_trx_id ,poh.segment1 po_number
                        --,MAX(fnd_date.canonical_to_date(pol.attribute1)) bpa_start_date
                        --,MAX(fnd_date.canonical_to_date(pol.attribute2)) bpa_end_date
                    from bmlcustm2.tr_trip_hdr tr
                        ,po_headers_all poh
                        ,po_lines_all   pol   
                   where pol.po_header_id       = poh.po_header_id
                     and pol.vendor_product_num = tr.parent_route_number
                     and tr.schedule_date between fnd_date.canonical_to_date(pol.attribute1) and fnd_date.canonical_to_date(pol.attribute2) 
                     and tr.calc_request_id     = g_conc_request_id
                     and tr.ebs_bpa_po_hdr_id is null
                      ---Added by Subodh on 07-MAR-2024 to pick only Active BPAs
   and exists
  (select 1 from po_headers_all pha 
   where poh.po_header_id = pha.po_header_id 
  and NVL(pha.CLOSED_CODE,'OPEN'  )= 'OPEN'
   and NVL(pha.CLOSED_DATE,trunc(sysdate+1)) >= trunc(sysdate) 
   and NVL(pha.CANCEL_FLAG,'N')='N' )
                     )
                     
loop

--FND_FILE.put_line(FND_FILE.log,'Inside po loop  ');
    update bmlcustm2.tr_trip_hdr
       set ebs_bpa_po_hdr_id = route_rec.po_header_id
        ,ebs_bpa_po_number = route_rec.po_number --Added by Subodh on 07-MAR-2024
     where trip_trx_id       = route_rec.trip_trx_id;
end loop;

commit;

  --loop trip_header
  l_step := 'LOOP_TRIP: ';
  --FND_FILE.put_line(FND_FILE.log,'Before trip_cur loop :'||g_conc_request_id);
  for trip_rec in trip_cur (g_route_type
                           ,g_route_Shift
                           ,g_route_number
                           ,g_vendor_id
                           ,g_from_date
                           ,g_to_date
                           ,g_conc_request_id)
  loop
    begin
    --FND_FILE.put_line(FND_FILE.log,'Inside trip_cur loop 1 ');
      --
      l_step                        := 'LOOPING TRIP_TRX_ID:' || trip_rec.trip_trx_id || ': ';
      l_return_msg                  := '';
      --
      l_payable_diesel_price        := 0;
      l_payable_diesel_price_per_km := 0;
      l_revised_diesel_price_per_km := 0;
      l_total_trip_kilometers       := 0;
      l_total_trip_amount           := 0;
      l_return_msg                  := '';
      l_cost_ltr                    :=0 ;
      l_pro_cost                    :=0 ;
      l_amount                      :=0 ;
      
      --FND_FILE.put_line(FND_FILE.log,'Inside trip_cur loop 2 ');

--FND_FILE.put_line (APPS.FND_FILE.LOG,'1');
      --Validate PO NUMBER
      l_step := 'VAL_PO_NUMBER: ';
      if trip_rec.po_number is null then
         l_return_msg := 'H: No BPA/PO Number on Trip.';
         raise l_skip;
      end if;
--FND_FILE.put_line (APPS.FND_FILE.LOG,'2');
      --Validate PO FUEL RATE
      l_step := 'VAL_PO_FUEL_RATE: ';
      if nvl(trip_rec.po_startdate_fuel_rate,0) = 0 then
         l_return_msg := 'H: No Fuel Rate for PO Start Date: ' || to_char(trip_rec.po_date,'DD-MON-RRRR');
         raise l_skip;
      end if;
--FND_FILE.put_line (APPS.FND_FILE.LOG,'3');
      --Validate PO RATE PER KILOMETER
      --l_step := 'VAL_PO_RATE_PER_KM: ';
      --if nvl(trip_rec.bpa_rate_per_pay_type,0) = 0 then
      --   l_return_msg := 'H: No Rate per KM in PO.';
      --   raise l_skip;
      --end if;

      --Validate CALC_PAYMENT_TYPE
      l_step := 'VAL_CALC_PAY_TYPE: ';
      if trip_rec.calc_pay_type is null then
         l_return_msg := 'H: No Payment Type on Trip.';
         raise l_skip;
      end if;
--FND_FILE.put_line (APPS.FND_FILE.LOG,'4');
      --Validate TRIP FUEL RATE
      l_step := 'VAL_TRIP_FUEL_RATE: ';
      if nvl(trip_rec.trip_fuel_rate,0) = 0 then
         l_return_msg := 'L: No Fuel Rate for Trip Date: ' || to_char(trip_rec.trip_date,'DD-MON-RRRR');
         raise l_skip;
      end if;
--FND_FILE.put_line (APPS.FND_FILE.LOG,'5');
      --Validate VEHICLE MIELAGE
      l_step := 'VAL_VEHICLE_MIELAGE: ';
      if nvl(trip_rec.vehicle_mileage,0) = 0 then
         l_return_msg := 'L: No Vehicle Milage.';
         raise l_skip;
      end if;

      --Validate RATE_PER_PAYMENT_TYPE
      l_step := 'VAL_RATE_PER_PAYMENT_TYPE: ';
      if nvl(trip_rec.bpa_rate_per_pay_type,nvl(trip_rec.calc_rate_per_pay_type,0)) = 0 then
         l_return_msg := 'L: No Rate Per Payment Type.';
         raise l_skip;
      end if;

--FND_FILE.put_line (APPS.FND_FILE.LOG,'6');
      l_step := 'START_AMOUNT_CALC: ';
      l_payable_diesel_price        := trip_rec.trip_fuel_rate - trip_rec.po_startdate_fuel_rate;
      l_payable_diesel_price_per_km := round(((trip_rec.trip_fuel_rate - trip_rec.po_startdate_fuel_rate)/trip_rec.vehicle_mileage),2);

      l_revised_diesel_price_per_km := trip_rec.bpa_rate_per_pay_type + l_payable_diesel_price_per_km;
      l_step := 'AFTER_BASIC_CALC: ';
--FND_FILE.put_line (APPS.FND_FILE.LOG,'7');
      if     trip_rec.calc_pay_type = 'TRIP' then

             l_total_trip_amount       :=   nvl(trip_rec.bpa_rate_per_pay_type,nvl(trip_rec.calc_rate_per_pay_type,0))
                                          + nvl(trip_rec.calc_extra_rate_per_pay_type,0);

      elsif  trip_rec.calc_pay_type in ('KILOMETER','KILOGRAM') then
--FND_FILE.put_line (APPS.FND_FILE.LOG,'8');
             l_total_trip_kilometers   :=   nvl(trip_rec.bpa_rate_per_pay_type,nvl(trip_rec.calc_rate_per_pay_type,0))
                                          + nvl(trip_rec.calc_extra_rate_per_pay_type,0);
             l_total_trip_amount       := l_revised_diesel_price_per_km * l_total_trip_kilometers;
      else
             l_total_trip_amount       := l_revised_diesel_price_per_km * trip_rec.trip_in_kilometers;
      end if;
      --FND_FILE.put_line (APPS.FND_FILE.LOG,'9');
       l_cost_ltr := round((l_revised_diesel_price_per_km*trip_rec.trip_in_kilometers)/nvl(trip_rec.vehicle_CAPACITY,0),2) ;
       --FND_FILE.put_line (APPS.FND_FILE.LOG,'10');
       

       IF l_sum_transport_qty >  l_sum_vehicle_capacity THEN
       g_prorate_flag := 'Y';
       
       l_pro_cost :=  (nvl(trip_rec.transport_qty,0) - nvl(trip_rec.vehicle_CAPACITY,0))*l_cost_ltr;
       END IF;
      -- FND_FILE.put_line (APPS.FND_FILE.LOG,'11');
        l_amount := trip_rec.trip_in_kilometers *trip_rec.calc_rate_km ;
       l_total_trip_amount := nvl(l_pro_cost,0)+l_amount+trip_rec.calc_extra_rate_per_pay_type;
       
      
       
       
      -- FND_FILE.put_line (APPS.FND_FILE.LOG,'12');

      l_step := 'AFTER_TOTAL_TRIP_AMT_CALC: ';

      --log start
      log(l_prc,'LOG START ******* TRIP_DATE = ' || trip_rec.trip_date || '  *******');
      log(l_prc,'trip_rec.trip_fuel_rate         = ' || trip_rec.trip_fuel_rate);
      log(l_prc,'trip_rec.po_startdate_fuel_rate = ' || trip_rec.po_startdate_fuel_rate);
      log(l_prc,'l_payable_diesel_price          = ' || l_payable_diesel_price);
      --
      log(l_prc,'trip_rec.vehicle_mileage        = ' || trip_rec.vehicle_mileage);
      log(l_prc,'l_payable_diesel_price_per_km   = ' || l_payable_diesel_price_per_km);
      --
      log(l_prc,'trip_rec.bpa_rate_per_pay_type  = ' || trip_rec.bpa_rate_per_pay_type);
      log(l_prc,'l_revised_diesel_price_per_km   = ' || l_revised_diesel_price_per_km);
      --
      log(l_prc,'trip_rec.trip_in_kilometers     = ' || trip_rec.trip_in_kilometers);
      log(l_prc,'rip_rec.calc_rate_km             = ' || trip_rec.calc_rate_km);
      log(l_prc,'l_amount             = '              || l_amount);
      log(l_prc,'l_total_trip_amount             = ' || l_total_trip_amount);
       log(l_prc,'l_pro_cost             = ' || l_pro_cost);
       log(l_prc,'l_cost_ltr             = ' || l_cost_ltr);
       log(l_prc,'l_sum_transport_qty             = ' || l_sum_transport_qty);
       log(l_prc,'l_sum_vehicle_capacity             = ' || l_sum_vehicle_capacity);
         log(l_prc,'g_prorate_flag             = ' || g_prorate_flag);
      log(l_prc,'LOG END   ******* TRIP_DATE = ' || trip_rec.trip_date || '  *******');
      --log end

      /*update tr_trip_hdr
         set payment_amount          = round(l_total_trip_amount,2) --Added by Subodh on 22-MAR-2024
            ,amount                  = round(l_amount,2) --Added by Subodh on 22-MAR-2024
            ,calc_payable_fuel_price = round(l_payable_diesel_price_per_km,2) --Round added by Subodh on 02-APR-2024
            ,calc_rate_km            = round(l_revised_diesel_price_per_km,2) --Added by Subodh on 07-MAR-2024 --Round added by Subodh on 02-APR-2024
            ,cost_ltr                = round(l_cost_ltr,2) --Round added by Subodh on 02-APR-2024
            ,pro_cost                = round(l_pro_cost,2)--Round added by Subodh on 02-APR-2024
            ,calc_status             = 'CALCULATED'
            ,calc_error_msg          = ''
       where trip_trx_id             = trip_rec.trip_trx_id;*/
	   
	   --Added by Subodh on 04-NOV-2025 
	    update tr_trip_hdr
         set payment_amount          = trunc(l_total_trip_amount,2) --Added by Subodh on 22-MAR-2024
            ,amount                  = trunc(l_amount,2) --Added by Subodh on 22-MAR-2024
            ,calc_payable_fuel_price = trunc(l_payable_diesel_price_per_km,2) --Round added by Subodh on 02-APR-2024
            ,calc_rate_km            = trunc(l_revised_diesel_price_per_km,2) --Added by Subodh on 07-MAR-2024 --Round added by Subodh on 02-APR-2024
            ,cost_ltr                = trunc(l_cost_ltr,2) --Round added by Subodh on 02-APR-2024
            ,pro_cost                = trunc(l_pro_cost,2)--Round added by Subodh on 02-APR-2024
            ,calc_status             = 'CALCULATED'
            ,calc_error_msg          = ''
       where trip_trx_id             = trip_rec.trip_trx_id;
	   --------------------------------------------------------

      l_step := 'CALC_AMOUNT_UPDATED: ';
      --
      commit;

       --FND_FILE.put_line (APPS.FND_FILE.LOG,'8 '||trip_rec.trip_trx_id);

    exception
      when l_skip then
        rollback;
        update tr_trip_hdr tr
           set tr.calc_status    = 'ERROR'
              ,tr.calc_error_msg = l_return_msg
         where trip_trx_id       = trip_rec.trip_trx_id;
      when l_stop then
       x_errcode := 2;
       x_errbuf  := ' END ERROR L_STOP: ' || l_return_msg;
       log(l_prc,x_errbuf); 
       output(l_return_msg);     
      when others then
        rollback;
        l_return_msg := l_step || substr(sqlerrm,1,200);
        update tr_trip_hdr tr
           set tr.calc_status    = 'ERROR'
              ,tr.calc_error_msg = l_return_msg
         where trip_trx_id       = trip_rec.trip_trx_id;
       
    end;
    --
  end loop;  --for trip_rec
  --
  if g_generate_invoice = 'Y' then
     --if g_route_number = 'BMC-3' then Commented by Subodh on 19-MAR-2024
        ap_invoices_interface_insert(g_conc_request_id
                                    ,l_return_status
                                    ,l_return_msg);
    -- end if;
  else
      update tr_trip_hdr tr
         set tr.ebs_ap_invoice_id = null
            ,tr.calc_status       = case when tr.calc_status = 'ERROR' then 'ERROR' else 'NEW' end
       where tr.calc_request_id   = g_conc_request_id
         and tr.ebs_ap_invoice_id = -9;
  end if;

  write_output_rate();

log(l_prc, ' END');
exception
    when l_skip then
       x_errcode := 2;
       x_errbuf  := ' END ERROR L_SKIP: ' || l_return_msg;
       log(l_prc,x_errbuf);
    when others then
       x_errcode := 2;
       x_errbuf  := 'END ERROR: ' || l_step || substr(sqlerrm,1,250);
       log(l_prc, x_errbuf);
end billing_rate_based;


procedure transport_billing_process(x_errbuf            out varchar2
                                   ,x_errcode           out number
                                   ,p_from_date          in varchar2
                                   ,p_to_date            in varchar2
                                   ,p_reporting_month    in varchar2
                                   ,p_reporting_period   in varchar2
                                   ,p_route_type         in varchar2
                                   ,p_route_shift        in varchar2
                                   ,p_route_number       in varchar2
                                   ,p_vendor_id          in number
                                   ,p_generate_invoice   in varchar2)
as
l_prc                      varchar2(50) := 'transport_billing_process';
l_step                     varchar2(100);
l_return_status            varchar2(1);
l_return_msg               varchar2(4000);
l_skip                     exception;
l_from_date                date;
l_to_date                  date;
--

begin
x_errcode        := 0;
l_return_status  := 'S';
l_step           := 'START: ';
log(l_prc,l_step);

l_from_date       := to_date(p_from_date,'YYYY/MM/DD HH24:MI:SS');
l_to_date         := to_date(p_to_date,'YYYY/MM/DD HH24:MI:SS');

log(l_prc,l_step || 'p_from_date =' || p_from_date || ',p_to_date=' || p_to_date
                 || ',p_route_type=' || p_route_type || ',p_route_shift=' || p_route_shift
                 || ',p_route_number=' || p_route_number || ',p_vendor_id=' || p_vendor_id
                 || ',p_generate_invoice=' || p_generate_invoice);

g_from_date        := l_from_date;
g_to_date          := l_to_date;
g_reporting_month  := to_char(to_date(p_reporting_month,'RRRR\MM\DD HH24:MI:SS'),'RRRR-MON');
g_reporting_period := p_reporting_period;
g_route_type       := p_route_type;
g_route_shift      := p_route_shift;
g_route_number     := p_route_number;
g_vendor_id        := p_vendor_id;
g_generate_invoice := p_generate_invoice;

if     p_route_type = 'BMC_ITB' then
       billing_bmc_itb(x_errbuf,x_errcode);
--elsif  p_route_type = 'DTC' then
elsif  p_route_type IN ( 'DTC','ADH') then --ADH Added by Subodh on 27-JUN-2024
       billing_dtc(x_errbuf,x_errcode);
elsif  p_route_type IN( 'TCD','RCD','CFR') then --RCD Added by Subodh on 16-JUL-2025 --CFR Added by Subodh on 16-AUG-2025
       billing_tcd(x_errbuf,x_errcode);
elsif  p_route_type In ('PTC','MVR','EV','OTR') then --'MVR','EV','OTR' Added by Subodh on 04-NOV-2025 
       billing_ptc(x_errbuf,x_errcode);
elsif  p_route_type in ('BMC','EMR','DCS') then
       billing_rate_based(x_errbuf,x_errcode);
end if;

log(l_prc, ' END');
exception
    when l_skip then
       x_errcode := 2;
       x_errbuf  := ' END ERROR L_SKIP: ' || l_return_msg;
       log(l_prc,x_errbuf);
    when others then
       x_errcode := 2;
       x_errbuf  := 'END ERROR: ' || l_step || substr(sqlerrm,1,250);
       log(l_prc, x_errbuf);
end transport_billing_process;


END XXBML_TPT_PROCESS_PKG;


-- Step 3: Insert test trip record
-- trip_trx_id = 9999999 (well above max 1660933)
-- INSERT INTO bmlcustm2.tr_trip_hdr (
--     trip_trx_id, route_id, route_number, route_type, parent_route_number,
--     route_shift, schedule_date, payment_amount,
--     vehicle_id, vehicle_num, transporter_id, transporter_name,
--     ebs_bpa_po_number, ebs_bpa_po_hdr_id,
--     rt_dist_in_km, est_total_qty, capacity_qty,
--     route_status, calc_status, ebs_ap_invoice_id,
--     creation_date, created_by
-- ) VALUES (
--      9999999,            -- unique test trip_trx_id
--      28,                 -- route_id for BDTC028
--     'BDTC028',          -- route_number
--     'DTC',              -- route_type
--     'BDTC028',          -- parent_route_number (NOT starting with 'A')
--     'E',                -- route_shift (Evening)
--     TRUNC(SYSDATE),     -- schedule_date = today (12-MAY-2026)
--     NULL,               -- payment_amount (will be calculated by billing)
--     268,                -- vehicle_id (KA05AE5583, mileage=4.5, fuel=Diesel)
--     'KA05AE5583',       -- vehicle_num
--     200687,             -- transporter_id (= vendor segment1 for BASAVARAJU)
--     'BASAVARAJU',       -- transporter_name (must match ap_suppliers.vendor_name)
--     '1285656',          -- BPA PO number
--     1686349,            -- BPA PO header_id (OPEN blanket PO, vendor 9088)
--     21,                 -- rt_dist_in_km
--     702,                -- est_total_qty
--     440,                -- capacity_qty
--     'SCHEDULED',        -- route_status
--     'NEW',              -- calc_status
--     -9,                 -- ebs_ap_invoice_id (marker: ready for billing)
--     SYSDATE,            -- creation_date
--     -1                  -- created_by
-- );
-- COMMIT;