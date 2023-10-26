drop table quan_tbl_final_0;
---------
drop table quan_tbl_final_3; --(Application)
drop table quan_tbl_final_4_1;
drop table quan_tbl_final_4; --(Linkage Information
drop table quan_tbl_final_5; --(Behavior)
drop table quan_tbl_final_7; --(CIC Information)
---------
drop table quan_tbl_final_9; --(Collection) (Group with Table 6)
drop table quan_tbl_final_10; --(Behavior) (Some features may not use)
drop table quan_tbl_final_13; --(Application)
drop table quan_tbl_final_11_3; --(Linkage Information)
drop table quan_tbl_final_11_4; --(Linkage Information)
---------
drop table quan_tbl_final_12; --(Behavior)
drop table quan_tbl_final_16; --(Behavior)
drop table quan_tbl_final_8_0; --(Behavior)
drop table quan_tbl_final_8; --(Behavior)
---------
drop table quan_tbl_final_all;

-------------------------------------------------------------
--STEP 1: Get X-sell Loan apps
-------------------------------------------------------------
create table quan_tbl_final_0 as
       ------------------------- loan_apps
       with D as
       (
       select contract_id, contract_no, contract_st
       from sdm_feprd.sdm_col_balance
       where balance_dt = trunc(sysdate) - 10
       ),
       ------------------------- xsell_loan_apps
       E as
       (
       select B.cif_nb, 
       B.app_id as app_id_xsell, 
       B.applied_date as applied_date_xsell,
       substr(to_char(B.applied_date, 'YYYY/MM'), 1, 7) as applied_month_xsell,
       B.app_status as app_status_xsell, 
       B.Crprodgroup as product_group,
       (
       case when
                (
                C.Del30_Mob12_App
                > 0
                )
                then 1
                else 0
                end
        )
        as Del30_Mob12_App,
        (
        case when
                (
                C.Del90_Mob12_App
                > 0
                )
                then 1
                else 0
                end
        )
        as Del90_Mob12_App,
        D.*
        from pol_tbl_raw_app_data B
        left join Pol_Tbl_Xuan_Pay9 C on B.app_id = C.app_id
        join D on D.contract_no = B.agreement_no
        where B.crprodgroup = 'X-sell' 
        and B.app_status = 'Approve'
        and add_months(trunc(sysdate)-1, -24) < B.applied_date
        and C.MOB12_BASE = '1'
        ),
        ------------------------------------ xsell loan order by applied_date
        F as
        (
        select E.*,
               row_number() over (partition by E.cif_nb order by E.applied_date_xsell desc) as cnt_row
        from E
        ),
        ------------------------------------ xsell loan most recent
        G as
        (
        select *
        from F
        where cnt_row = 1
        ),
        ------------------------------------
        H as
        (
        select *
        from G
        ),
        ------------------------------------
        I as
        (
        select H.cif_nb, 
            H.app_id_xsell, 
            H.applied_date_xsell,
            H.applied_month_xsell,
            H.app_status_xsell, 
            H.product_group,
            H.Del30_Mob12_App,
            H.Del90_Mob12_App,
            A.app_id, A.applied_date, 
            substr(to_char(A.applied_date, 'YYYY/MM'), 1, 7) as applied_month,
            A.agreement_no,
            row_number() over (partition by A.cif_nb order by A.applied_date desc) as cnt_row
            from H
            join pol_tbl_raw_app_data A on A.cif_nb = H.cif_nb
            where A.reject = 1
            and A.applied_date < H.applied_date_xsell
            )
        ------------------------------------
        select I.*,
               to_char(add_months(trunc(sysdate)-1, -24),'yyyy/mm') as run_month
        from I
        where I.cnt_row = 1;
        
-----------------------------------------------
--STEP 2: Get features
-----------------------------------------------

------------------------------ Application
create table quan_tbl_final_3  as
      select a.cif_nb,a.agreement_no,a.applied_date,
      max(c.NATIONALID_DATE) as max_NATIONALID_DATE,min(c.NATIONALID_DATE) as min_NATIONALID_DATE,max(APPEMAIL) as max_app_email,
      sum(APPEMAIL) as sum_APPEMAIL,count(distinct APPREGREGION) as distinct_APPREGREGION,max(APPREGREGION) as max_APPREGREGION,min(APPREGREGION) as min_APPREGREGION,STATS_MODE(APPREGREGION) as mode_APPREGREGION,
      count(distinct APPRESREGION) as distinct_APPRESREGION,max(APPRESREGION) as max_APPRESREGION,min(APPRESREGION) as min_APPRESREGION,STATS_MODE(APPRESREGION) as mode_APPRESREGION,
      max(APPRESPERIOD) as max_APPRESPERIOD,min(APPRESPERIOD) as min_APPRESPERIOD,avg(APPRESPERIOD) as avg_APPRESPERIOD,
      max(APPRESPOW) as max_APPRESPOW,min(APPRESPOW) as min_APPRESPOW,avg(APPRESPOW) as avg_APPRESPOW,
      STATS_MODE(APPRESPOW) as mode_APPRESPOW,count(distinct APPRESPOW) as distinct_APPRESPOW,
      max(BACKUPCONTACTNAME) as max_BACKUPCONTACTNAME,min(BACKUPCONTACTNAME) as min_BACKUPCONTACTNAME,sum(BACKUPCONTACTNAME) as sum_BACKUPCONTACTNAME,avg(BACKUPCONTACTNAME) as avg_BACKUPCONTACTNAME,
      max(appeducation) as appeducation,min(APPEDUCATION) as min_APPEDUCATION,STATS_MODE(APPEDUCATION) as mode_APPEDUCATION,count(distinct APPEDUCATION) as distinct_APPEDUCATION,
      max(appwperiodg) as max_appwperiodg,min(appwperiodg) as min_appwperiodg,max(appwperiod) as max_appwperiod,min(appwperiod) as min_appwperiod,
      max(APPFAMILYSTATUS) as max_APPFAMILYSTATUS,min(APPFAMILYSTATUS) as min_APPFAMILYSTATUS,STATS_MODE(APPFAMILYSTATUS) as mode_APPFAMILYSTATUS,
      max(APPFMQNTY) as max_APPFMQNTY,min(APPFMQNTY) as min_APPFMQNTY,avg(APPFMQNTY) as avg_APPFMQNTY,STATS_MODE(appfmqnty) as mode_appfmqnty,
      max(APPCHILDQNTY) as max_APPCHILDQNTY,min(APPCHILDQNTY) as min_APPCHILDQNTY,avg(APPCHILDQNTY) as avg_APPCHILDQNTY,stats_mode(APPCHILDQNTY) as mode_APPCHILDQNTY,
      max(PRIMARYINCOME) as max_PRIMARYINCOME,min(PRIMARYINCOME) as min_PRIMARYINCOME,avg(PRIMARYINCOME) as avg_PRIMARYINCOME,
      max(FAMILYINCOME) as max_FAMILYINCOME,min(FAMILYINCOME) as min_FAMILYINCOME,avg(FAMILYINCOME) as avg_FAMILYINCOME,
      max(FAMILYEXPENSE) as max_FAMILYEXPENSE,min(FAMILYEXPENSE) as min_FAMILYEXPENSE,avg(FAMILYEXPENSE) as avg_FAMILYEXPENSE,
      max(BASICEXPENCES) as max_BASICEXPENCES,min(BASICEXPENCES) as min_BASICEXPENCES,avg(BASICEXPENCES) as avg_BASICEXPENCES,
      max(PRIMARYINCOME-BASICEXPENCES) as max_dis_income,min(PRIMARYINCOME-BASICEXPENCES) as min_dis_income,avg(PRIMARYINCOME-BASICEXPENCES) as avg_dis_income,
      max(FAMILYINCOME-FAMILYEXPENSE) as max_dis_f_income,min(FAMILYINCOME-FAMILYEXPENSE) as min_dis_f_income,avg(FAMILYINCOME-FAMILYEXPENSE) as avg_dis_f_income,
      max(GDSPRICE) as max_GDSPRICE,min(GDSPRICE) as min_GDSPRICE,avg(GDSPRICE) as avg_GDSPRICE,sum(GDSPRICE) as sum_GDSPRICE,
      max(ADVPAY) max_ADVPAY,min(ADVPAY) as min_ADVPAY,avg(ADVPAY) as avg_ADVPAY,
      max(CREDITSUM) as max_CREDITSUM,min(CREDITSUM) as min_CREDITSUM,avg(CREDITSUM) as avg_CREDITSUM,
      max(CREDITTERM) as max_CREDITTERM,min(CREDITTERM) as min_CREDITTERM,avg(CREDITTERM) as avg_CREDITTERM,
      max(APPMONTHPAYMENT) as max_APPMONTHPAYMENT,min(APPMONTHPAYMENT) as min_APPMONTHPAYMENT,avg(APPMONTHPAYMENT) as avg_APPMONTHPAYMENT,
      max(APPINTEREST) as max_APPINTEREST,min(APPINTEREST) min_APPINTEREST,avg(APPINTEREST) as avg_APPINTEREST,stats_mode(APPINTEREST) as mode_APPINTEREST,
      stats_mode(DOP_MAINGOODSCATEGORY) as mode_DOP_MAINGOODSCATEGORY,stats_mode(ASSET_BRAND) as mode_ASSET_BRAND,stats_mode(PORT_NONPORT) as mode_PORT_NONPORT,
      stats_mode(product_segment) as mode_product_segment,max(COMPANY_CATEGORY) as max_COMPANY_CATEGORY,min(COMPANY_CATEGORY) as min_COMPANY_CATEGORY,stats_mode(COMPANY_CATEGORY) as mode_COMPANY_CATEGORY,
      max(APPDISBCHAN) as max_APPDISBCHAN,min(APPDISBCHAN) as min_APPDISBCHAN,stats_mode(APPDISBCHAN) as mode_APPDISBCHAN
      from quan_tbl_final_0 a
      left join pol_tbl_tuan_previous_app b on a.agreement_no=b.agreement_no and b.approved_prev=1
      left join pol_tbl_raw_app_data c on c.app_id=b.app_id_prev
      group by a.cif_nb,a.agreement_no,a.applied_date;

------------------------------ Keep Table 4_1 for further use (see Table 4, Table 11_3, Table 11_4, Table 12)
create table quan_tbl_final_4_1 as
      select  a.cif_nb,a.agreement_no,a.app_id,a.applied_date,c.app_id_c_ref,
      max(c.cif_ref) cif_ref,max(c.is_spouse) is_spouse,max(c.is_owner) is_owner,max(c.is_reference) is_reference,max(c.is_relative) is_relative,max(c.is_same_ref) is_same_ref
      from quan_tbl_final_0 a
      left join pol_tbl_tuan_previous_app b on a.agreement_no=b.agreement_no and b.approved_prev=1
      left join pol_tbl_raw_ref_data c on b.app_id_c=c.app_id_c
      group by a.cif_nb,a.agreement_no,a.app_id,a.applied_date,c.app_id_c_ref;

------------------------------ Linkage Information: Get DPD information of linkage applications
create table quan_tbl_final_4 as
    select a.cif_nb,a.agreement_no,a.app_id,a.applied_date,max(c.dpdall) max_DPDall_ref,max(c.dpd) max_DPD_ref
    from quan_tbl_final_0 a
    left join quan_tbl_final_4_1 b on a.agreement_no=b.agreement_no and a.cif_nb=b.cif_nb
    left join (select * from ntbv3_esb_raw_beh_data
              union all
              select * from ntbv3_esb_raw_beh_data_update
              ) c on c.agreementno = b.agreement_no and c.report_month=a.run_month
    group by a.cif_nb,a.agreement_no,a.app_id,a.applied_date;

------------------------------ Behavior: Approve and Reject
create table quan_tbl_final_5 as
      select a.cif_nb,a.agreement_no,a.app_id,a.applied_date,sum(case when b.approved_prev=1 then 1 else 0 end) as prev_approved,
      sum(case when c.reject in (1,5) then 0 else 1 end) as prev_rejected,
      max( case when c.reject=2 then 1 else 0 end) as ever_rejected_score,
      max( case when c.reject_stage='REJ_CIC' then 1 else 0 end) as ever_rejected_CIC,
      max( case when c.reject_reason like '%lacklist status%' then 1 else 0 end) as ever_rejected_BL,
      max( case when b.approved_prev=1 and b.app_id_c<>b.app_id_prev then 1 else 0 end) as approved_returning_cust,
      max( case when b.app_id_c<>b.app_id_prev then 1 else 0 end) as ever_returning_cust
      from quan_tbl_final_0 a
      left join pol_tbl_tuan_previous_app b on a.agreement_no=b.agreement_no and a.app_id=b.app_id_c
      left join pol_tbl_raw_app_data c on b.app_id_prev=c.app_id and b.agreement_no_prev=c.agreement_no
      group by a.cif_nb,a.agreement_no,a.app_id,a.applied_date;

------------------------------ Collection: Attempt and Good, Bad response (We can group Table 6 and Table 9 together)
create table quan_tbl_final_6 as
      select  a.cif_nb,a.agreement_no,a.app_id,a.applied_date,
      sum(cnt) col_cnt,sum(attempt_cnt) attempt_cnt,
      sum(bad_response) bad_response,sum(good_response) good_response,
      sum(bad_response)/greatest(sum(attempt_cnt),1) as bad_response_ratio,
      sum(good_response)/greatest(sum(attempt_cnt),1) as good_response_ratio
      from quan_tbl_final_0 a
      left join pol_tbl_tuan_previous_app a1 on a.agreement_no=a1.agreement_no and a.app_id=a1.app_id_c
      left join (
                select * from ntbv3_esb_raw_follow_data
                union all
                select * from ntbv3_esb_raw_follow_data_update
                ) b on a1.agreement_no_prev=b.agreement_no and to_date(a.run_month,'yyyy/mm')>=b.contact_month
      group by a.cif_nb,a.agreement_no,a.app_id,a.applied_date;


------------------------------ CIC Information
create table quan_tbl_final_7 as
      with quan_tbl_final_7_0 as
      (
      select  a.*,a1.applied_date_prev,
      c.creditinstitution_no,c.cic_ok,c.cic_not_ok,c.cic_warning,c.cic_not_found,c.cic_cannot_check,c.cic_result
      from quan_tbl_final_0 a
      left join pol_tbl_tuan_previous_app a1 on a.agreement_no=a1.agreement_no and a.app_id=a1.app_id_c
      left join pol_tbl_tuan_cic_general c on a1.app_id_prev=c.app_id
      )
      select  a.cif_nb,a.agreement_no,a.app_id,a.applied_date,max(creditinstitution_no) as max_cic_institution,
      max(cic_ok) as cic_ever_ok,max(cic_not_ok) as cic_ever_not_ok,
      max(cic_warning) as cic_ever_warning,max(cic_not_found) as cic_ever_not_found,
      max(cic_cannot_check) as cic_ever_cannot_check
      from quan_tbl_final_7_0  a
      group by a.cif_nb,a.agreement_no,a.app_id,a.applied_date;


------------------------------ Collection (Group Table 9 with Table 6 together)
create table quan_tbl_final_9  as
      SELECT  t.cif_nb,t.app_id,t.agreement_no
             , nvl(sum(BAD_RESPONSE),0) BAD_RESPONSE_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=3 then BAD_RESPONSE end),0) BAD_RESPONSE_3M_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=6 then BAD_RESPONSE end),0) BAD_RESPONSE_6M_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=9 then BAD_RESPONSE end),0) BAD_RESPONSE_9M_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=12 then BAD_RESPONSE end),0) BAD_RESPONSE_12M_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=24 then BAD_RESPONSE end),0) BAD_RESPONSE_24M_CNT

             , nvl(sum(GOOD_RESPONSE),0) GOOD_RESPONSE_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=3 then GOOD_RESPONSE end),0) GOOD_RESPONSE_3M_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=6 then GOOD_RESPONSE end),0) GOOD_RESPONSE_6M_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=9 then GOOD_RESPONSE end),0) GOOD_RESPONSE_9M_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=12 then GOOD_RESPONSE end),0) GOOD_RESPONSE_12M_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=24 then GOOD_RESPONSE end),0) GOOD_RESPONSE_24M_CNT

             , nvl(sum(ATTEMPT_CNT),0) ATTEMPT_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=3 then ATTEMPT_CNT end),0) ATTEMPT_3M_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=6 then ATTEMPT_CNT end),0) ATTEMPT_6M_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=9 then ATTEMPT_CNT end),0) ATTEMPT_9M_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=12 then ATTEMPT_CNT end),0) ATTEMPT_12M_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=24 then ATTEMPT_CNT end),0) ATTEMPT_24M_CNT

             , nvl(sum(CONNECT_CNT),0) CONNECT_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=3 then CONNECT_CNT end),0) CONNECT_3M_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=6 then CONNECT_CNT end),0) CONNECT_6M_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=9 then CONNECT_CNT end),0) CONNECT_9M_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=12 then CONNECT_CNT end),0) CONNECT_12M_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=24 then CONNECT_CNT end),0) CONNECT_24M_CNT

             , nvl(sum(CONTACT_CLIENT_CNT),0) CONTACT_CLIENT_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=3 then CONTACT_CLIENT_CNT end),0) CONTACT_CLIENT_3M_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=6 then CONTACT_CLIENT_CNT end),0) CONTACT_CLIENT_6M_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=9 then CONTACT_CLIENT_CNT end),0) CONTACT_CLIENT_9M_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=12 then CONTACT_CLIENT_CNT end),0) CONTACT_CLIENT_12M_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=24 then CONTACT_CLIENT_CNT end),0) CONTACT_CLIENT_24M_CNT
             , nvl(min(case when CONTACT_CLIENT_CNT >= 1 then months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) end),99) CONTACT_CLIENT_LAST
             , nvl(max(case when CONTACT_CLIENT_CNT >= 1 then months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) end),99) CONTACT_CLIENT_FIRST

             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=3 and CONTACT_CLIENT_CNT > 1 then 1 end),0) MONTH_CONTACT_CLIENT_3M
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=6 and CONTACT_CLIENT_CNT > 1 then 1 end),0) MONTH_CONTACT_CLIENT_6M
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=9 and CONTACT_CLIENT_CNT > 1 then 1 end),0) MONTH_CONTACT_CLIENT_9M
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=12 and CONTACT_CLIENT_CNT > 1 then 1 end),0) MONTH_CONTACT_CLIENT_12M
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=24 and CONTACT_CLIENT_CNT > 1 then 1 end),0) MONTH_CONTACT_CLIENT_24M

             , nvl(sum(CONTACT_CNT),0) CONTACT_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=3 then CONTACT_CNT end),0) CONTACT_3M_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=6 then CONTACT_CNT end),0) CONTACT_6M_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=9 then CONTACT_CNT end),0) CONTACT_9M_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=12 then CONTACT_CNT end),0) CONTACT_12M_CNT
             , nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=24 then CONTACT_CNT end),0) CONTACT_24M_CNT


             , round(nvl(sum(BAD_RESPONSE)/sum(GOOD_RESPONSE + BAD_RESPONSE + 0.01),0),4) BAD_GOOD_RESPONSE_CNT
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=3 then BAD_RESPONSE end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=3 then BAD_RESPONSE + GOOD_RESPONSE + 0.01 end),0),4) BAD_GOOD_RESPONSE_3M_PCT
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=6 then BAD_RESPONSE end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=6 then BAD_RESPONSE + GOOD_RESPONSE + 0.01 end),0),4) BAD_GOOD_RESPONSE_6M_PCT
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=9 then BAD_RESPONSE end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=9 then BAD_RESPONSE + GOOD_RESPONSE + 0.01 end),0),4) BAD_GOOD_RESPONSE_9M_PCT
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=12 then BAD_RESPONSE end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=12 then BAD_RESPONSE + GOOD_RESPONSE + 0.01 end),0),4) BAD_GOOD_RESPONSE_12M_PCT
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=24 then BAD_RESPONSE end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=24 then BAD_RESPONSE + GOOD_RESPONSE + 0.01 end),0),4) BAD_GOOD_RESPONSE_24M_PCT

             , round(nvl(sum(BAD_RESPONSE)/sum(ATTEMPT_CNT + 0.01),0),4) BAD_ATTEMPT_RESPONSE_CNT
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=3 then BAD_RESPONSE end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=3 then ATTEMPT_CNT + 0.01 end),0),4) BAD_ATTEMPT_RESPONSE_3M_PCT
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=6 then BAD_RESPONSE end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=6 then ATTEMPT_CNT + 0.01 end),0),4) BAD_ATTEMPT_RESPONSE_6M_PCT
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=9 then BAD_RESPONSE end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=9 then ATTEMPT_CNT + 0.01 end),0),4) BAD_ATTEMPT_RESPONSE_9M_PCT
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=12 then BAD_RESPONSE end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=12 then ATTEMPT_CNT + 0.01 end),0),4) BAD_ATTEMPT_RESPONSE_12M_PCT
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=24 then BAD_RESPONSE end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=24 then ATTEMPT_CNT + 0.01 end),0),4) BAD_ATTEMPT_RESPONSE_24M_PCT

             , round(nvl(sum(BAD_RESPONSE)/sum(CONNECT_CNT + 0.01),0),4) BAD_CONNECT_RESPONSE_CNT
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=3 then BAD_RESPONSE end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=3 then CONNECT_CNT + 0.01 end),0),4) BAD_CONNECT_RESPONSE_3M_PCT
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=6 then BAD_RESPONSE end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=6 then CONNECT_CNT + 0.01 end),0),4) BAD_CONNECT_RESPONSE_6M_PCT
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=9 then BAD_RESPONSE end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=9 then CONNECT_CNT + 0.01 end),0),4) BAD_CONNECT_RESPONSE_9M_PCT
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=12 then BAD_RESPONSE end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=12 then CONNECT_CNT + 0.01 end),0),4) BAD_CONNECT_RESPONSE_12M_PCT
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=24 then BAD_RESPONSE end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=24 then CONNECT_CNT + 0.01 end),0),4) BAD_CONNECT_RESPONSE_24M_PCT

             , round(nvl(sum(BAD_RESPONSE)/sum(CONTACT_CNT + 0.01),0),4) BAD_CONTACT_RESPONSE_CNT
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=3 then BAD_RESPONSE end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=3 then CONTACT_CNT + 0.01 end),0),4) BAD_CONTACT_RESPONSE_3M_PCT
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=6 then BAD_RESPONSE end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=6 then CONTACT_CNT + 0.01 end),0),4) BAD_CONTACT_RESPONSE_6M_PCT
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=9 then BAD_RESPONSE end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=9 then CONTACT_CNT + 0.01 end),0),4) BAD_CONTACT_RESPONSE_9M_PCT
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=12 then BAD_RESPONSE end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=12 then CONTACT_CNT + 0.01 end),0),4) BAD_CONTACT_RESPONSE_12M_PCT
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=24 then BAD_RESPONSE end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=24 then CONTACT_CNT + 0.01 end),0),4) BAD_CONTACT_RESPONSE_24M_PCT

             , round(nvl(sum(CONNECT_CNT)/sum(ATTEMPT_CNT  + 0.01),0),4) CONNECT_RATE
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=3 then CONNECT_CNT end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=3 then ATTEMPT_CNT  + 0.01 end),0),4) CONNECT_RATE_3M
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=6 then CONNECT_CNT end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=6 then ATTEMPT_CNT  + 0.01 end),0),4) CONNECT_RATE_6M
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=9 then CONNECT_CNT end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=9 then ATTEMPT_CNT  + 0.01 end),0),4) CONNECT_RATE_9M
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=12 then CONNECT_CNT end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=12 then ATTEMPT_CNT  + 0.01 end),0),4) CONNECT_RATE_12M
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=24 then CONNECT_CNT end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=24 then ATTEMPT_CNT  + 0.01 end),0),4) CONNECT_RATE_24M

             , round(nvl(sum(CONTACT_CLIENT_CNT)/sum(ATTEMPT_CNT + 0.01),0),4) CONTACT_CLIENT_RATE
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=3 then CONTACT_CLIENT_CNT end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=3 then ATTEMPT_CNT + 0.01 end),0),4) CONTACT_CLIENT_RATE_3M
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=6 then CONTACT_CLIENT_CNT end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=6 then ATTEMPT_CNT + 0.01 end),0),4) CONTACT_CLIENT_RATE_6M
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=9 then CONTACT_CLIENT_CNT end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=9 then ATTEMPT_CNT + 0.01 end),0),4) CONTACT_CLIENT_RATE_9M
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=12 then CONTACT_CLIENT_CNT end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=12 then ATTEMPT_CNT + 0.01 end),0),4) CONTACT_CLIENT_RATE_12M
             , round(nvl(sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=24 then CONTACT_CLIENT_CNT end)/
                        sum(case when months_between(add_months(to_date(t.run_month,'yyyy/mm'),1), f.contact_month) <=24 then ATTEMPT_CNT + 0.01 end),0),4) CONTACT_CLIENT_RATE_24M

             --------------------------- Group with quan_tbl_final_6
             ,sum(f.cnt) as col_cnt,
             sum(f.attempt_cnt) as attempt_cnt_duplicate, -- To avoid duplicate column name
             sum(bad_response) as bad_response,sum(good_response) as good_response,
             sum(bad_response)/greatest(sum(attempt_cnt),1) as bad_response_ratio,
             sum(good_response)/greatest(sum(attempt_cnt),1) as good_response_ratio
             
      from quan_tbl_final_0 t
      left join pol_tbl_tuan_previous_app p on t.app_id = p.app_id_c and t.agreement_no=p.agreement_no
      left join (
                select * from ntbv3_esb_raw_follow_data
                union all
                select * from ntbv3_esb_raw_follow_data_update
                ) f on f.agreement_no = p.agreement_no_prev and to_date(t.run_month,'yyyy/mm') >= f.contact_month
      group by t.cif_nb,t.app_id,t.agreement_no;


------------------------------ Behaviour: DPD and DPDALL by different periods; ENR; Cash Flow; Arrear
---- Delete some features from source code because they are not in use for creating Final Table
create table quan_tbl_final_10 as
    SELECT a.app_id, a.agreement_no
           , nvl(-min(t.mob),0) month_on_FI
           , nvl(max(t.dpdall),0) dpdall_ever
           --max DPD
           , max(case when mob >=-3 then t.dpdall else 0 end) dpdall_3mob
           , max(case when mob >=-6 and mob <=-4 then t.dpdall else 0 end) dpdall_4_6mob
           , max(case when mob >=-9 and mob <=-7 then t.dpdall else 0 end) dpdall_7_9mob
           , max(case when mob >=-12 and mob <=-10 then t.dpdall else 0 end) dpdall_10_12mob
           , max(case when mob >=-6 then t.dpdall else 0 end) dpdall_6mob
           , max(case when mob >=-12 and mob <=-7 then t.dpdall else 0 end) dpdall_7_12mob
           , max(case when mob >=-9 then t.dpdall else 0 end) dpdall_9mob
           , max(case when mob >=-18 and mob <=-10 then t.dpdall else 0 end) dpdall_10_18mob
           , max(case when mob >=-12 then t.dpdall else 0 end) dpdall_12mob
           , max(case when mob >=-24 and mob <=-13 then t.dpdall else 0 end) dpdall_13_24mob
           , max(case when mob >=-24 then t.dpdall else 0 end) dpdall_24mob
           --months with 1 day past due
           , sum(case when mob >=-3 and t.dpdall >= 1 then 1 else 0 end) DPDALL_1DPD_CNT_3MOB
           , sum(case when mob >=-6 and mob <=-4 and t.dpdall >= 1 then 1 else 0 end) DPDALL_1DPD_CNT_4_6MOB
           , sum(case when mob >=-9 and mob <=-7 and t.dpdall >= 1 then 1 else 0 end) DPDALL_1DPD_CNT_7_9MOB
           , sum(case when mob >=-12 and mob <=-10 and t.dpdall >= 1 then 1 else 0 end) DPDALL_1DPD_CNT_10_12MOB
           , sum(case when mob >=-6 and t.dpdall >= 1 then 1 else 0 end) DPDALL_1DPD_CNT_6MOB
           , sum(case when mob >=-12 and mob <=-7 and t.dpdall >= 1 then 1 else 0 end) DPDALL_1DPD_CNT_7_12MOB
           , sum(case when mob >=-9 and t.dpdall >= 1 then 1 else 0 end) DPDALL_1DPD_CNT_9MOB
           , sum(case when mob >=-18 and mob <=-10 and t.dpdall >= 1 then 1 else 0 end) DPDALL_1DPD_CNT_10_18MOB
           , sum(case when mob >=-12 and t.dpdall >= 1 then 1 else 0 end) DPDALL_1DPD_CNT_12MOB
           , sum(case when mob >=-24 and t.dpdall >= 1 then 1 else 0 end) DPDALL_1DPD_CNT_24MOB
           --months with 2 day past due
           , sum(case when mob >=-3 and t.dpdall >= 2 then 1 else 0 end) DPDALL_2DPD_CNT_3MOB
           , sum(case when mob >=-6 and mob <=-4 and t.dpdall >= 2 then 1 else 0 end) DPDALL_2DPD_CNT_4_6MOB
           , sum(case when mob >=-9 and mob <=-7 and t.dpdall >= 2 then 1 else 0 end) DPDALL_2DPD_CNT_7_9MOB
           , sum(case when mob >=-12 and mob <=-10 and t.dpdall >= 2 then 1 else 0 end) DPDALL_2DPD_CNT_10_12MOB
           , sum(case when mob >=-6 and t.dpdall >= 2 then 1 else 0 end) DPDALL_2DPD_CNT_6MOB
           , sum(case when mob >=-12 and mob <=-7 and t.dpdall >= 2 then 1 else 0 end) DPDALL_2DPD_CNT_7_12MOB
           , sum(case when mob >=-9 and t.dpdall >= 2 then 1 else 0 end) DPDALL_2DPD_CNT_9MOB
           , sum(case when mob >=-18 and mob <=-10 and t.dpdall >= 2 then 1 else 0 end) DPDALL_2DPD_CNT_10_18MOB
           , sum(case when mob >=-12 and t.dpdall >= 2 then 1 else 0 end) DPDALL_2DPD_CNT_12MOB
           , sum(case when mob >=-24 and t.dpdall >= 2 then 1 else 0 end) DPDALL_2DPD_CNT_24MOB
           --months with 3 day past due
           , sum(case when mob >=-3 and t.dpdall >= 3 then 1 else 0 end) DPDALL_3DPD_CNT_3MOB
           , sum(case when mob >=-6 and mob <=-4 and t.dpdall >= 3 then 1 else 0 end) DPDALL_3DPD_CNT_4_6MOB
           , sum(case when mob >=-9 and mob <=-7 and t.dpdall >= 3 then 1 else 0 end) DPDALL_3DPD_CNT_7_9MOB
           , sum(case when mob >=-12 and mob <=-10 and t.dpdall >= 3 then 1 else 0 end) DPDALL_3DPD_CNT_10_12MOB
           , sum(case when mob >=-6 and t.dpdall >= 3 then 1 else 0 end) DPDALL_3DPD_CNT_6MOB
           , sum(case when mob >=-12 and mob <=-7 and t.dpdall >= 3 then 1 else 0 end) DPDALL_3DPD_CNT_7_12MOB
           , sum(case when mob >=-9 and t.dpdall >= 3 then 1 else 0 end) DPDALL_3DPD_CNT_9MOB
           , sum(case when mob >=-18 and mob <=-10 and t.dpdall >= 3 then 1 else 0 end) DPDALL_3DPD_CNT_10_18MOB
           , sum(case when mob >=-12 and t.dpdall >= 3 then 1 else 0 end) DPDALL_3DPD_CNT_12MOB
           , sum(case when mob >=-24 and t.dpdall >= 3 then 1 else 0 end) DPDALL_3DPD_CNT_24MOB

           , max(case when mob >=-3 then t.dpdall_eom else 0 end) dpdall_eom_3mob
           , max(case when mob >=-6 then t.dpdall_eom else 0 end) dpdall_eom_6mob
           , max(case when mob >=-9 then t.dpdall_eom else 0 end) dpdall_eom_9mob
           , max(case when mob >=-12 then t.dpdall_eom else 0 end) dpdall_eom_12mob
           , max(case when mob >=-24 then t.dpdall_eom else 0 end) dpdall_eom_24mob

           , -max(case when t.dpdall >=1 then mob else -1 end ) MONTH_LAST1DPD
           , -max(case when t.dpdall >=2 then mob else -1 end ) MONTH_LAST2DPD
           , -max(case when t.dpdall >=3 then mob else -1 end ) MONTH_LAST3DPD
           , -max(case when t.dpdall >=4 then mob else -1 end ) MONTH_LAST4DPD
           , -max(case when t.dpdall >=5 then mob else -1 end ) MONTH_LAST5DPD

           , -min(case when t.dpdall >=1 then mob else -1 end ) MONTH_FIRST1DPD
           , -min(case when t.dpdall >=2 then mob else -1 end ) MONTH_FIRST2DPD
           , -min(case when t.dpdall >=3 then mob else -1 end ) MONTH_FIRST3DPD
           , -min(case when t.dpdall >=4 then mob else -1 end ) MONTH_FIRST4DPD
           , -min(case when t.dpdall >=5 then mob else -1 end ) MONTH_FIRST5DPD

           , max(case when mob >=-3 then t.ENRALL else 0 end ) ENRALL_MAX_3MOB
           , max(case when mob >=-6 then t.ENRALL else 0 end ) ENRALL_MAX_6MOB
           , max(case when mob >=-9 then t.ENRALL else 0 end ) ENRALL_MAX_9MOB
           , max(case when mob >=-12 then t.ENRALL else 0 end ) ENRALL_MAX_12MOB
           , max(case when mob >=-24 then t.ENRALL else 0 end ) ENRALL_MAX_24MOB

           , round(avg(case when mob >=-3 then t.ENRALL else 0 end )) ENRALL_avg_3MOB
           , round(avg(case when mob >=-6 then t.ENRALL else 0 end )) ENRALL_avg_6MOB
           , round(avg(case when mob >=-9 then t.ENRALL else 0 end )) ENRALL_avg_9MOB
           , round(avg(case when mob >=-12 then t.ENRALL else 0 end )) ENRALL_avg_12MOB
           , round(avg(case when mob >=-24 then t.ENRALL else 0 end )) ENRALL_avg_24MOB

           , sum(case when mob >=-3 and t.enrall <= 30000 then 1 else 0 end) MONTH_CNT_0ENR_3MOB
           , sum(case when mob >=-6 and t.enrall <= 30000 then 1 else 0 end) MONTH_CNT_0ENR_6MOB
           , sum(case when mob >=-9 and t.enrall <= 30000 then 1 else 0 end) MONTH_CNT_0ENR_9MOB
           , sum(case when mob >=-12 and t.enrall <= 30000 then 1 else 0 end) MONTH_CNT_0ENR_12MOB
           , sum(case when mob >=-24 and t.enrall <= 30000 then 1 else 0 end) MONTH_CNT_0ENR_24MOB
           , -max(case when t.enrall <= 30000 then mob else 0 end) MONTH_0ENR_LAST
           , -min(case when t.enrall <= 30000 then mob else 0 end) MONTH_0ENR_FIRST

           , sum(case when mob >=-3 and t.cfall > t.amdall + 30000 then 1 else 0 end) EARLY_PMT_TOL_CNT_3MOB
           , sum(case when mob >=-6 and t.cfall > t.amdall + 30000 then 1 else 0 end) EARLY_PMT_TOL_CNT_6MOB
           , sum(case when mob >=-9 and t.cfall > t.amdall + 30000 then 1 else 0 end) EARLY_PMT_TOL_CNT_9MOB
           , sum(case when mob >=-12 and t.cfall > t.amdall + 30000 then 1 else 0 end) EARLY_PMT_TOL_CNT_12MOB
           , sum(case when mob >=-24 and t.cfall > t.amdall + 30000 then 1 else 0 end) EARLY_PMT_TOL_CNT_24MOB

           , sum(case when mob >=-3 and t.cfall > t.amdall then 1 else 0 end) EARLY_PMT_CNT_3MOB
           , sum(case when mob >=-6 and t.cfall > t.amdall then 1 else 0 end) EARLY_PMT_CNT_6MOB
           , sum(case when mob >=-9 and t.cfall > t.amdall then 1 else 0 end) EARLY_PMT_CNT_9MOB
           , sum(case when mob >=-12 and t.cfall > t.amdall then 1 else 0 end) EARLY_PMT_CNT_12MOB

    from quan_tbl_final_0 a
    left join (select * from ntbv3_esb_raw_beh_data
              union all
              select * from ntbv3_esb_raw_beh_data_update
              ) t on t.agreementno = a.agreement_no
    group by a.app_id, a.agreement_no;

------------------------------ Application: AVG_FMINCOME and INCTOEXP
/*
Code was fixed because previous code, which produced categorical values, caused too much 'No Infor' values

This is previous code

create table quan_tbl_final_13 as
      select a.agreement_no, a.app_id,a.cif_nb,
      case when appwfmqnty is null or appwfmqnty =0 or primaryincome=0 or familyexpense = 0 then 'No infor' --tuan add familyexpense = 0
      when (familyincome/primaryincome > 15 and familyincome/familyexpense > 15) then 'No infor'
      when (s.familyincome +  s.primaryincome)/s.appwfmqnty <= 3000000 then '<=3mil'
      when (s.familyincome +  s.primaryincome)/s.appwfmqnty <= 5000000 then '3-5mil'
      when (s.familyincome +  s.primaryincome)/s.appwfmqnty <= 7000000 then '5-7mil'
      when (s.familyincome +  s.primaryincome)/s.appwfmqnty <= 10000000 then '7-10mil'
      when (s.familyincome +  s.primaryincome)/s.appwfmqnty > 10000000 then '>10mil'
      end avg_fmincome,
      case when primaryincome=0 or familyexpense=0 then 'No infor'
      when (familyincome/primaryincome > 15 and familyincome/familyexpense > 15) or basicexpences is null or s.basicexpences=0 then 'No infor'
      when (s.primaryincome)/s.basicexpences <=3 then '<=3'
      when (s.primaryincome)/s.basicexpences <=4 then '3-4'
      when (s.primaryincome)/s.basicexpences <=6 then '4-6'
      when (s.primaryincome)/s.basicexpences <=8 then '6-8'
      when (s.primaryincome)/s.basicexpences >8 then '>8'
      end inctoexp
      from quan_tbl_final_0 a
      left join pol_tbl_raw_app_data s on a.agreement_no=s.agreement_no and a.app_id=s.app_id;
*/

-------- New code
create table quan_tbl_final_13 as
      select a.agreement_no, a.app_id,a.cif_nb,
      case when appwfmqnty is null or appwfmqnty =0 or primaryincome=0 or familyexpense = 0 then null --tuan add familyexpense = 0
           else (s.familyincome +  s.primaryincome)/(s.appwfmqnty * power(10,6))
      end avg_fmincome,
      case when primaryincome=0 or familyexpense=0 or basicexpences is null or basicexpences=0 then null
           else (s.primaryincome)/s.basicexpences
      end inctoexp
      from quan_tbl_final_0 a
      left join pol_tbl_raw_app_data s on a.agreement_no=s.agreement_no and a.app_id=s.app_id;       


------------------------------ Linkage Information: Reference application information related to family
create table quan_tbl_final_11_3 as
      with quan_tbl_final_11_1 as 
      (
      SELECT  t.agreement_no,
             t.app_id,
             t.applied_date,
             t.run_month,
             r.app_id_c_ref,
             a.reject_stage ref_reject_stage,
             a.reject_reason ref_reject_reason,
             a.app_status ref_app_status,
             a.applied_date ref_applied_date,
             a.agreement_no ref_agreement_no,
             ceil(months_between(a.applied_date,t.applied_date)) month_since_offer
      from quan_tbl_final_0 t
      left join quan_tbl_final_4_1 r on t.agreement_no = r.agreement_no and t.app_id=r.app_id and is_spouse + is_owner + r.is_reference + is_relative > 0
      left join pol_tbl_raw_app_data a on a.app_id = r.app_id_c_ref
                                       and add_months(a.applied_date,24) > add_months(to_date(t.run_month,'yyyy/mm'),1)
                                       and ceil(months_between(a.applied_date,add_months(to_date(t.run_month,'yyyy/mm'),1))) < = 0
                                       and a.app_status in ('Approve','Reject','Cancel')
      )
      SELECT  r.app_id, r.agreement_no

             , sum(case when r.ref_app_status = 'Reject' and month_since_offer >=-3 then 1 else 0 end ) ref_fam_d24_reject_3m_cnt
             , sum(case when r.ref_app_status = 'Reject' and month_since_offer >=-6 then 1 else 0 end ) ref_fam_d24_reject_6m_cnt
             , sum(case when r.ref_app_status = 'Reject' and month_since_offer >=-9 then 1 else 0 end ) ref_fam_d24_reject_9m_cnt
             , sum(case when r.ref_app_status = 'Reject' and month_since_offer >=-12 then 1 else 0 end ) ref_fam_d24_reject_12m_cnt
             , sum(case when r.ref_app_status = 'Reject' and month_since_offer >=-24 then 1 else 0 end ) ref_fam_d24_reject_24m_cnt

             , sum(case when r.ref_app_status = 'Approve' and month_since_offer >=-3 then 1 else 0 end ) ref_fam_d24_approve_3m_cnt
             , sum(case when r.ref_app_status = 'Approve' and month_since_offer >=-6 then 1 else 0 end ) ref_fam_d24_approve_6m_cnt
             , sum(case when r.ref_app_status = 'Approve' and month_since_offer >=-9 then 1 else 0 end ) ref_fam_d24_approve_9m_cnt
             , sum(case when r.ref_app_status = 'Approve' and month_since_offer >=-12 then 1 else 0 end ) ref_fam_d24_approve_12m_cnt
             , sum(case when r.ref_app_status = 'Approve' and month_since_offer >=-24 then 1 else 0 end ) ref_fam_d24_approve_24m_cnt

             , sum(case when r.ref_app_status = 'Cancel' and month_since_offer >=-3 then 1 else 0 end ) ref_fam_d24_Cancel_3m_cnt
             , sum(case when r.ref_app_status = 'Cancel' and month_since_offer >=-6 then 1 else 0 end ) ref_fam_d24_Cancel_6m_cnt
             , sum(case when r.ref_app_status = 'Cancel' and month_since_offer >=-9 then 1 else 0 end ) ref_fam_d24_Cancel_9m_cnt
             , sum(case when r.ref_app_status = 'Cancel' and month_since_offer >=-12 then 1 else 0 end ) ref_fam_d24_Cancel_12m_cnt
             , sum(case when r.ref_app_status = 'Cancel' and month_since_offer >=-24 then 1 else 0 end ) ref_fam_d24_Cancel_24m_cnt

             , count(distinct (case when r.ref_app_status = 'Reject' and month_since_offer >=-3 then app_id_c_ref  end )) ref_fam_d24_reject_3m_headcnt
             , count(distinct (case when r.ref_app_status = 'Reject' and month_since_offer >=-6 then app_id_c_ref  end )) ref_fam_d24_reject_6m_headcnt
             , count(distinct (case when r.ref_app_status = 'Reject' and month_since_offer >=-9 then app_id_c_ref  end )) ref_fam_d24_reject_9m_headcnt
             , count(distinct (case when r.ref_app_status = 'Reject' and month_since_offer >=-12 then app_id_c_ref  end )) ref_fam_d24_reject_12m_headcnt
             , count(distinct (case when r.ref_app_status = 'Reject' and month_since_offer >=-24 then app_id_c_ref  end )) ref_fam_d24_reject_24m_headcnt

             , count(distinct (case when r.ref_app_status = 'Approve' and month_since_offer >=-3 then app_id_c_ref end )) ref_fam_d24_approve_3m_headcnt
             , count(distinct (case when r.ref_app_status = 'Approve' and month_since_offer >=-6 then app_id_c_ref end )) ref_fam_d24_approve_6m_headcnt
             , count(distinct (case when r.ref_app_status = 'Approve' and month_since_offer >=-9 then app_id_c_ref end )) ref_fam_d24_approve_9m_headcnt
             , count(distinct (case when r.ref_app_status = 'Approve' and month_since_offer >=-12 then app_id_c_ref end )) ref_fam_d24_approve_12m_headcnt
             , count(distinct (case when r.ref_app_status = 'Approve' and month_since_offer >=-24 then app_id_c_ref end )) ref_fam_d24_approve_24m_headcnt

             , -max(case when r.ref_app_status = 'Approve' then month_since_offer end) ref_fam_d24_approve_last
             , -min(case when r.ref_app_status = 'Approve' then month_since_offer end) ref_fam_d24_approve_first
             , -max(case when r.ref_app_status = 'Reject' then month_since_offer end) ref_fam_d24_reject_last
             , -min(case when r.ref_app_status = 'Reject' then month_since_offer end) ref_fam_d24_reject_first
      from quan_tbl_final_11_1 r
      group by r.agreement_no, r.app_id;

------------------------------ Linkage Information: Reference application information of the same reference                                  
create table quan_tbl_final_11_4 as
      with quan_tbl_final_11_2 as
      (
      SELECT  t.agreement_no,
             t.app_id,
             t.applied_date,
             t.run_month,
             r.app_id_c_ref,
             a.reject_stage ref_reject_stage,
             a.reject_reason ref_reject_reason,
             a.app_status ref_app_status,
             a.applied_date ref_applied_date,
             a.agreement_no ref_agreement_no,
             ceil(months_between(a.applied_date,t.applied_date)) month_since_offer
      from quan_tbl_final_0 t
      left join quan_tbl_final_4_1 r on t.agreement_no = r.agreement_no and t.app_id=r.app_id and is_spouse + is_owner + r.is_reference + is_relative = 0 and is_same_ref = 1
      left join pol_tbl_raw_app_data a on a.app_id = r.app_id_c_ref
                                       and add_months(a.applied_date,12) > add_months(to_date(t.run_month,'yyyy/mm'),1)
                                       and ceil(months_between(a.applied_date,add_months(to_date(t.run_month,'yyyy/mm'),1))) < = 0
                                       and a.app_status in ('Approve','Reject','Cancel')
      )
      SELECT  r.app_id, r.agreement_no

             , sum(case when r.ref_app_status = 'Reject' and month_since_offer >=-3 then 1 else 0 end ) ref_ref_d24_reject_3m_cnt
             , sum(case when r.ref_app_status = 'Reject' and month_since_offer >=-6 then 1 else 0 end ) ref_ref_d24_reject_6m_cnt
             , sum(case when r.ref_app_status = 'Reject' and month_since_offer >=-9 then 1 else 0 end ) ref_ref_d24_reject_9m_cnt
             , sum(case when r.ref_app_status = 'Reject' and month_since_offer >=-12 then 1 else 0 end ) ref_ref_d24_reject_12m_cnt
             , sum(case when r.ref_app_status = 'Reject' and month_since_offer >=-24 then 1 else 0 end ) ref_ref_d24_reject_24m_cnt

             , sum(case when r.ref_app_status = 'Approve' and month_since_offer >=-3 then 1 else 0 end ) ref_ref_d24_approve_3m_cnt
             , sum(case when r.ref_app_status = 'Approve' and month_since_offer >=-6 then 1 else 0 end ) ref_ref_d24_approve_6m_cnt
             , sum(case when r.ref_app_status = 'Approve' and month_since_offer >=-9 then 1 else 0 end ) ref_ref_d24_approve_9m_cnt
             , sum(case when r.ref_app_status = 'Approve' and month_since_offer >=-12 then 1 else 0 end ) ref_ref_d24_approve_12m_cnt
             , sum(case when r.ref_app_status = 'Approve' and month_since_offer >=-24 then 1 else 0 end ) ref_ref_d24_approve_24m_cnt

             , sum(case when r.ref_app_status = 'Cancel' and month_since_offer >=-3 then 1 else 0 end ) ref_ref_d24_Cancel_3m_cnt
             , sum(case when r.ref_app_status = 'Cancel' and month_since_offer >=-6 then 1 else 0 end ) ref_ref_d24_Cancel_6m_cnt
             , sum(case when r.ref_app_status = 'Cancel' and month_since_offer >=-9 then 1 else 0 end ) ref_ref_d24_Cancel_9m_cnt
             , sum(case when r.ref_app_status = 'Cancel' and month_since_offer >=-12 then 1 else 0 end ) ref_ref_d24_Cancel_12m_cnt
             , sum(case when r.ref_app_status = 'Cancel' and month_since_offer >=-24 then 1 else 0 end ) ref_ref_d24_Cancel_24m_cnt

             , count(distinct (case when r.ref_app_status = 'Reject' and month_since_offer >=-3 then app_id_c_ref  end )) ref_ref_d24_reject_3m_headcnt
             , count(distinct (case when r.ref_app_status = 'Reject' and month_since_offer >=-6 then app_id_c_ref  end )) ref_ref_d24_reject_6m_headcnt
             , count(distinct (case when r.ref_app_status = 'Reject' and month_since_offer >=-9 then app_id_c_ref  end )) ref_ref_d24_reject_9m_headcnt
             , count(distinct (case when r.ref_app_status = 'Reject' and month_since_offer >=-12 then app_id_c_ref  end )) ref_ref_d24_reject_12m_headcnt
             , count(distinct (case when r.ref_app_status = 'Reject' and month_since_offer >=-24 then app_id_c_ref  end )) ref_ref_d24_reject_24m_headcnt

             , count(distinct (case when r.ref_app_status = 'Approve' and month_since_offer >=-3 then app_id_c_ref end )) ref_ref_d24_approve_3m_headcnt
             , count(distinct (case when r.ref_app_status = 'Approve' and month_since_offer >=-6 then app_id_c_ref end )) ref_ref_d24_approve_6m_headcnt
             , count(distinct (case when r.ref_app_status = 'Approve' and month_since_offer >=-9 then app_id_c_ref end )) ref_ref_d24_approve_9m_headcnt
             , count(distinct (case when r.ref_app_status = 'Approve' and month_since_offer >=-12 then app_id_c_ref end )) ref_ref_d24_approve_12m_headcnt
             , count(distinct (case when r.ref_app_status = 'Approve' and month_since_offer >=-24 then app_id_c_ref end )) ref_ref_d24_approve_24m_headcnt

             , -max(case when r.ref_app_status = 'Approve' then month_since_offer end) ref_ref_d24_approve_last
             , -min(case when r.ref_app_status = 'Approve' then month_since_offer end) ref_ref_d24_approve_first
             , -max(case when r.ref_app_status = 'Reject' then month_since_offer end) ref_ref_d24_reject_last
             , -min(case when r.ref_app_status = 'Reject' then month_since_offer end) ref_ref_d24_reject_first
      from quan_tbl_final_11_2 r
      group by r.agreement_no, r.app_id;


------------------------------  Behavior: DPD information of the primary application
create table quan_tbl_final_12 as
     with quan_tbl_final_12_0 as
     (
     select a.agreement_no,a.app_id,a.applied_date, max(c.dpd) linkDPD_24M,
           max(c.dpdall) linkDPDALL_24M, count(distinct b.app_id_c_ref) linkAPPS_24M
     from quan_tbl_final_0 a
     left join quan_tbl_final_4_1 b on a.app_id=b.app_id and a.agreement_no=b.agreement_no
     left join (select * from ntbv3_esb_raw_beh_data
              union all
              select * from ntbv3_esb_raw_beh_data_update_bk -- backup of ntbv3_esb_raw_beh_data_update
              ) c on c.agreementno = b.agreement_no
     inner join (select * from pol_tbl_raw_app_data
     where trunc(nvl(dateact, datel), 'mm') between
        add_months(trunc(sysdate)-1, -47) and add_months(trunc(sysdate)-1, -24) )dd
        --add_months(trunc(sysdate,'mm'),-23) and trunc(sysdate,'mm') ) dd
        on dd.app_id=b.app_id_c_ref and c.report_month=a.run_month
     group by a.agreement_no,a.applied_date,a.app_id
     ),
     quan_tbl_final_12_1 as
     (
     select a.agreement_no,a.app_id,a.applied_date,max(c.dpd) linkDPD_12M,
           max(c.dpdall) linkDPDALL_12M, count(distinct b.app_id_c_ref) linkAPPS_12M
     from quan_tbl_final_0 a
     left join quan_tbl_final_4_1 b on a.app_id=b.app_id and a.agreement_no=b.agreement_no
     left join (select * from ntbv3_esb_raw_beh_data
              union all
              select * from ntbv3_esb_raw_beh_data_update_bk -- backup of ntbv3_esb_raw_beh_data_update
              ) c on c.agreementno = b.agreement_no
     inner join (select * from pol_tbl_raw_app_data
     where trunc(nvl(dateact, datel), 'mm') between
           add_months(trunc(sysdate)-1, -35) and add_months(trunc(sysdate)-1, -24) )dd
           --add_months(trunc(sysdate,'mm'),-11) and trunc(sysdate,'mm') ) dd
           on dd.app_id=b.app_id_c_ref and c.report_month=a.run_month
     group by a.agreement_no,a.applied_date,a.app_id
     )
    select s.agreement_no,s.app_id, nvl(linkdpd_24m, -1) linkdpd_24m, nvl(linkdpdall_24m, -1) linkdpdall_24m,
           nvl(linkapps_24m, -1) linkapps_24m, nvl(linkdpd_12m, -1) linkdpd_12m,
           nvl(linkdpdall_12m, -1) linkdpdall_12m, nvl(linkapps_12m, -1) linkapps_12m
    from quan_tbl_final_0 s
    left join quan_tbl_final_12_0 t0 on s.agreement_no = t0.agreement_no and s.app_id=t0.app_id --T LA edit
    left join quan_tbl_final_12_1 t1 on s.agreement_no = t1.agreement_no and s.app_id=t1.app_id;


------------------------------ Behavior: Information of early closed loans (i.e. Loans which are closed before planned termination)
create table quan_tbl_final_16 as
              with base_account as (
                                   select a.account_no, a.appl_id, a.contract_no, nvl(b2.cif_nb, b1.cif) cif_nb 
                                   from sdm_feprd.sdm_com_crc_cas a
                                   left join pol_tbl_tuan_cif_data b1 on a.appl_id = b1.app_id_c
                                   left join pol_tbl_raw_app_data b2 on a.appl_id = b2.app_id
                                   left join quan_tbl_final_0 c on nvl(b2.cif_nb, b1.cif) = c.cif_nb
                                   where c.cif_nb is not null
                                   ),
                                   
                   base as (
                           select account_no, nvl(cel_epp_auth_code, concat(concat(to_char(created_dt),'_'),to_char(end_dt))) cel_epp_auth_code, 
                           cel_epp_close_date, end_dt , created_dt
                           from sdm_feprd.sdm_com_crc_loan_full
                           where cel_epp_close_date is not null
                           and end_dt is not null
                           and account_no in (select distinct account_no from base_account)
                           ),
                           
                   quan_tbl_final_16_3 as
                           (
                           select distinct account_no, cel_epp_auth_code, created_dt, cel_epp_close_date, end_dt 
                           from base
                           ),
                           
                   quan_tbl_final_16_1 as
                           (
                           select a.app_id, a.agreement_no, a.applied_date, c.agreement_no_prev, c.app_id_prev, 
                           NVL(t3.account_no,'null') account_no_prev, nvl(t4.cel_epp_auth_code,'null') cel_epp_auth_code,
                           t4.cel_epp_close_date actual_close_date_cardloan, dd.ACTUAL_CLOSE_DATE actual_close_date_loan
                           from quan_tbl_final_0 a
                           left join pol_tbl_tuan_previous_app c on c.agreement_no = a.agreement_no and c.app_id_c = a.app_id and c.approved_prev = 1 
                           left join sdm_feprd.sdm_fin_loan_parameter dd on c.agreement_no_prev=dd.contract_no and dd.contract_st in ('C')
                           left join SDM_FEPRD.SDM_COM_CRC_CAS t3 on c.app_id_prev = t3.appl_id
                           left join quan_tbl_final_16_3 t4 on t3.account_no = t4.account_no
                           ),
                           
                   quan_tbl_final_16_2  as
                           (
                           select  a.app_id, a.agreement_no, a.applied_date, c.agreement_no_prev, c.app_id_prev, 
                           NVL(t3.account_no,'null') account_no_prev, nvl(t4.cel_epp_auth_code,'null') cel_epp_auth_code, 
                           max(dd.duedate) as plan_close_date_loan, t4.end_dt plan_close_date_cardloan 
                           from quan_tbl_final_0 a
                           left join pol_tbl_tuan_previous_app c on c.agreement_no=a.agreement_no and c.app_id_c = a.app_id and c.approved_prev=1
                           left join SDM_FEPRD.SDM_COL_PAYMENT_SCHEDULE dd on c.agreement_no_prev=dd.agreementno 
                           left join SDM_FEPRD.SDM_COM_CRC_CAS t3 on c.app_id_prev = t3.appl_id
                           left join quan_tbl_final_16_3 t4 on t3.account_no = t4.account_no      
                           group by a.app_id, a.agreement_no, a.applied_date, c.agreement_no_prev, c.app_id_prev, t3.account_no,
                           t4.cel_epp_auth_code, t4.end_dt
                           ),
                           
                   quan_tbl_final_16_0  as
                           (
                           select  a.*, b.plan_close_date_loan
                           from quan_tbl_final_16_1 a
                           left join quan_tbl_final_16_2 b 
                           on a.agreement_no = b.agreement_no and a.applied_date=b.applied_date and a.agreement_no_prev=b.agreement_no_prev
                           and a.account_no_prev = b.account_no_prev and a.cel_epp_auth_code = b.cel_epp_auth_code
                           ),
                           
                   quan_tbl_final_16_4 as
                           (
                           select  agreement_no,app_id,applied_date,
                           sum(case when a.actual_close_date_loan is not null and a.actual_close_date_loan != to_date('31/12/2100','dd/mm/yyyy') and a.plan_close_date_loan is not null
                           and add_months(a.actual_close_date_loan,1) <= plan_close_date_loan then 1 else 0 end) 
                           as total_early_closed_loan,      
      
                               
                           max(case when a.actual_close_date_loan is not null and a.actual_close_date_loan != to_date('31/12/2100','dd/mm/yyyy') and a.plan_close_date_loan is not null
                           and add_months(a.actual_close_date_loan,1)<=plan_close_date_loan then round(months_between(plan_close_date_loan,actual_close_date_loan),0) else 0 end)       
                           as max_early_closed_loan,
            
                           sum(case when a.actual_close_date_loan is not null and a.actual_close_date_loan != to_date('31/12/2100','dd/mm/yyyy') and a.plan_close_date_loan is not null then 1 else 0 end) 
                           as number_active_acct_loan,
            
                           sum(case when a.actual_close_date_loan is not null and a.actual_close_date_loan != to_date('31/12/2100','dd/mm/yyyy') and a.plan_close_date_loan is not null
                           and add_months(a.actual_close_date_loan,6)<=plan_close_date_loan then 1 else 0 end) as total_early_closed_6m_loan,
      
                                
                           sum(case when a.actual_close_date_loan is not null and a.actual_close_date_loan != to_date('31/12/2100','dd/mm/yyyy') and a.plan_close_date_loan is not null
                           and add_months(a.actual_close_date_loan,3)<=plan_close_date_loan then 1 else 0 end) as total_early_closed_3m_loan,

                           sum(case when a.actual_close_date_loan is not null and a.actual_close_date_loan != to_date('31/12/2100','dd/mm/yyyy') and a.plan_close_date_loan is not null
                           and add_months(a.actual_close_date_loan,12)<=plan_close_date_loan then 1 else 0 end) as total_early_closed_12m_loan,


                           sum(case when a.actual_close_date_loan is not null and a.actual_close_date_loan != to_date('31/12/2100','dd/mm/yyyy') and a.plan_close_date_loan is not null
                           and add_months(a.actual_close_date_loan,24)<=plan_close_date_loan then 1 else 0 end) as total_early_closed_24m_loan


                           from quan_tbl_final_16_0 a
                           group by agreement_no,applied_date,app_id
                           ),
                           
                   quan_tbl_final_16 as
                           (
                           select a.*, b.total_early_closed_loan as total_early_closed,
                           b.max_early_closed_loan as max_early_closed,
                           number_active_acct_loan as number_active_acct,  
                           b.total_early_closed_3m_loan as total_early_closed_3m,                             
                           b.total_early_closed_6m_loan as total_early_closed_6m,                             
                           b.total_early_closed_12m_loan as total_early_closed_12m,                                                    
                           b.total_early_closed_24m_loan as total_early_closed_24m                                                  
                           from quan_tbl_final_0 a
                           left join quan_tbl_final_16_4 b on a.agreement_no = b.agreement_no
                           )
                           
      select agreement_no, applied_date, app_id, total_early_closed, max_early_closed, number_active_acct,
      total_early_closed_3m, total_early_closed_6m, total_early_closed_12m, total_early_closed_24m
      from quan_tbl_final_16;


------------------------------ Behavior: Base table to generate Table 8, and some features in Final Table
create table quan_tbl_final_8_0 as
    select a.app_id,a.agreement_no agreement_no1,a.applied_date,b.*,
    row_number() over (partition by a.app_id order by b.report_month desc) as cnt_row1,
    row_number() over (partition by a.app_id order by b.report_month asc) as cnt_row2,
    lag(dpdall,1) over (partition by a.app_id order by b.report_month asc) as dpdall_lag,
    lag(cfall,1) over (partition by a.app_id order by b.report_month asc) as cfall_lag,
    lag(enrall,1) over (partition by a.app_id order by b.report_month asc) as enrall_lag,
    lag(numactl,1) over (partition by a.app_id order by b.report_month asc) as numactl_lag,
    lag(numcll,1) over (partition by a.app_id order by b.report_month asc) as numcll_lag
    from quan_tbl_final_0 a
    left join (select * from ntbv3_esb_raw_beh_data
              where enrall >0
              union all
              select * from ntbv3_esb_raw_beh_data_update
              where enrall >0
              ) b on a.agreement_no=b.agreementno;


------------------------------ Behavior: DPD information; ENR; Cash Flow; Arrear; Active Loans and Closed Loan
create table quan_tbl_final_8 as
    select a.app_id,a.agreement_no1,a.applied_date,
    max(early_pmt_day_max) as max_early_pmt_pay_max,
    min(early_pmt_day_max) as min_early_pmt_pay_max,
    min(early_pmt_day_min) as min_early_pmt_pay_min,
    max(early_pmt_day_min) as max_early_pmt_pay_min,
    max(cnt_row1) as max_life_time,
    max(case when report_month is null then -1 else dpdall end) as max_dpd_ever,
    max(case when report_month is null then -1 when cnt_row1<=3 then dpdall else -1 end) as max_dpd_last_3_active_months,
    max(case when report_month is null then -1 when cnt_row1<=6 and cnt_row1>3 then dpdall else -1 end) as max_dpd_last_3_6_active_months,
    max(case when report_month is null then -1 when cnt_row1<=12 and cnt_row1>6 then dpdall else -1 end) as max_dpd_last_6_12_active_months,
    max(case when report_month is null then -1 when cnt_row1<=24 and cnt_row1>12 then dpdall else -1 end) as max_dpd_last_12_24_active_months,
    max(case when report_month is null then -1 else months_between(applied_date,to_date(report_month,'yyyy/mm')) end) as max_months_journey,
    min(case when report_month is null then -1 else months_between(applied_date,to_date(report_month,'yyyy/mm')) end) as min_months_journey,
    max(months_between(a.applied_date,case when DPDall>0 then to_date(report_month,'yyyy/mm') else to_date('01Jan1900') end)) as max_months_DPD_0,
    min(months_between(a.applied_date,case when DPDall>0 then to_date(report_month,'yyyy/mm') else to_date('01Jan1900') end)) as min_months_DPD_0,
    sum(case when report_month is null then -1 when dpdall>0 then 1 else 0 end)/max(case when report_month is null then 1 else cnt_row2 end) as months_dpd_0_cus_journey,
    sum(case when report_month is null then -1 when dpdall>10 then 1 else 0 end)/max(case when report_month is null then 1 else cnt_row2 end) as months_dpd_10_cus_journey,
    sum(case when report_month is null then -1 when dpdall>30 then 1 else 0 end)/max(case when report_month is null then 1 else cnt_row2 end) as months_dpd_30_cus_journey,
    avg(case when report_month is null then -1 when cnt_row1<=3 then enrall else null end) as avg_enr_all_last_3m,
    avg(case when report_month is null then -1 when cnt_row1> 3 and cnt_row1<=6 then enrall else null end) as avg_enr_all_last_3_6m,
    avg(case when report_month is null then -1 when cnt_row1> 6 and cnt_row1<=12 then enrall else null end) as avg_enr_all_last_6_12m,
    avg(case when report_month is null then -1 when cnt_row1> 12 and cnt_row1<=24 then enrall else null end) as avg_enr_all_last_12_24m,
    avg(case when report_month is null then -1 when cnt_row1<=3 then enrall else null end)/
    max(case when report_month is null then 1 when cnt_row1<=3 then enrall else null end) as avg_enr_ratio_last_3m,
    avg(case when report_month is null then -1 when cnt_row1> 3 and cnt_row1<=6 then enrall else null end)/
    max(case when report_month is null then 1 when cnt_row1> 3 and cnt_row1<=6 then enrall else null end) as avg_enr_ratio_last_3_6m,
    avg(case when report_month is null then -1 when cnt_row1> 6 and cnt_row1<=12 then enrall else null end)/
    max(case when report_month is null then 1 when cnt_row1> 6 and cnt_row1<=12 then enrall else null end) as avg_enr_ratio_last_6_12m,
    avg(case when report_month is null then -1 when cnt_row1> 12 and cnt_row1<=24 then enrall else null end)/
    max(case when report_month is null then 1 when cnt_row1> 12 and cnt_row1<=24 then enrall else null end) as avg_enr_ratio_last_12_24m,
    avg(case when report_month is null then -1 else enrall end)/
    max(case when report_month is null then 1 else enrall end) as avg_enr_ratio_ever,
    max(case when report_month is null then -1 else cfall end) as max_cf_all_ever,
    max(case when report_month is null then -1 else amdall end) as max_amd_all_ever,
    max(case when report_month is null then -1 when cnt_row1> 0 and cnt_row1<=3 then cfall else -1 end) as max_cf_all_last_3m,
    max(case when report_month is null then -1 when cnt_row1> 3 and cnt_row1<=6 then cfall else -1 end) as max_cf_all_last_3_6m,
    max(case when report_month is null then -1 when cnt_row1> 6 and cnt_row1<=12 then cfall else -1 end) as max_cf_all_last_6_12m,
    max(case when report_month is null then -1 when cnt_row1> 12 and cnt_row1<=24 then cfall else -1 end) as max_cf_all_last_12_24m,
    max((case when report_month is null then -1 else cfall end)/(case when report_month is null then 1 else greatest(amdall,1) end)) max_cf_amd_ever,
    max((case when report_month is null then -1 when cnt_row1> 0 and cnt_row1<=3 then cfall else -1 end)/
    (case when report_month is null then -1 when cnt_row1> 3 and cnt_row1<=6 then greatest(amdall,1) else 1 end)) as max_cf_amd_last_3m,
    max((case when report_month is null then -1 when cnt_row1> 3 and cnt_row1<=6 then cfall else -1 end)/
    (case when report_month is null then -1 when cnt_row1> 3 and cnt_row1<=6 then greatest(amdall,1) else 1 end)) as max_cf_amd_last_3_6m,
    max((case when report_month is null then -1 when cnt_row1> 6 and cnt_row1<=12 then cfall else -1 end)/
    (case when report_month is null then -1 when cnt_row1> 6 and cnt_row1<=12 then greatest(amdall,1) else 1 end)) as max_cf_amd_last_6_12m,
    max((case when report_month is null then -1 when cnt_row1> 12 and cnt_row1<=24 then cfall else -1 end)/
    (case when report_month is null then -1 when cnt_row1> 12 and cnt_row1<=24 then greatest(amdall,1) else 1 end)) as max_cf_amd_last_12_24m,
    max(case when report_month is null then -1 else arrall end) as max_arr_ever,
    max(case when report_month is null then -1 when cnt_row1> 0 and cnt_row1<=3 then arrall else -1 end) as max_arr_last_3m,
    max(case when report_month is null then -1 when cnt_row1> 3 and cnt_row1<=6 then arrall else -1 end) as max_arr_last_3_6m,
    max(case when report_month is null then -1 when cnt_row1> 6 and cnt_row1<=12 then arrall else -1 end) as max_arr_last_6_12m,
    max(case when report_month is null then -1 when cnt_row1> 12 and cnt_row1<=24 then arrall else -1 end) as max_arr_last_12_24m,
    max((case when report_month is null then -1 else nvl(arrall,-1) end)/(case when report_month is null then 1 else nvl(enrall,1) end)) as max_arr_enr_ever,
    max((case when report_month is null then -1 when cnt_row1> 0 and cnt_row1<=3 then nvl(arrall,-1) else -1 end)/
    (case when report_month is null then 1 when cnt_row1> 0 and cnt_row1<=3 then nvl(enrall,1) else 1 end)) as max_arr_enr_last_3m,
    max((case when report_month is null then -1 when cnt_row1> 3 and cnt_row1<=6 then nvl(arrall,-1) else -1 end)/
    (case when report_month is null then 1 when cnt_row1> 3 and cnt_row1<=6 then nvl(enrall,1) else 1 end)) as max_arr_enr_last_3_6m,
    max((case when report_month is null then -1 when cnt_row1> 6 and cnt_row1<=12 then nvl(arrall,-1) else -1 end)/
    (case when report_month is null then 1 when cnt_row1> 6 and cnt_row1<=12 then nvl(enrall,1) else 1 end)) as max_arr_enr_last_6_12m,
    max((case when report_month is null then -1 when cnt_row1> 12 and cnt_row1<=24 then nvl(arrall,-1) else -1 end)/
    (case when report_month is null then 1 when cnt_row1> 12 and cnt_row1<=24 then nvl(enrall,1) else 1 end)) as max_arr_enr_last_12_24m,
    max(case when report_month is null then -1 else nvl(limall,-1) end) as max_lim_ever,
    max(case when report_month is null then -1 when cnt_row1> 0 and cnt_row1<=3 then nvl(limall,-1) else -1 end) as max_lim_last_3m,
    max(case when report_month is null then -1 when cnt_row1> 3 and cnt_row1<=6 then nvl(limall,-1) else -1 end) as max_lim_last_3_6m,
    max(case when report_month is null then -1 when cnt_row1> 6 and cnt_row1<=12 then nvl(limall,-1) else -1 end) as max_lim_last_6_12m,
    max(case when report_month is null then -1 when cnt_row1> 12 and cnt_row1<=24 then nvl(limall,-1) else -1 end) as max_lim_last_12_24m,
    sum(case when cfall>amdall and amdall>0 then 1 else 0 end) as cf_amd_comp_ever,
    sum(case when cnt_row1> 0 and cnt_row1<=3 and cfall>amdall and amdall>0 then 1 else 0 end) as cf_amd_comp_last_3m,
    sum(case when cnt_row1> 3 and cnt_row1<=6 and cfall>amdall and amdall>0 then 1 else 0 end) as cf_amd_comp_last_3_6m,
    sum(case when cnt_row1> 6 and cnt_row1<=12 and cfall>amdall and amdall>0 then 1 else 0 end) as cf_amd_comp_last_6_12m,
    sum(case when cnt_row1> 12 and cnt_row1<=24 and cfall>amdall and amdall>0 then 1 else 0 end) as cf_amd_comp_last_12_24m,
    max(case when report_month is null then 0 else nvl(numactl,0) end) as max_actl_ever,
    max(case when cnt_row1> 0 and cnt_row1<=3 then nvl(numactl,0) else 0 end) as max_actl_last_3m,
    max(case when cnt_row1> 3 and cnt_row1<=6 then nvl(numactl,0) else 0 end) as max_actl_last_3_6m,
    max(case when cnt_row1> 6 and cnt_row1<=12 then nvl(numactl,0) else 0 end) as max_actl_last_6_12m,
    max(case when cnt_row1> 12 and cnt_row1<=24 then nvl(numactl,0) else 0 end) as max_actl_last_12_24m,
    max(case when report_month is null then 0 else nvl(numcll,0) end) as max_cll_ever,
    max(case when cnt_row1> 0 and cnt_row1<=3 then nvl(numcll,0) else 0 end) as max_cll_last_3m,
    max(case when cnt_row1> 3 and cnt_row1<=6 then nvl(numcll,0) else 0 end) as max_cll_last_3_6m,
    max(case when cnt_row1> 6 and cnt_row1<=12 then nvl(numcll,0) else 0 end) as max_cll_last_6_12m,
    max(case when cnt_row1> 12 and cnt_row1<=24 then nvl(numcll,0) else 0 end) as max_cll_last_12_24m,
    max(case when report_month is null then 0 else nvl(numcll,0)+nvl(numactl,0) end) as max_alctcll_ever,
    max(case when cnt_row1> 0 and cnt_row1<=3 then nvl(numcll,0)+nvl(numactl,0) else 0 end) as max_atclcll_last_3m,
    max(case when cnt_row1> 3 and cnt_row1<=6 then nvl(numcll,0)+nvl(numactl,0) else 0 end) as max_atclcll_last_3_6m,
    max(case when cnt_row1> 6 and cnt_row1<=12 then nvl(numcll,0)+nvl(numactl,0) else 0 end) as max_atclcll_last_6_12m,
    max(case when cnt_row1> 12 and cnt_row1<=24 then nvl(numcll,0)+nvl(numactl,0) else 0 end) as max_atclcll_last_12_24m,
    max(case when nvl(numactl_lag,0)<numactl then cnt_row1 else 0 end) as max_months_last_active,
    max(case when nvl(numcll_lag,0)<numcll then cnt_row1 else 0 end) as max_months_last_close
    from quan_tbl_final_8_0 a
    group by a.app_id,a.agreement_no1,a.applied_date;

-------------------------------------------------------------
--STEP 3: Final Table
-------------------------------------------------------------

create table quan_tbl_final_all as
-----------------------------------------------------------------------------------
--quan_tbl_final_8_1
with g as
     (
    SELECT app_id,
    MAX(consecutive_dpd0) AS max_months_consecutive_dpd0
    FROM quan_tbl_final_8_0 a
    MATCH_RECOGNIZE (PARTITION BY app_id ORDER BY report_month MEASURES COUNT(1) AS consecutive_dpd0
       PATTERN(a+) DEFINE a AS dpdall >0)
    GROUP BY app_id
    ),
--------------------------------------------------
--quan_tbl_final_8_2
    h as
    (
    SELECT app_id,
    MAX(consecutive_dpd0) AS max_months_consecutive_dpd10
    FROM quan_tbl_final_8_0 a
    MATCH_RECOGNIZE (PARTITION BY app_id ORDER BY report_month MEASURES COUNT(1) AS consecutive_dpd0
       PATTERN(a+) DEFINE a AS dpdall >10)
    GROUP BY app_id
    ),
--------------------------------------------------
--quan_tbl_final_8_3
    i as
    (
    SELECT app_id,
    MAX(consecutive_dpd0) AS max_months_consecutive_dpd30
    FROM quan_tbl_final_8_0 a
    MATCH_RECOGNIZE (PARTITION BY app_id ORDER BY report_month MEASURES COUNT(1) AS consecutive_dpd0
       PATTERN(a+) DEFINE a AS dpdall >30)
    GROUP BY app_id
    ),
--------------------------------------------------
--quan_tbl_final_8_4
    k as
    (
    SELECT app_id,
    MAX(consecutive_dpd0) AS max_months_consecutive_dpd0_3m
    FROM quan_tbl_final_8_0 a
    MATCH_RECOGNIZE (PARTITION BY app_id ORDER BY report_month MEASURES COUNT(1) AS consecutive_dpd0
       PATTERN(a+) DEFINE a AS dpdall >0 and cnt_row1<=3 and cnt_row1>0)
    GROUP BY app_id
    ),
--------------------------------------------------
--quan_tbl_final_8_5
    l as
    (
    SELECT app_id,
    MAX(consecutive_dpd0) AS max_months_consecutive_dpd0_3_6m
    FROM quan_tbl_final_8_0 a
    MATCH_RECOGNIZE (PARTITION BY app_id ORDER BY report_month MEASURES COUNT(1) AS consecutive_dpd0
       PATTERN(a+) DEFINE a AS dpdall >0 and cnt_row1<=6 and cnt_row1>3)
    GROUP BY app_id
    ),
---------------------------------------------------
--quan_tbl_final_8_6
    m as
    (
    SELECT app_id,
    MAX(consecutive_dpd0) AS max_months_consecutive_dpd0_6_12m
    FROM quan_tbl_final_8_0 a
    MATCH_RECOGNIZE (PARTITION BY app_id ORDER BY report_month MEASURES COUNT(1) AS consecutive_dpd0
       PATTERN(a+) DEFINE a AS dpdall >0 and cnt_row1<=12 and cnt_row1>6)
    GROUP BY app_id
    ),
---------------------------------------------------
--quan_tbl_final_8_7
    n as
    (
    SELECT app_id,
    MAX(consecutive_dpd0) AS max_months_consecutive_dpd0_12_24m
    FROM quan_tbl_final_8_0 a
    MATCH_RECOGNIZE (PARTITION BY app_id ORDER BY report_month MEASURES COUNT(1) AS consecutive_dpd0
       PATTERN(a+) DEFINE a AS dpdall >0 and cnt_row1<=24 and cnt_row1>12)
    GROUP BY app_id
    ),
---------------------------------------------------
--quan_tbl_final_8_8
    o as
    (
    SELECT app_id,
    MAX(consecutive_dpd0) AS max_months_consecutive_equalcf
    FROM quan_tbl_final_8_0 a
    MATCH_RECOGNIZE (PARTITION BY app_id ORDER BY report_month MEASURES COUNT(1) AS consecutive_dpd0
       PATTERN(a+) DEFINE a AS cfall=nvl(cfall_lag,cfall))
    GROUP BY app_id
    )
-----------------------------------------------------------------------------------

    select a.cif_nb,
           a.app_id_xsell, a.applied_date_xsell, a.applied_month_xsell,
           a.app_id, a.applied_date, a.applied_month,
           a.del30_mob12_app, a.del90_mob12_app,
    case when f3.crprodgroup='CDL' then 'X-sell CDL'
         when f3.crprodgroup='TW' then 'X-sell TW'
         when f3.crprodgroup in ('New-to-bank','Fast-track') then 'X-sell PL'
         when f3.crprodgroup in ('Top-up','X-sell') then 'X-sell 2ND' end as product_group_dt2,
    nvl(b.max_dpdall_ref,-1) max_dpdall_ref,nvl(b.max_dpd_ref,-1) max_dpd_ref,
    c.prev_approved,c.prev_rejected,c.ever_rejected_score,c.ever_rejected_cic,c.ever_rejected_bl,
    c.approved_returning_cust,c.ever_returning_cust,
    nvl(t.col_cnt,0) as col_cnt,nvl(t.attempt_cnt_duplicate,0) as attempt_cnt,nvl(t.bad_response,0) bad_response,
    nvl(t.good_response,0) good_response,nvl(t.bad_response_ratio,0) bad_response_ratio,nvl(t.good_response_ratio,0) good_response_ratio,
    nvl(e.max_cic_institution,-1) max_cic_institution,nvl(e.cic_ever_ok,-1) cic_ever_ok,nvl(e.cic_ever_not_ok,-1) cic_ever_not_ok,
    nvl(e.cic_ever_warning,-1) cic_ever_warning,nvl(e.cic_ever_not_found,-1) cic_ever_not_found,
    nvl(e.cic_ever_cannot_check,-1) cic_ever_cannot_check,
    f.MAX_LIFE_TIME,f.MAX_DPD_EVER,f.MAX_DPD_LAST_3_ACTIVE_MONTHS,f.MAX_DPD_LAST_3_6_ACTIVE_MONTHS,
    f.MAX_DPD_LAST_6_12_ACTIVE_MONTHs MAX_DPD_LAST_6_12_ACTIVE_MONTH,
    f.Max_Dpd_Last_12_24_Active_Months Max_Dpd_Last_12_24_Active_Mont,f.MAX_MONTHS_JOURNEY,f.MIN_MONTHS_JOURNEY,
    f.MAX_MONTHS_DPD_0,f.MIN_MONTHS_DPD_0,f.MONTHS_DPD_0_CUS_JOURNEY,
    f.MONTHS_DPD_10_CUS_JOURNEY,f.MONTHS_DPD_30_CUS_JOURNEY,f.AVG_ENR_ALL_LAST_3M,f.AVG_ENR_ALL_LAST_3_6M,
    f.AVG_ENR_ALL_LAST_6_12M,f.AVG_ENR_ALL_LAST_12_24M,
    f.AVG_ENR_RATIO_LAST_3M,f.AVG_ENR_RATIO_LAST_3_6M,f.AVG_ENR_RATIO_LAST_6_12M,f.AVG_ENR_RATIO_LAST_12_24M,
    f.AVG_ENR_RATIO_EVER,f.MAX_CF_ALL_EVER,
    f.MAX_AMD_ALL_EVER,f.MAX_CF_ALL_LAST_3M,f.MAX_CF_ALL_LAST_3_6M,f.MAX_CF_ALL_LAST_6_12M,f.MAX_CF_ALL_LAST_12_24M,f.MAX_CF_AMD_EVER,
    f.MAX_CF_AMD_LAST_3M,f.MAX_CF_AMD_LAST_3_6M,f.MAX_CF_AMD_LAST_6_12M,f.MAX_CF_AMD_LAST_12_24M,f.MAX_ARR_EVER,f.MAX_ARR_LAST_3M,
    f.MAX_ARR_LAST_3_6M,f.MAX_ARR_LAST_6_12M,f.MAX_ARR_LAST_12_24M,f.MAX_ARR_ENR_EVER,f.MAX_ARR_ENR_LAST_3M,f.MAX_ARR_ENR_LAST_3_6M,
    f.MAX_ARR_ENR_LAST_6_12M,f.MAX_ARR_ENR_LAST_12_24M,f.MAX_LIM_EVER,f.MAX_LIM_LAST_3M,f.MAX_LIM_LAST_3_6M,f.MAX_LIM_LAST_6_12M,
    f.MAX_LIM_LAST_12_24M,f.CF_AMD_COMP_EVER,f.CF_AMD_COMP_LAST_3M,f.CF_AMD_COMP_LAST_3_6M,f.CF_AMD_COMP_LAST_6_12M,f.CF_AMD_COMP_LAST_12_24M,
    f.MAX_ACTL_EVER,f.MAX_ACTL_LAST_3M,f.MAX_ACTL_LAST_3_6M,f.MAX_ACTL_LAST_6_12M,f.MAX_ACTL_LAST_12_24M,f.MAX_CLL_EVER,f.MAX_CLL_LAST_3M,
    f.MAX_CLL_LAST_3_6M,f.MAX_CLL_LAST_6_12M,f.MAX_CLL_LAST_12_24M,f.MAX_ALCTCLL_EVER,f.MAX_ATCLCLL_LAST_3M,f.MAX_ATCLCLL_LAST_3_6M,
    f.MAX_ATCLCLL_LAST_6_12M,f.MAX_ATCLCLL_LAST_12_24M,f.max_months_last_active,f.max_months_last_close,--f.MONTHS_FIRST_CLOSE,
    nvl(g.max_months_consecutive_dpd0,0) max_months_consecutive_dpd0,
    nvl(h.max_months_consecutive_dpd10,0) max_months_consecutive_dpd10,
    nvl(i.max_months_consecutive_dpd30,0) max_months_consecutive_dpd30,
    nvl(k.max_months_consecutive_dpd0_3m,0) max_months_consecutive_dpd0_3m,
    nvl(l.max_months_consecutive_dpd0_3_6m,0) max_months_consecutive_dpd0_3_,
    nvl(m.max_months_consecutive_dpd0_6_12m,0) max_months_consecutive_dpd0_6_,
    nvl(n.max_months_consecutive_dpd0_12_24m,0) max_months_consecutive_dpd0_12,
    nvl(o.max_months_consecutive_equalcf,0) max_months_consecutive_equalcf,
    p.MAX_NATIONALID_DATE,p.MIN_NATIONALID_DATE,p.MAX_APP_EMAIL,
    p.SUM_APPEMAIL,p.DISTINCT_APPREGREGION,p.MAX_APPREGREGION,
    p.MIN_APPREGREGION,p.MODE_APPREGREGION,p.DISTINCT_APPRESREGION,
    p.MAX_APPRESREGION,p.MIN_APPRESREGION,p.MODE_APPRESREGION,
    p.MAX_APPRESPERIOD,p.MIN_APPRESPERIOD,p.AVG_APPRESPERIOD,
    p.MAX_APPRESPOW,p.MIN_APPRESPOW,p.AVG_APPRESPOW,p.MAX_BACKUPCONTACTNAME,
    p.MIN_BACKUPCONTACTNAME,p.SUM_BACKUPCONTACTNAME,p.AVG_BACKUPCONTACTNAME,
    p.APPEDUCATION,p.MIN_APPEDUCATION,p.MODE_APPEDUCATION,p.DISTINCT_APPEDUCATION,
    p.MAX_APPWPERIODG,p.MIN_APPWPERIODG,p.MAX_APPWPERIOD,p.MIN_APPWPERIOD,
    p.MAX_APPFAMILYSTATUS,p.MIN_APPFAMILYSTATUS,p.MODE_APPFAMILYSTATUS,
    p.MAX_APPFMQNTY,p.MIN_APPFMQNTY,p.AVG_APPFMQNTY,p.MODE_APPFMQNTY,
    p.MAX_APPCHILDQNTY,p.MIN_APPCHILDQNTY,p.AVG_APPCHILDQNTY,p.MODE_APPCHILDQNTY,
    p.MAX_PRIMARYINCOME,p.MIN_PRIMARYINCOME,p.AVG_PRIMARYINCOME,p.MAX_FAMILYINCOME,
    p.MIN_FAMILYINCOME,p.AVG_FAMILYINCOME,p.MAX_FAMILYEXPENSE,p.MIN_FAMILYEXPENSE,
    p.AVG_FAMILYEXPENSE,p.MAX_BASICEXPENCES,p.MIN_BASICEXPENCES,p.AVG_BASICEXPENCES,
    p.MAX_DIS_INCOME,p.MIN_DIS_INCOME,p.AVG_DIS_INCOME,p.MAX_DIS_F_INCOME,
    p.MIN_DIS_F_INCOME,p.AVG_DIS_F_INCOME,p.MAX_GDSPRICE,p.MIN_GDSPRICE,
    p.AVG_GDSPRICE,p.SUM_GDSPRICE,p.MAX_ADVPAY,p.MIN_ADVPAY,
    p.AVG_ADVPAY,p.MAX_CREDITSUM,p.MIN_CREDITSUM,p.AVG_CREDITSUM,
    p.MAX_CREDITTERM,p.MIN_CREDITTERM,p.AVG_CREDITTERM,
    P.distinct_APPRESPOW,P.MODE_APPRESPOW,
    p.MAX_APPMONTHPAYMENT,p.MIN_APPMONTHPAYMENT,p.AVG_APPMONTHPAYMENT,
    p.MAX_APPINTEREST,p.MIN_APPINTEREST,p.AVG_APPINTEREST,
    p.MODE_APPINTEREST,p.MODE_DOP_MAINGOODSCATEGORY,p.MODE_ASSET_BRAND,
    p.MODE_PORT_NONPORT,p.MODE_PRODUCT_SEGMENT,p.MAX_COMPANY_CATEGORY,
    p.MIN_COMPANY_CATEGORY,p.MODE_COMPANY_CATEGORY,p.MAX_APPDISBCHAN,
    p.MIN_APPDISBCHAN,p.MODE_APPDISBCHAN,
    t.BAD_RESPONSE_CNT,t.BAD_RESPONSE_3M_CNT,t.BAD_RESPONSE_6M_CNT,
    t.BAD_RESPONSE_9M_CNT,t.BAD_RESPONSE_12M_CNT,t.BAD_RESPONSE_24M_CNT,
    t.GOOD_RESPONSE_CNT,t.GOOD_RESPONSE_3M_CNT,t.GOOD_RESPONSE_6M_CNT,
    t.GOOD_RESPONSE_9M_CNT,t.GOOD_RESPONSE_12M_CNT,t.GOOD_RESPONSE_24M_CNT,
    t.ATTEMPT_CNT ATTEMPT_CNT2,t.ATTEMPT_3M_CNT,t.ATTEMPT_6M_CNT,t.ATTEMPT_9M_CNT,
    t.ATTEMPT_12M_CNT,t.ATTEMPT_24M_CNT,t.CONNECT_CNT,t.CONNECT_3M_CNT,
    t.CONNECT_6M_CNT,t.CONNECT_9M_CNT,t.CONNECT_12M_CNT,t.CONNECT_24M_CNT,
    t.CONTACT_CLIENT_CNT,t.CONTACT_CLIENT_3M_CNT,t.CONTACT_CLIENT_6M_CNT,
    t.CONTACT_CLIENT_9M_CNT,t.CONTACT_CLIENT_12M_CNT,t.CONTACT_CLIENT_24M_CNT,
    t.CONTACT_CLIENT_LAST,t.CONTACT_CLIENT_FIRST,t.MONTH_CONTACT_CLIENT_3M,
    t.MONTH_CONTACT_CLIENT_6M,t.MONTH_CONTACT_CLIENT_9M,t.MONTH_CONTACT_CLIENT_12M,
    t.MONTH_CONTACT_CLIENT_24M,t.CONTACT_CNT,t.CONTACT_3M_CNT,t.CONTACT_6M_CNT,t.CONTACT_9M_CNT,
    t.CONTACT_12M_CNT,t.CONTACT_24M_CNT,t.BAD_GOOD_RESPONSE_CNT,t.BAD_GOOD_RESPONSE_3M_PCT,
    t.BAD_GOOD_RESPONSE_6M_PCT,t.BAD_GOOD_RESPONSE_9M_PCT,t.BAD_GOOD_RESPONSE_12M_PCT,
    t.BAD_GOOD_RESPONSE_24M_PCT,t.BAD_ATTEMPT_RESPONSE_CNT,t.BAD_ATTEMPT_RESPONSE_3M_PCT,
    t.BAD_ATTEMPT_RESPONSE_6M_PCT,t.BAD_ATTEMPT_RESPONSE_9M_PCT,t.BAD_ATTEMPT_RESPONSE_12M_PCT,
    t.BAD_ATTEMPT_RESPONSE_24M_PCT,t.BAD_CONNECT_RESPONSE_CNT,t.BAD_CONNECT_RESPONSE_3M_PCT,
    t.BAD_CONNECT_RESPONSE_6M_PCT,t.BAD_CONNECT_RESPONSE_9M_PCT,t.BAD_CONNECT_RESPONSE_12M_PCT,
    t.BAD_CONNECT_RESPONSE_24M_PCT,t.BAD_CONTACT_RESPONSE_CNT,t.BAD_CONTACT_RESPONSE_3M_PCT,
    t.BAD_CONTACT_RESPONSE_6M_PCT,t.BAD_CONTACT_RESPONSE_9M_PCT,t.BAD_CONTACT_RESPONSE_12M_PCT,
    t.BAD_CONTACT_RESPONSE_24M_PCT,t.CONNECT_RATE,t.CONNECT_RATE_3M,t.CONNECT_RATE_6M,
    t.CONNECT_RATE_9M,t.CONNECT_RATE_12M,t.CONNECT_RATE_24M,t.CONTACT_CLIENT_RATE,
    t.CONTACT_CLIENT_RATE_3M,t.CONTACT_CLIENT_RATE_6M,t.CONTACT_CLIENT_RATE_9M,
    t.CONTACT_CLIENT_RATE_12M,t.CONTACT_CLIENT_RATE_24M,
    u.MONTH_ON_FI,u.DPDALL_EVER,u.DPDALL_3MOB,u.DPDALL_4_6MOB,u.DPDALL_7_9MOB,
    u.DPDALL_10_12MOB,u.DPDALL_6MOB,u.DPDALL_7_12MOB,u.DPDALL_9MOB,u.DPDALL_10_18MOB,u.DPDALL_12MOB,
    u.DPDALL_13_24MOB,u.DPDALL_24MOB,u.DPDALL_1DPD_CNT_3MOB,u.DPDALL_1DPD_CNT_4_6MOB,
    u.DPDALL_1DPD_CNT_7_9MOB,u.DPDALL_1DPD_CNT_10_12MOB,u.DPDALL_1DPD_CNT_6MOB,
    u.DPDALL_1DPD_CNT_7_12MOB,u.DPDALL_1DPD_CNT_9MOB,u.DPDALL_1DPD_CNT_10_18MOB,
    u.DPDALL_1DPD_CNT_12MOB,u.DPDALL_1DPD_CNT_24MOB,u.DPDALL_2DPD_CNT_3MOB,
    u.DPDALL_2DPD_CNT_4_6MOB,u.DPDALL_2DPD_CNT_7_9MOB,u.DPDALL_2DPD_CNT_10_12MOB,
    u.DPDALL_2DPD_CNT_6MOB,u.DPDALL_2DPD_CNT_7_12MOB,u.DPDALL_2DPD_CNT_9MOB,
    u.DPDALL_2DPD_CNT_10_18MOB,u.DPDALL_2DPD_CNT_12MOB,u.DPDALL_2DPD_CNT_24MOB,
    u.DPDALL_3DPD_CNT_3MOB,u.DPDALL_3DPD_CNT_4_6MOB,u.DPDALL_3DPD_CNT_7_9MOB,
    u.DPDALL_3DPD_CNT_10_12MOB,u.DPDALL_3DPD_CNT_6MOB,u.DPDALL_3DPD_CNT_7_12MOB,
    u.DPDALL_3DPD_CNT_9MOB,u.DPDALL_3DPD_CNT_10_18MOB,u.DPDALL_3DPD_CNT_12MOB,
    u.DPDALL_3DPD_CNT_24MOB,u.DPDALL_EOM_3MOB,u.DPDALL_EOM_6MOB,u.DPDALL_EOM_9MOB,
    u.DPDALL_EOM_12MOB,u.DPDALL_EOM_24MOB,u.MONTH_LAST1DPD,u.MONTH_LAST2DPD,
    u.MONTH_LAST3DPD,u.MONTH_LAST4DPD,u.MONTH_LAST5DPD,u.MONTH_FIRST1DPD,
    u.MONTH_FIRST2DPD,u.MONTH_FIRST3DPD,u.MONTH_FIRST4DPD,u.MONTH_FIRST5DPD,
    u.ENRALL_MAX_3MOB,u.ENRALL_MAX_6MOB,u.ENRALL_MAX_9MOB,u.ENRALL_MAX_12MOB,
    u.ENRALL_MAX_24MOB,u.ENRALL_AVG_3MOB,u.ENRALL_AVG_6MOB,u.ENRALL_AVG_9MOB,
    u.ENRALL_AVG_12MOB,u.ENRALL_AVG_24MOB,u.MONTH_CNT_0ENR_3MOB,u.MONTH_CNT_0ENR_6MOB,
    u.MONTH_CNT_0ENR_9MOB,u.MONTH_CNT_0ENR_12MOB,u.MONTH_CNT_0ENR_24MOB,u.MONTH_0ENR_LAST,u.MONTH_0ENR_FIRST,
    u.EARLY_PMT_TOL_CNT_3MOB,u.EARLY_PMT_TOL_CNT_6MOB,u.EARLY_PMT_TOL_CNT_9MOB,u.EARLY_PMT_TOL_CNT_12MOB,
    x.REF_FAM_D24_REJECT_3M_CNT,x.REF_FAM_D24_REJECT_6M_CNT,x.REF_FAM_D24_REJECT_9M_CNT,x.REF_FAM_D24_REJECT_12M_CNT,
    x.REF_FAM_D24_REJECT_24M_CNT,x.REF_FAM_D24_APPROVE_3M_CNT,x.REF_FAM_D24_APPROVE_6M_CNT,x.REF_FAM_D24_APPROVE_9M_CNT,
    x.REF_FAM_D24_APPROVE_12M_CNT,x.REF_FAM_D24_APPROVE_24M_CNT,x.REF_FAM_D24_CANCEL_3M_CNT,x.REF_FAM_D24_CANCEL_6M_CNT,
    x.REF_FAM_D24_CANCEL_9M_CNT,x.REF_FAM_D24_CANCEL_12M_CNT,x.REF_FAM_D24_CANCEL_24M_CNT,x.REF_FAM_D24_REJECT_3M_HEADCNT,

    y.REF_REF_D24_REJECT_3M_CNT,y.REF_REF_D24_REJECT_6M_CNT,y.REF_REF_D24_REJECT_9M_CNT,y.REF_REF_D24_REJECT_12M_CNT,
    y.REF_REF_D24_REJECT_24M_CNT,y.REF_REF_D24_APPROVE_3M_CNT,y.REF_REF_D24_APPROVE_6M_CNT,y.REF_REF_D24_APPROVE_9M_CNT,
    y.REF_REF_D24_APPROVE_12M_CNT,y.REF_REF_D24_APPROVE_24M_CNT,y.REF_REF_D24_CANCEL_3M_CNT,y.REF_REF_D24_CANCEL_6M_CNT,
    y.REF_REF_D24_CANCEL_9M_CNT,y.REF_REF_D24_CANCEL_12M_CNT,y.REF_REF_D24_CANCEL_24M_CNT,y.REF_REF_D24_REJECT_3M_HEADCNT,

    y.REF_REF_D24_APPROVE_LAST,y.REF_REF_D24_APPROVE_FIRST,y.REF_REF_D24_REJECT_LAST,y.REF_REF_D24_REJECT_FIRST,
    z.AVG_FMINCOME,z.INCTOEXP,
    a0.AGREEMENT_NO,a0.LINKDPD_24M,a0.LINKDPDALL_24M,a0.LINKAPPS_24M,a0.LINKDPD_12M,a0.LINKDPDALL_12M,a0.LINKAPPS_12M,
    c0.TOTAL_EARLY_CLOSED,c0.MAX_EARLY_CLOSED,c0.NUMBER_ACTIVE_ACCT,
    c0.TOTAL_EARLY_CLOSED_6M,c0.TOTAL_EARLY_CLOSED_3M,
    c0.TOTAL_EARLY_CLOSED_12M,c0.TOTAL_EARLY_CLOSED_24M,
    case when c0.NUMBER_ACTIVE_ACCT=0 then -1 else round(c0.total_early_closed/c0.NUMBER_ACTIVE_ACCT,2) end as early_ratio,

    E0.PROVINCE_UNSIGN PROVINCE

    from quan_tbl_final_0 a
    left join quan_tbl_final_4 b on a.app_id=b.app_id and a.agreement_no=b.agreement_no
    left join quan_tbl_final_5 c on a.app_id=c.app_id and a.agreement_no=c.agreement_no

    left join quan_tbl_final_7 e on a.app_id=e.app_id and a.agreement_no=e.agreement_no
    left join quan_tbl_final_8 f on a.app_id=f.app_id
    left join g on a.app_id=g.app_id 
    left join h on a.app_id=h.app_id
    left join i on a.app_id=i.app_id
    left join k on a.app_id=k.app_id
    left join l on a.app_id=l.app_id
    left join m on a.app_id=m.app_id
    left join n on a.app_id=n.app_id
    left join o on a.app_id=o.app_id
    left join quan_tbl_final_3 p on p.agreement_no=a.agreement_no and p.cif_nb=a.cif_nb
    left join quan_tbl_final_9 t on t.agreement_no=a.agreement_no and t.app_id=a.app_id
    left join quan_tbl_final_10 u on u.agreement_no=a.agreement_no and u.app_id=a.app_id
    left join quan_tbl_final_11_3 x on x.agreement_no=a.agreement_no and x.app_id=a.app_id
    left join quan_tbl_final_11_4 y on y.agreement_no=a.agreement_no and y.app_id=a.app_id
    left join quan_tbl_final_13 z on z.agreement_no=a.agreement_no and z.app_id=a.app_id
    left join quan_tbl_final_12 a0 on a0.agreement_no=a.agreement_no and a0.app_id=a.app_id
    left join quan_tbl_final_16 c0 on c0.agreement_no=a.agreement_no and c0.app_id=a.app_id

    left join pol_tbl_quang_province_mapping e0 on e0.province_code=p.mode_APPRESREGION
    left join pol_tbl_raw_app_data f3 on f3.app_id=a.app_id and f3.agreement_no=a.agreement_no
