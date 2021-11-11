
--- get members from 18 months
drop table if exists dw_mem;
create temp table dw_mem as
select distinct dw_member_id, udf26id as person_id
from stage1_acl_una_extract_20211025.membermonths
where (date_part(year,to_date(activemonth, 'YYYY/MM/DD')) IN (2020) or
       (date_part(year,to_date(activemonth, 'YYYY/MM/DD')) IN (2021) and
        date_part(month,to_date(activemonth, 'YYYY/MM/DD')) between 1 and 6))
and udf16id = 'In-Scope';


--- get IP visits
drop table if exists  DW_IP_visits;
create temp table DW_IP_visits as
select dw_member_id, udf26 as person_id,visitid,visit_type,visittypedesc, startdate, enddate, los, admissiontype, admitcount, totalallowedamount,totalpaidamount
from stage1_acl_una_extract_20211025.utilization
where dw_member_id IN (
    select dw_member_id
    from dw_mem
    )
and svc_pos_code = 21
and (date_part(year,to_date(startdate, 'YYYY/MM/DD')) IN (2020) or
       (date_part(year,to_date(startdate, 'YYYY/MM/DD')) IN (2021) and
        date_part(month,to_date(startdate, 'YYYY/MM/DD')) between 1 and 6));

select to_date(date_trunc('month',startdate), 'YYYY-MM') as admit_dt,count(visitid) as total_IP_visits, avg(los) as avg_los, avg(totalallowedamount) as avg_allowed_amt
from DW_IP_visits
where admitcount = 1
group by date_trunc('month',startdate)
order by admit_dt;

select distinct person_id
from DW_IP_visits;