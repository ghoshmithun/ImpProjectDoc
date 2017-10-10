/*US DM Mailable customer Definition */
set mapred.job.queue.name=aa;  
set hive.cli.print.header=true;
drop table if exists sruidas.gp_dm_mailable;
create table sruidas.gp_dm_mailable as
select 
a.cust_key as customer_key
from mds_new.ods_brand_customer_t a 
where 
a.country='US'
and a.brand='GP'
and nvl(a.POSTAL_CRD_MARKETABILITY_FLAG,'NC') not like '%NOMAIL'
group by a.cust_key;

drop table if exists sruidas.gp_dm_mailable_match;
create table sruidas.gp_dm_mailable_match as
select t1.customer_key,coalesce(t2.customer_key,0) as mailable_cust_key
from mghosh.gp_attrition_household_zip t1
left outer join sruidas.gp_dm_mailable t2
on t1.customer_key=t2.customer_key;


drop table if exists sruidas.gp_dm_mailable_binary;
create table sruidas.gp_dm_mailable_binary as
select t1.customer_key,
(
case
when t1.customer_key=t1.mailable_cust_key then 1L else 0L end
) as DM_mailable_binary
from sruidas.gp_dm_mailable_match t1;

select count(*) from sruidas.gp_dm_mailable_binary;
--8340478

select count(distinct customer_key) from sruidas.gp_dm_mailable_binary;
---8340478
select count(distinct customer_key) from mghosh.gp_attrition_household_zip;

--8340478


drop table if exists sruidas.gp_em_mailable;
create table sruidas.gp_em_mailable as
select
e.brand,
e.country,
c.cust_key as customer_key,
count(distinct(e.email_key)) as emailable_emails
from mds_new.ods_brand_email_t e
inner join mds_new.ods_brand_cust_email_t c
on e.email_key=c.email_key
and e.brand=c.brand
and e.country=c.country
where nvl(e.email_status,'V') in ('V','P') and nvl(e.nactv_12_mo_eml_addr_ind,'N')='N' and nvl(e.email_crd_marketability_flag,'NC') not like '%NOMAIL'
and e.country='US'and e.brand='GP' 
group by e.brand, e.country, c.cust_key;


select count(*) from sruidas.gp_em_mailable;
--9233611
select count(*) from sruidas.gp_em_mailable where emailable_emails=0;
--0


drop table if exists sruidas.gp_em_mailable_match;
create table sruidas.gp_em_mailable_match as
select t1.customer_key,t1.DM_mailable_binary,coalesce(t2.customer_key,0) as mailable_cust_key
from sruidas.gp_dm_mailable_binary t1
left outer join sruidas.gp_em_mailable t2
on t1.customer_key=t2.customer_key;

drop table if exists sruidas.gp_dm_em_mailable_binary;
create table sruidas.gp_dm_em_mailable_binary as
select t1.customer_key,t1.DM_mailable_binary,
(
case
when t1.customer_key=t1.mailable_cust_key then 1L else 0L end
) as EM_mailable_binary
from sruidas.gp_em_mailable_match t1;

select count(*) from sruidas.gp_dm_em_mailable_binary;
---8340478
select count(distinct customer_key) from sruidas.gp_dm_em_mailable_binary;
--8340478

drop table if exists mghosh.gp_dm_em_mailable_binary;
create external table mghosh.gp_dm_em_mailable_binary 
(customer_key bigint,
dm_mailable_binary  bigint ,
em_mailable_binary bigint )
row format delimited 
fields terminated by '|' 
lines terminated by '\n' 
location "/user/mghosh/gp_email_table"
TBLPROPERTIES ("serialization.null.format"=""); 

location '/user/mghosh/gp_analysis'

insert overwrite table mghosh.gp_dm_em_mailable_binary
select * from sruidas.gp_dm_em_mailable_binary;

---select a.*, EM_Flag from tableA a left outer join (select distinct cust_key, 1 as EM_Flag from tableB) b 
--on a.key=b.key