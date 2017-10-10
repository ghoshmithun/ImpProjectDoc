drop table if exists ssoni.ON_Attrition_Scored_June2017;
create table ssoni.ON_Attrition_Scored_June2017 stored as orc as 
select customerkey,medianscore from evergreenattribute.ONattritionattribute
where processingdate = '2017-06-03';
--24064657


--drop table if exists ssoni.ON_Attrition_Scored_June2017_Exploded;
--create table ssoni.ON_Attrition_Scored_June2017_Exploded stored as orc as 
--select distinct customerkey,medianscore,decilerank 
--from 
--(select  customerkey[0] as customerkey,medianscore,decilerank from ssoni.ON_Attrition_Scored_June2017
--union all select customerkey[1] as customerkey,medianscore,decilerank from ssoni.ON_Attrition_Scored_June2017
--union all select customerkey[2] as customerkey,medianscore,decilerank from ssoni.ON_Attrition_Scored_June2017
--union all select customerkey[3] as customerkey,medianscore,decilerank from ssoni.ON_Attrition_Scored_June2017
--) combined;


create table ssoni.ON_Attrition_Scored_June2017_Deciles stored as orc as 
select customerkey,medianscore,ntile(10) over (order by medianscore desc) as decile
from ssoni.ON_Attrition_Scored_June2017;






drop table if exists ssoni.ON_Attrition_Scored_June2017_Exploded;
create table ssoni.ON_Attrition_Scored_June2017_Exploded stored as orc as 
select distinct customerkey[0] as customerkey,medianscore,decile
from ssoni.ON_Attrition_Scored_June2017_Deciles;

drop table if exists ssoni.ON_Attrition_Scored_June2017_Emailkey;
create table ssoni.ON_Attrition_Scored_June2017_Emailkey stored as orc as 
select distinct a.customerkey,a.decile,b.emailkey
from ssoni.ON_Attrition_Scored_June2017_Exploded a 
inner join crmanalytics.customerresolution b on a.customerkey = b.customerkey;

create table ssoni.ON_Attrition_Scored_June2017_Emailkey_Final stored as orc as 
select emailkey,decile,(case when rand() < 0.1 then "control" else "test" end) as testcontrol
from ssoni.ON_Attrition_Scored_June2017_Emailkey
where emailkey is not null;


drop table if exists ssoni.ON_Attrition_Scored_June2017_Emailkey_Export;
create external table ssoni.ON_Attrition_Scored_June2017_Emailkey_Export
(emailkey string,
decile int,
testcontrol string
)
row format delimited 
fields terminated by ',' 
lines terminated by '\n' 
location '/user/ssoni/ON_Attrition_Scored_June2017_Emailkey_Export'
TBLPROPERTIES('serialization.null.format'='');

insert overwrite table ssoni.ON_Attrition_Scored_June2017_Emailkey_Export
select * from ssoni.ON_Attrition_Scored_June2017_Emailkey_Final;
exit;
hadoop fs -getmerge /user/ssoni/ON_Attrition_Scored_June2017_Emailkey_Export /home/ssoni