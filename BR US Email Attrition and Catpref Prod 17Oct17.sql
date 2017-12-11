
set mapred.job.queue.name=aa;

drop table if exists mghosh.BRUS_emailresponse_09Oct17;
create table mghosh.BRUS_emailresponse_09Oct17 as
select masterkey , customerkey, regressionprobability ,ntile(10) over(order by regressionprobability)
from evergreenattribute.br_emailresponseattribute
where brand='BR' and market='US' and processingdate = "2017-10-09";

drop table if exists mghosh.BRUS_emailresponse_09Oct17_v2 ;
create table mghosh.BRUS_emailresponse_09Oct17_v2  as
select customerkeys, masterkey,regressionprobability	, tok_windowspec as Decile from
mghosh.BRUS_emailresponse_09Oct17 lateral view explode(customerkey) atable as customerkeys;

select Decile, count(distinct masterkey) , count(distinct customerkeys) from mghosh.BRUS_emailresponse_09Oct17_v2
group by Decile;

-- 1	 1,656,349 	 1,819,791 
-- 2	 1,656,349 	 1,951,633 
-- 3	 1,656,348 	 2,233,464 
-- 4	 1,656,348 	 2,371,571 
-- 5	 1,656,348 	 2,411,354 
-- 6	 1,656,348 	 2,540,865 
-- 7	 1,656,348 	 2,512,559 
-- 8	 1,656,348 	 3,001,762 
-- 9	 1,656,348 	 3,020,982 
-- 10	 1,656,348 	 4,095,909 

 
 drop table if exists mghosh.BRUS_emailresponse_09Oct17_v3;
 create external table mghosh.BRUS_emailresponse_09Oct17_v3
 ( customerkey bigint,
 regressionprobability double,
 Decile  int)
row format delimited 
fields terminated by '|' 
lines terminated by '\n' 
location '/user/mghosh/BRUS_emailresponse'
TBLPROPERTIES ("serialization.null.format"=""); 

insert overwrite table mghosh.BRUS_emailresponse_09Oct17_v3 
select  customerkeys, regressionprobability ,decile from mghosh.BRUS_emailresponse_09Oct17_v2;




drop table if exists mghosh.BRUS_attritionscore_09Oct17;
create table mghosh.BRUS_attritionscore_09Oct17 as 
select masterkey, customerkey, medianscore ,ntile(10) over(order by medianscore) 
from evergreenattribute.brattritionattribute where processingdate="2017-09-29";

drop table if exists mghosh.BRUS_attritionscore_09Oct17_v2 ;
create table mghosh.BRUS_attritionscore_09Oct17_v2  as
select customerkeys, masterkey,medianscore	, tok_windowspec as Decile from
mghosh.BRUS_attritionscore_09Oct17 lateral view explode(customerkey) atable as customerkeys;

select Decile, count(distinct masterkey) , count(distinct customerkeys) from mghosh.BRUS_attritionscore_09Oct17_v2
group by Decile;

1	0	463579
2	0	463579
3	0	463578
4	0	463578
5	0	463578
6	0	463578
7	0	463578
8	0	463578
9	0	463578
10	0	463578

 drop table if exists mghosh.BRUS_attritionscore_09Oct17_v3;
 create external table mghosh.BRUS_attritionscore_09Oct17_v3
 ( customerkey bigint,
 medianscore double,
 Decile  int)
row format delimited 
fields terminated by '|' 
lines terminated by '\n' 
location '/user/mghosh/BRUS_attritionscore'
TBLPROPERTIES ("serialization.null.format"="");

insert overwrite table mghosh.BRUS_attritionscore_09Oct17_v3 
select  customerkeys, medianscore, Decile from mghosh.BRUS_attritionscore_09Oct17_v2;

drop table if exists mghosh.brus_catpref ;
create table  mghosh.brus_catpref as
select customerkey,masterkey,attribute 
from evergreenattribute.brcategorypreferenceattribute where processingdate = "2017-10-16" ;
--4,904,548

drop table if exists mghosh.brus_catpref_v2;
create table mghosh.brus_catpref_v2 as
select customerkeys, attribute from mghosh.brus_catpref lateral view explode(customerkey) atab as customerkeys;

drop table if exists mghosh.brus_catpref_v3;
create table mghosh.brus_catpref_v3 as
select customerkeys, attributes from mghosh.brus_catpref_v2 lateral view explode(attribute) btab as attributes;

drop table if exists mghosh.brus_catpref_v4;
create external table mghosh.brus_catpref_v4 
(customerkey bigint,
attribute string)
row format delimited
fields terminated by '|'
lines terminated by '\n'
location '/user/mghosh/brus_catpref'
TBLPROPERTIES ("serialization.null.format"="");

insert overwrite table mghosh.brus_catpref_v4
select customerkey,attribute from mghosh.brus_catpref_v3;

hadoop fs -getmerge /user/mghosh/brus_catpref/ /home/mghosh/brus_catpref/brus_catpref.txt
hadoop fs -getmerge /user/mghosh/BRUS_attritionscore/ /home/mghosh/BRUS_attritionscore/BRUS_attritionscore.txt
hadoop fs -getmerge /user/mghosh/BRUS_emailresponse/ /home/mghosh/BRUS_emailresponse/BRUS_emailresponse.txt