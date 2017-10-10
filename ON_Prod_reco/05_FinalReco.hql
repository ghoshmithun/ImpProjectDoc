use sdube_on;
set SEG_FILE=/user/sdube/seg_file;
set IB_RECO_OUTPUT=/user/sdube/reco_output;
set ET_OUTPUT=$4;

SET mapred.job.queue.name=cem;


--read in final recos for PAS
drop table if exists ib_recs;
create external table ib_recs
(
masterkey_map INT,
reco1 INT,
reco2 INT,
reco3 INT,
reco4 INT,
reco5 INT,
reco6 INT,
reco7 INT,
reco8 INT,
reco9 INT,
reco10 INT,
reco11 INT,
reco12 INT,
reco13 INT,
reco14 INT,
reco15 INT,
reco16 INT,
reco17 INT,
reco18 INT,
reco19 INT,
reco20 INT,
reco21 INT,
reco22 INT,
reco23 INT,
reco24 INT,
reco25 INT,
reco26 INT,
reco27 INT,
reco28 INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hiveconf:IB_RECO_OUTPUT}'
;


--append the masterkey to the recs above
drop table if exists masterkey_cust_recos_ib_0;
create table masterkey_cust_recos_ib_0 as 
select a.masterkey_map, a.masterkey, b.reco1, b.reco2, b.reco3, b.reco4, b.reco5, b.reco6, b.reco7, b.reco8, b.reco9, b.reco10, b.reco11, b.reco12, b.reco13, b.reco14, b.reco15, b.reco16, b.reco17, b.reco18, b.reco19, b.reco20, b.reco21, b.reco22, b.reco23, b.reco24, b.reco25, b.reco26, b.reco27, b.reco28
from
(select * from masterkey_map_0) a
right outer join
(select * from ib_recs) b
on
a.masterkey_map = b.masterkey_map
;


--append the emailkey, customerkey etc from all_keys to the above table
drop table if exists masterkey_cust_recos_ib;
create table masterkey_cust_recos_ib as 
select a.masterkey, a.emailkey, a.unknownshopperid, a.ecommerceid, a.customerkey, b.reco1, b.reco2, b.reco3, b.reco4, b.reco5, b.reco6, b.reco7, b.reco8, b.reco9, b.reco10, b.reco11, b.reco12, b.reco13, b.reco14, b.reco15, b.reco16, b.reco17, b.reco18, b.reco19, b.reco20, b.reco21, b.reco22, b.reco23, b.reco24, b.reco25, b.reco26, b.reco27, b.reco28
from 
(select * from all_keys) a
inner join
(select * from masterkey_cust_recos_ib_0) b
on
a.masterkey = b.masterkey
;

drop table if exists masterkey_cust_recos_ib_v2;
create table masterkey_cust_recos_ib_v2 as
SELECT distinct masterkey, concat_ws(",", cast(coalesce(reco1   ,'') as string ),     cast(coalesce(reco2   ,'') as string ),                cast(coalesce(reco3   ,'') as string ),          cast(coalesce(reco4   ,'') as string ),          cast(coalesce(reco5   ,'') as string ),                cast(coalesce(reco6   ,'') as string ),          cast(coalesce(reco7   ,'') as string ),          cast(coalesce(reco8   ,'') as string ),                cast(coalesce(reco9   ,'') as string ),          cast(coalesce(reco10  ,'') as string ),         cast(coalesce(reco11  ,'') as string ),                cast(coalesce(reco12  ,'') as string ),         cast(coalesce(reco13  ,'') as string ),         cast(coalesce(reco14  ,'') as string ),                cast(coalesce(reco15  ,'') as string ),         cast(coalesce(reco16  ,'') as string ),         cast(coalesce(reco17  ,'') as string ),                cast(coalesce(reco18  ,'') as string ),         cast(coalesce(reco19  ,'') as string ),         cast(coalesce(reco20  ,'') as string ),                cast(coalesce(reco21  ,'') as string ),         cast(coalesce(reco22  ,'') as string ),         cast(coalesce(reco23  ,'') as string ),                cast(coalesce(reco24  ,'') as string ),         cast(coalesce(reco25  ,'') as string ),         cast(coalesce(reco26  ,'') as string ),                cast(coalesce(reco27  ,'') as string ),         
cast(coalesce(reco28  ,'') as string )) as My_Recommendation FROM masterkey_cust_recos_ib;

drop table if exists masterkey_cust_recos_ib_v22;
create table masterkey_cust_recos_ib_v22 as
SELECT masterkey, Reco
FROM masterkey_cust_recos_ib_v2 
LATERAL VIEW explode(split(My_Recommendation,',')) myTable2  AS Reco;

DROP TABLE IF EXISTS  masterkey_cust_recos_ib_v3;
create table masterkey_cust_recos_ib_v3 AS 
SELECT *, rank() over(partition by masterkey order by reco) AS Rnk 
FROM 
masterkey_cust_recos_ib_v22;




















