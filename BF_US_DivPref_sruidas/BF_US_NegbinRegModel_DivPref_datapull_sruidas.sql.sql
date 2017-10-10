--pulling data for training--
drop table if exists sruidas.shraddha_brfs;
create table sruidas.shraddha_brfs as
select customer_key,transaction_num,line_num,order_status,brand,product_key,item_qty,sales_amt,transaction_date from mds_new.ods_orderline_t
where transaction_date between '2015-03-01' and '2016-08-31' and brand='BF' and country='US' and (order_status='R' or order_status='O');

--mapping product key to the pulled data--
drop table if exists sruidas.shraddha_brfs1;
create table sruidas.shraddha_brfs1 as
select t1.customer_key,t1.transaction_num,t1.line_num,t1.order_status,t1.brand,t1.product_key,t1.item_qty,t1.sales_amt,t1.transaction_date,t2.mdse_div_desc,t2.mdse_dept_desc,t2.style_color_cd
from sruidas.shraddha_brfs t1
left outer join mds_new.ods_product_t t2
on t1.product_key=t2.product_key;

--filter division that are not BRFS--
drop table if exists sruidas.brfs_only;
create table sruidas.brfs_only as
select customer_key,transaction_num,line_num,order_status,brand,product_key,item_qty,sales_amt,transaction_date,mdse_div_desc,mdse_dept_desc,style_color_cd 
from sruidas.shraddha_brfs1
where mdse_div_desc not like 'GAP%' and mdse_div_desc  not like 'ONO%' and mdse_div_desc not like '%BABY%' and mdse_div_desc not like '%MATERNITY%'
and mdse_div_desc not like '%BOYS%' and mdse_div_desc not like '%GIRLS%' and mdse_div_desc not like '%KIDS%' and mdse_div_desc not like '%MISCELLANEOUS%'and
mdse_div_desc not like '%MISC%' and mdse_div_desc not like '%UNDETERMINABLES%' and mdse_div_desc not like '%UNDETER%' and mdse_div_desc not like '%OTHER%'
and mdse_div_desc not like '%FUNZONE%' and mdse_div_desc not like '%FLAGSHIP TRANSFERS%' and mdse_div_desc not like '%NULL%';

--mapping customerkeys to define masterkeys--
drop table if exists sruidas.brfs_only_1;
create table sruidas.brfs_only_1 as
select t1.customer_key,t1.transaction_num,t1.line_num,t1.order_status,t1.brand,t1.product_key,t1.item_qty,t1.sales_amt,t1.transaction_date,t1.mdse_div_desc,t1.mdse_dept_desc,t1.style_color_cd,t2.customerkey,t2.masterkey
from sruidas.brfs_only t1
 left outer join
(select distinct customerkey, masterkey from crmanalytics.customerresolution) t2
on t1.customer_key = t2.customerkey;

---- Filter the previous table so null masterkeys are removed--
drop table if exists sruidas.brfs_only_masterkey;
create table sruidas.brfs_only_masterkey as
select t1.*
from sruidas.brfs_only_1 t1
where t1.masterkey is not Null;

--define the divisions--
drop table if exists sruidas.brfs_div_defined;
create table sruidas.brfs_div_defined as
select t1.*,
(case when lower(t1.mdse_div_desc)='accessories' and lower(t1.mdse_dept_desc)='body' then 'accessories'
when lower(t1.mdse_div_desc)='accessories' and lower(t1.mdse_dept_desc)='m accessories' then 'accessories'
when lower(t1.mdse_div_desc)='accessories' and lower(t1.mdse_dept_desc)='w accessories' then 'accessories'
when lower(t1.mdse_div_desc)='accessories' and lower(t1.mdse_dept_desc)='womens accessories' then 'accessories'
when lower(t1.mdse_div_desc)='access/shoes/nonapprl' and lower(t1.mdse_dept_desc)='non apparel' then 'accessories'
when lower(t1.mdse_div_desc)='adult accessories'  then 'accessories'
when lower(t1.mdse_div_desc)='brc shoes' and lower(t1.mdse_dept_desc)='w shoes' then 'shoes'
when lower(t1.mdse_div_desc)='outlet' and lower(t1.mdse_dept_desc)='brfsj womens' then 'women'
when lower(t1.mdse_div_desc)='shoe division'  then 'shoes'
when lower(t1.mdse_div_desc)='accessories' and lower(t1.mdse_dept_desc)='boys accessories' then 'accessories'
when lower(t1.mdse_div_desc)='accessories' and lower(t1.mdse_dept_desc)='mens body' then 'accessories'
when lower(t1.mdse_div_desc)='access/shoes/nonapprl' and lower(t1.mdse_dept_desc)='womens accessories' then 'accessories'
when lower(t1.mdse_div_desc) ='brc accessories' and lower(t1.mdse_dept_desc)='jewelry' then 'jewelry'
when lower(t1.mdse_div_desc)='outlet' and lower(t1.mdse_dept_desc)='brfsj accessories' then 'accessories'
when lower(t1.mdse_div_desc)='outlet' and lower(t1.mdse_dept_desc)='brfsj mens' then 'men'
when lower(t1.mdse_div_desc)='accessories' and lower(t1.mdse_dept_desc)='girls accessories' then 'accessories'
when lower(t1.mdse_div_desc)='accessories' and lower(t1.mdse_dept_desc)='jewelry' then 'jewelry'
when lower(t1.mdse_div_desc)='accessories' and lower(t1.mdse_dept_desc)='mens accessories' then 'accessories'
when lower(t1.mdse_div_desc)='accessories' and lower(t1.mdse_dept_desc)='personal care' then 'accessories'
when lower(t1.mdse_div_desc)='access/shoes/nonapprl' and lower(t1.mdse_dept_desc)='womens shoes' then 'shoes'
when lower(t1.mdse_div_desc)='brc accessories' and lower(t1.mdse_dept_desc)='m accessories' then 'accessories'
when lower(t1.mdse_div_desc)='brc accessories' and lower(t1.mdse_dept_desc)='w accessories' then 'accessories'
when lower(t1.mdse_div_desc)='brc mens'  then 'men'
when lower(t1.mdse_div_desc)='brc womens' then 'women'
when lower(t1.mdse_div_desc)='mens'  then 'men'
when lower(t1.mdse_div_desc)='mens division'  then 'men'
when lower(t1.mdse_div_desc)='petites'  then 'petites'
when lower(t1.mdse_div_desc)='womens'  then 'women'
when lower(t1.mdse_div_desc)='womens division'  then 'women'
else Null end) as division
from sruidas.brfs_only_masterkey t1;

---creating predictor of last 12 months purchase--
drop table if exists sruidas.brfs_input_intime1;
create table sruidas.brfs_input_intime1 as
select masterkey,division ,sum(item_qty) as items_pur_12
from sruidas.brfs_div_defined
where transaction_date between '2015-03-01' and '2016-02-29' and item_qty >0
group by masterkey,division
order by masterkey;

--number of purchases for each masterkey across all divisions in last 12 months
drop table if exists sruidas.brfs_sum_input;
create table sruidas.brfs_sum_input as
select masterkey,sum(items_pur_12) as sum_items_pur_12
from sruidas.brfs_input_intime1
group by masterkey
order by masterkey;

---proportion of purchase in each division for each masterkey in the last 12 months
drop table if exists sruidas.brfs_prop_input;
create table sruidas.brfs_prop_input as 
select t1.masterkey,t1.division,
(case when t1.items_pur_12 > 0 then t1.items_pur_12/t2.sum_items_pur_12 else 0.0 end )as n_items_pur_12
from sruidas.brfs_input_intime1 t1
inner join
sruidas.brfs_sum_input t2
on t1.masterkey=t2.masterkey;

--creating response variable--
---pulling data--
drop table if exists sruidas.brfs_response;
create table sruidas.brfs_response as
select customer_key,transaction_num,line_num,order_status,brand,product_key,item_qty,sales_amt,transaction_date from sruidas.shraddha_brfs
where transaction_date between '2016-03-01' and '2016-05-31' and brand='BF' and (order_status='R' or order_status='O');

--mapping product key--
drop table if exists sruidas.brfs_response1;
create table sruidas.brfs_response1 as
select t1.customer_key,t1.transaction_num,t1.line_num,t1.order_status,t1.brand,t1.product_key,t1.item_qty,t1.sales_amt,t1.transaction_date,t2.mdse_div_desc,t2.mdse_dept_desc,t2.style_color_cd
from sruidas.brfs_response t1
left outer join mds_new.ods_product_t t2
on t1.product_key=t2.product_key;

---filter division that are not BRFS--
drop table if exists sruidas.brfs_response_only;
create table sruidas.brfs_response_only as
select customer_key,transaction_num,line_num,order_status,brand,product_key,item_qty,sales_amt,transaction_date,mdse_div_desc,mdse_dept_desc,style_color_cd 
from sruidas.brfs_response1
where mdse_div_desc not like 'GAP%' and mdse_div_desc  not like 'ONO%' and mdse_div_desc not like '%BABY%' and mdse_div_desc not like '%MATERNITY%'
and mdse_div_desc not like '%BOYS%' and mdse_div_desc not like '%GIRLS%' and mdse_div_desc not like '%KIDS%' and mdse_div_desc not like '%MISCELLANEOUS%'and
mdse_div_desc not like '%MISC%' and mdse_div_desc not like '%UNDETERMINABLES%' and mdse_div_desc not like '%UNDETER%' and mdse_div_desc not like '%OTHER%'
and mdse_div_desc not like '%FUNZONE%' and mdse_div_desc not like '%FLAGSHIP TRANSFERS%' and mdse_div_desc not like '%NULL%';

----mapping customerkeys to define masterkeys--
drop table if exists sruidas.brfs_response_only_1;
create table sruidas.brfs_response_only_1 as
select t1.customer_key,t1.transaction_num,t1.line_num,t1.order_status,t1.brand,t1.product_key,t1.item_qty,t1.sales_amt,t1.transaction_date,t1.mdse_div_desc,t1.mdse_dept_desc,t1.style_color_cd,t2.customerkey,t2.masterkey
from sruidas.brfs_response_only t1
 left outer join
(select distinct customerkey, masterkey from crmanalytics.customerresolution) t2
on t1.customer_key = t2.customerkey;

-----mapping customerkeys to define masterkeys--
drop table if exists sruidas.brfs_response_masterkey;
create table sruidas.brfs_response_masterkey as
select t1.*
from sruidas.brfs_response_only_1 t1
where t1.masterkey is not Null;

----define the divisions--
drop table if exists sruidas.brfs_response_div;
create table sruidas.brfs_response_div as
select t1.*,
(case when lower(t1.mdse_div_desc)='accessories' and lower(t1.mdse_dept_desc)='body' then 'accessories'
when lower(t1.mdse_div_desc)='accessories' and lower(t1.mdse_dept_desc)='m accessories' then 'accessories'
when lower(t1.mdse_div_desc)='accessories' and lower(t1.mdse_dept_desc)='w accessories' then 'accessories'
when lower(t1.mdse_div_desc)='accessories' and lower(t1.mdse_dept_desc)='womens accessories' then 'accessories'
when lower(t1.mdse_div_desc)='access/shoes/nonapprl' and lower(t1.mdse_dept_desc)='non apparel' then 'accessories'
when lower(t1.mdse_div_desc)='adult accessories'  then 'accessories'
when lower(t1.mdse_div_desc)='brc shoes' and lower(t1.mdse_dept_desc)='w shoes' then 'shoes'
when lower(t1.mdse_div_desc)='outlet' and lower(t1.mdse_dept_desc)='brfsj womens' then 'women'
when lower(t1.mdse_div_desc)='shoe division'  then 'shoes'
when lower(t1.mdse_div_desc)='accessories' and lower(t1.mdse_dept_desc)='boys accessories' then 'accessories'
when lower(t1.mdse_div_desc)='accessories' and lower(t1.mdse_dept_desc)='mens body' then 'accessories'
when lower(t1.mdse_div_desc)='access/shoes/nonapprl' and lower(t1.mdse_dept_desc)='womens accessories' then 'accessories'
when lower(t1.mdse_div_desc) ='brc accessories' and lower(t1.mdse_dept_desc)='jewelry' then 'jewelry'
when lower(t1.mdse_div_desc)='outlet' and lower(t1.mdse_dept_desc)='brfsj accessories' then 'accessories'
when lower(t1.mdse_div_desc)='outlet' and lower(t1.mdse_dept_desc)='brfsj mens' then 'men'
when lower(t1.mdse_div_desc)='accessories' and lower(t1.mdse_dept_desc)='girls accessories' then 'accessories'
when lower(t1.mdse_div_desc)='accessories' and lower(t1.mdse_dept_desc)='jewelry' then 'jewelry'
when lower(t1.mdse_div_desc)='accessories' and lower(t1.mdse_dept_desc)='mens accessories' then 'accessories'
when lower(t1.mdse_div_desc)='accessories' and lower(t1.mdse_dept_desc)='personal care' then 'accessories'
when lower(t1.mdse_div_desc)='access/shoes/nonapprl' and lower(t1.mdse_dept_desc)='womens shoes' then 'shoes'
when lower(t1.mdse_div_desc)='brc accessories' and lower(t1.mdse_dept_desc)='m accessories' then 'accessories'
when lower(t1.mdse_div_desc)='brc accessories' and lower(t1.mdse_dept_desc)='w accessories' then 'accessories'
when lower(t1.mdse_div_desc)='brc mens'  then 'men'
when lower(t1.mdse_div_desc)='brc womens' then 'women'
when lower(t1.mdse_div_desc)='mens'  then 'men'
when lower(t1.mdse_div_desc)='mens division'  then 'men'
when lower(t1.mdse_div_desc)='petites'  then 'petites'
when lower(t1.mdse_div_desc)='womens'  then 'women'
when lower(t1.mdse_div_desc)='womens division'  then 'women'
else Null end) as division
from sruidas.brfs_response_masterkey t1;

--number of purchases for mar-may in each division for each customerkey
drop table if exists sruidas.brfs_response_intime;
create table sruidas.brfs_response_intime as
select masterkey,division,sum(item_qty) as items_pur_resp
from sruidas.brfs_response_div
where item_qty >0 
group by masterkey,division
order by masterkey;

---merging proportion  and response
drop table if exists sruidas.brfs_negbin_merge;
create table sruidas.brfs_negbin_merge as
select (case when a.masterkey is null then b.masterkey else a.masterkey end) as masterkey,
(case when a.division is null then b.division else a.division end) as division,
coalesce (a.n_items_pur_12,0) as prop_items_pur_12,coalesce (b.items_pur_resp,0) as items_pur_resp
from sruidas.brfs_prop_input a
full outer join
sruidas.brfs_response_intime b
on a.masterkey=b.masterkey and a.division=b.division;

---mrging previous table with count variable
drop table if exists sruidas.negbin_final;
create table sruidas.negbin_final as
select (case when a.masterkey is null then b.masterkey else a.masterkey end) as masterkey,
(case when a.division is null then b.division else a.division end) as division,
coalesce (a.prop_items_pur_12,0) as prop_items_pur_12,coalesce (a.items_pur_resp,0) as items_pur_resp,coalesce(b.items_pur_12,0) as count_items_pur_12
from sruidas.brfs_negbin_merge a
full outer join
sruidas.brfs_nonnull_div b
on a.masterkey=b.masterkey and a.division=b.division;

--convertiong the previous table into wide form
drop table if exists sruidas.negbin_wide;
create table sruidas.negbin_wide as
select masterkey,
sum(case when division='accessories' then prop_items_pur_12 end) as accessories_prop_12,
sum(case when division='men' then prop_items_pur_12 end) as men_prop_12,
sum(case when division='women' then prop_items_pur_12 end) as women_prop_12,
sum(case when division='petites' then prop_items_pur_12 end) as petites_prop_12,
sum(case when division='jewelry' then prop_items_pur_12 end) as jewelry_prop_12,
sum(case when division='shoes' then prop_items_pur_12 end) as shoes_prop_12,
sum(case when division='accessories' then items_pur_resp end) as accessories_resp,
sum(case when division='men' then items_pur_resp end) as men_resp,
sum(case when division='women' then items_pur_resp end) as women_resp,
sum(case when division='petites' then items_pur_resp end) as petites_resp,
sum(case when division='jewelry' then items_pur_resp end) as jewelry_resp,
sum(case when division='shoes' then items_pur_resp end) as shoes_resp,
sum(case when division='accessories' then count_items_pur_12 end) as accessories_count_12,
sum(case when division='men' then count_items_pur_12 end) as men_count_12,
sum(case when division='women' then count_items_pur_12 end) as women_count_12,
sum(case when division='petites' then count_items_pur_12 end) as petites_count_12,
sum(case when division='jewelry' then count_items_pur_12 end) as jewelry_count_12,
sum(case when division='shoes' then count_items_pur_12 end) as shoes_count_12
from sruidas.negbin_final
group by masterkey;

--replacing nulls in the all the columns by 0 or 0.0
drop table if exists sruidas.negbin_finaldata;
create table sruidas.negbin_finaldata as
select masterkey,
(case when isnull(accessories_prop_12) then 0.0 else accessories_prop_12 end) as accessories_prop_12,
(case when isnull(men_prop_12) then 0.0 else men_prop_12 end) as men_prop_12,
(case when isnull(women_prop_12) then 0.0 else women_prop_12 end) as women_prop_12,
(case when isnull(petites_prop_12) then 0.0 else petites_prop_12 end) as petites_prop_12,
(case when isnull(jewelry_prop_12) then 0.0 else jewelry_prop_12 end) as jewelry_prop_12,
(case when isnull(shoes_prop_12) then 0.0 else shoes_prop_12 end) as shoes_prop_12,
(case when isnull(accessories_resp) then cast(0 as bigint) else accessories_resp end) as accessories_resp,
(case when isnull(men_resp) then cast(0 as bigint) else men_resp end) as men_resp,
(case when isnull(women_resp) then cast(0 as bigint) else women_resp end) as women_resp,
(case when isnull(petites_resp) then cast(0 as bigint) else petites_resp end) as petites_resp,
(case when isnull(jewelry_resp) then cast(0 as bigint) else jewelry_resp end) as jewelry_resp,
(case when isnull(shoes_resp) then cast(0 as bigint) else shoes_resp end) as shoes_resp,
(case when isnull(accessories_count_12) then cast(0 as bigint) else accessories_count_12 end) as accessories_count_12,
(case when isnull(men_count_12) then cast(0 as bigint) else men_count_12 end) as men_count_12,
(case when isnull(women_count_12) then cast(0 as bigint) else women_count_12 end) as women_count_12,
(case when isnull(petites_count_12) then cast(0 as bigint) else petites_count_12 end) as petites_count_12,
(case when isnull(jewelry_count_12) then cast(0 as bigint) else jewelry_count_12 end) as jewelry_count_12,
(case when isnull(shoes_count_12) then cast(0 as bigint)else shoes_count_12 end) as shoes_count_12
from sruidas.negbin_wide;

---adding a coulmn for total of proportion
drop table if exists sruidas.negbin_finaldata1;
create table sruidas.negbin_finaldata1 as
select masterkey,accessories_prop_12,men_prop_12,women_prop_12,petites_prop_12,jewelry_prop_12,shoes_prop_12,accessories_resp,men_resp,women_resp,petites_resp,
jewelry_resp,shoes_resp,accessories_count_12,men_count_12,women_count_12,petites_count_12,jewelry_count_12,shoes_count_12,
(accessories_prop_12+men_prop_12+women_prop_12+petites_prop_12+jewelry_prop_12+shoes_prop_12) as tot_prop
from sruidas.negbin_finaldata;

---removing rows for which total of proportion is zero
drop table if exists sruidas.negbin_finaldata2;
create table sruidas.negbin_finaldata2 as
select masterkey,accessories_prop_12,men_prop_12,women_prop_12,petites_prop_12,jewelry_prop_12,shoes_prop_12,accessories_resp,men_resp,women_resp,petites_resp,
jewelry_resp,shoes_resp,accessories_count_12,men_count_12,women_count_12,petites_count_12,jewelry_count_12,shoes_count_12,tot_prop
from sruidas.negbin_finaldata1
where tot_prop!=0;

---exporting data
drop table if exists sruidas.nbtraindata;
create external table sruidas.nbtraindata 
(masterkey string,
accessories_prop_12 double,
men_prop_12 double,
women_prop_12 double,
petites_prop_12 double,
jewelry_prop_12 double,
shoes_prop_12 double,
accessories_resp bigint,
men_resp bigint,
women_resp bigint,
petites_resp bigint,
jewelry_resp bigint,
shoes_resp bigint,
accessories_count_12 bigint,
men_count_12 bigint,
women_count_12 bigint,
petites_count_12 bigint,
jewelry_count_12 bigint,
shoes_count_12 bigint,
tot_prop double)


row format delimited
fields terminated by '|'
lines terminated by '\n'
location '/user/sruidas/nbtraindata'
TBLPROPERTIES('serialization.null.format'='');
insert overwrite table sruidas.nbtraindata
select * from sruidas.negbin_finaldata2;
hadoop fs -getmerge /user/sruidas/nbtraindata /home/sruidas