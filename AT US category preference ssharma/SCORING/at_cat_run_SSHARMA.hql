set hive.variable.substitute=true;
set hive.exec.reducers.max=200;
set mapred.job.queue.name=aa;
SET mapred.job.priority=VERY_HIGH;
set DATE_NOW=2016-08-01;

drop table if exists ssharma_db.at_last_qrtr_clicks_run;
create table ssharma_db.at_last_qrtr_clicks_run as 
select distinct
unk_shop_id,
visit_num,
visid_high,
visid_low,
pagename,
hier1,
prop1,
purchaseid,
prop33,
country,
t_time_info,
product_list,
date_time 
from omniture.hit_data_t 
where (instr(pagename,'at:browse')>0 or instr(pagename,'mobile:at')>0) and 
UNIX_TIMESTAMP(date_time,'yyyy-MM-dd') BETWEEN
UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 91), 'yyyy-MM-dd') AND 
UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 1), 'yyyy-MM-dd');

drop table if exists ssharma_db.at_prd_clicks_run;
create table ssharma_db.at_prd_clicks_run as 
select *,substr(prop1,1,6) as style_cd from ssharma_db.at_last_qrtr_clicks_run
where prop33='product';

drop table if exists ssharma_db.at_prd_clicks_desc_run;
create table ssharma_db.at_prd_clicks_desc_run as 
select a.*,b.mdse_div_desc,b.mdse_dept_desc,b.mdse_class_desc
from ssharma_db.at_prd_clicks_run a 
inner join (select distinct style_cd,mdse_div_desc,mdse_dept_desc,mdse_class_desc from mds_new.ods_product_t where mdse_div_desc is not null and mdse_dept_desc is not null and mdse_class_desc is not null and mdse_corp_desc='ATH DIR') b 
on a.style_cd=b.style_cd
;

drop table if exists ssharma_db.at_clicks_cat_run;
create table ssharma_db.at_clicks_cat_run as 
select *,
(case when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='BOTTOMS' and mdse_class_desc='CAPRIS' then 'GIRLS CAPRIS'
when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='BOTTOMS' and mdse_class_desc='PANTS' then 'GIRLS PANTS'
when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='BOTTOMS' and mdse_class_desc='SHORTS' then 'GIRLS SHORTS'
when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='BOTTOMS' and mdse_class_desc='SKIRTS/SKORTS' then 'GIRLS SKIRTS/SKORTS'
when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='GIRLS ACCESSORY' then 'GIRLS ACCESSORY'
when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='GIRLS INTIMATES' then 'GIRLS INTIMATES'
when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='GIRLS SWIMWEAR' then 'GIRLS SWIMWEAR'
when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='OUTERWEAR' then 'GIRLS OUTERWEAR'
when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='TOPS/SUPPORT/SWEATER' and mdse_class_desc<>'SWEATSHIRTS' then 'GIRLS TOPS'
when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='TOPS/SUPPORT/SWEATER' and mdse_class_desc='SWEATSHIRTS' then 'GIRLS SWEATSHIRTS' 
when mdse_div_desc='WOMENS' and mdse_dept_desc='ACCESSORY' then 'WOMENS ACCESSORY' 
when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and instr(mdse_class_desc,'CAPRI')>0 then 'WOMENS CAPRIS' 
when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and mdse_class_desc='DRESS' then 'WOMENS DRESSES'
when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and (mdse_class_desc='JACKET' or mdse_class_desc='OUTERWEAR' or mdse_class_desc='VEST') then 'WOMENS OUTERWEAR'
when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and instr(mdse_class_desc,'PANT')>0 then 'WOMENS PANTS' 
when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and instr(mdse_class_desc,'SHORT')>0 then 'WOMENS SHORTS' 
when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and instr(mdse_class_desc,'SKIRT')>0 then 'WOMENS SKIRTS' 
when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and instr(mdse_class_desc,'SKORT')>0 then 'WOMENS SKORTS' 
when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and mdse_class_desc='SUPPORT TOPS' then 'WOMENS SUPPORT TOPS' 
when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and mdse_class_desc='SWEATER' then 'WOMENS SWEATERS' 
when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and (instr(mdse_class_desc,'COVER UP')>0 or instr(mdse_class_desc,'TOP')>0)  then 'WOMENS TOPS'
when mdse_div_desc='WOMENS' and mdse_dept_desc='FOOTWEAR' then 'WOMENS FOOTWEAR' 
when mdse_div_desc='WOMENS' and mdse_dept_desc='INTIMATES' and mdse_class_desc<>'BRAS' then 'WOMENS INTIMATES'
when mdse_div_desc='WOMENS' and mdse_dept_desc='INTIMATES' and mdse_class_desc='BRAS' then 'WOMENS BRAS'
when mdse_div_desc='WOMENS' and mdse_dept_desc='SWIMWEAR' and mdse_class_desc='BIKINI TOP' then 'WOMENS SWIM BIKINI'
when mdse_div_desc='WOMENS' and mdse_dept_desc='SWIMWEAR' and mdse_class_desc='SWIM BOTTOMS' then 'WOMENS SWIM BOTTOMS'
when mdse_div_desc='WOMENS' and mdse_dept_desc='SWIMWEAR' and mdse_class_desc='ONE PIECE' then 'WOMENS SWIM ONE PIECE'
when mdse_div_desc='WOMENS' and mdse_dept_desc='SWIMWEAR' and mdse_class_desc='RASHGUARDS' then 'WOMENS SWIM RASHGUARDS'
when mdse_div_desc='WOMENS' and mdse_dept_desc='SWIMWEAR' and mdse_class_desc='TANKINI' then 'WOMENS SWIM TANKINI'
else 'N/A' end) as category
from ssharma_db.at_prd_clicks_desc_run;




drop table if exists ssharma_db.at_bagadd_run;
create table ssharma_db.at_bagadd_run as 
select distinct
unk_shop_id,
visit_num,
visid_high,
visid_low,
pagename,
hier1,
prop1,
purchaseid,
prop33,
country,
t_time_info,
product_list,
date_time 
from ssharma_db.at_last_qrtr_clicks_run
where  prop33='inlineBagAdd';

drop table if exists ssharma_db.at_purchasenull_run;
create table ssharma_db.at_purchasenull_run as 
select unk_shop_id,visit_num
from ssharma_db.at_last_qrtr_clicks_run
where unk_shop_id != "" and (PurchaseID IS NULL or PurchaseID=='' or PurchaseID==' ');

drop table if exists ssharma_db.at_abandon_basket_run;
create table ssharma_db.at_abandon_basket_run  as 
select a.* 
from ssharma_db.at_bagadd_run a 
inner join  ssharma_db.at_purchasenull_run b 
on a.unk_shop_id=b.unk_shop_id and a.visit_num=b.visit_num;

drop table if exists ssharma_db.at_abandon_basket_prod_run;
create table ssharma_db.at_abandon_basket_prod_run as 
select a.*,b.mdse_div_desc,b.mdse_dept_desc,b.mdse_class_desc,
(case when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='CAPRIS' then 'GIRLS CAPRIS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='PANTS' then 'GIRLS PANTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SHORTS' then 'GIRLS SHORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SKIRTS/SKORTS' then 'GIRLS SKIRTS/SKORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS ACCESSORY' then 'GIRLS ACCESSORY'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS INTIMATES' then 'GIRLS INTIMATES'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS SWIMWEAR' then 'GIRLS SWIMWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='OUTERWEAR' then 'GIRLS OUTERWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc<>'SWEATSHIRTS' then 'GIRLS TOPS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc='SWEATSHIRTS' then 'GIRLS SWEATSHIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='ACCESSORY' then 'WOMENS ACCESSORY' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'CAPRI')>0 then 'WOMENS CAPRIS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='DRESS' then 'WOMENS DRESSES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (b.mdse_class_desc='JACKET' or b.mdse_class_desc='OUTERWEAR' or b.mdse_class_desc='VEST') then 'WOMENS OUTERWEAR'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'PANT')>0 then 'WOMENS PANTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SHORT')>0 then 'WOMENS SHORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKIRT')>0 then 'WOMENS SKIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKORT')>0 then 'WOMENS SKORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SUPPORT TOPS' then 'WOMENS SUPPORT TOPS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SWEATER' then 'WOMENS SWEATERS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (instr(b.mdse_class_desc,'COVER UP')>0 or instr(b.mdse_class_desc,'TOP')>0)  then 'WOMENS TOPS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='FOOTWEAR' then 'WOMENS FOOTWEAR' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc<>'BRAS' then 'WOMENS INTIMATES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc='BRAS' then 'WOMENS BRAS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='BIKINI TOP' then 'WOMENS SWIM BIKINI'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='SWIM BOTTOMS' then 'WOMENS SWIM BOTTOMS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='ONE PIECE' then 'WOMENS SWIM ONE PIECE'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='RASHGUARDS' then 'WOMENS SWIM RASHGUARDS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='TANKINI' then 'WOMENS SWIM TANKINI'
else 'N/A' end) as category
from ssharma_db.at_abandon_basket_run a 
inner join (select distinct style_cd,mdse_div_desc,mdse_dept_desc,mdse_class_desc from mds_new.ods_product_t where mdse_div_desc is not null and mdse_dept_desc is not null and mdse_class_desc is not null and mdse_corp_desc='ATH DIR') b 
on substr(a.prop1,1,6)=b.style_cd
;


drop table if exists ssharma_db.at_clicks_cat_master_run;
create table ssharma_db.at_clicks_cat_master_run as 
select a.*,b.masterkey
from ssharma_db.at_clicks_cat_run a 
left outer join 
(select distinct unknownshopperid,masterkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;

drop table if exists ssharma_db.at_abandon_basket_prod_master_run;
create table ssharma_db.at_abandon_basket_prod_master_run as 
select a.*,b.masterkey
from  ssharma_db.at_abandon_basket_prod_run a 
left outer join 
(select distinct unknownshopperid,masterkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;

drop table if exists ssharma_db.at_sugg_window_run;
create table ssharma_db.at_sugg_window_run as 
select customer_key,item_qty,product_key,transaction_date
from mds_new.ods_orderline_t 
where brand='AT' and country='US' 
and unix_timestamp(transaction_date,'yyyy-MM-dd') between UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 366), 'yyyy-MM-dd') AND 
UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}', - 1), 'yyyy-MM-dd')
and order_status in ('R','O') and item_qty>0 and sales_amt>0;

drop table if exists ssharma_db.at_sugg_run;
create table ssharma_db.at_sugg_run as
select a.customer_key,a.item_qty,a.product_key,b.mdse_div_desc,b.mdse_dept_desc,b.mdse_class_desc,a.transaction_date,
(case when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='CAPRIS' then 'GIRLS CAPRIS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='PANTS' then 'GIRLS PANTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SHORTS' then 'GIRLS SHORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SKIRTS/SKORTS' then 'GIRLS SKIRTS/SKORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS ACCESSORY' then 'GIRLS ACCESSORY'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS INTIMATES' then 'GIRLS INTIMATES'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS SWIMWEAR' then 'GIRLS SWIMWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='OUTERWEAR' then 'GIRLS OUTERWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc<>'SWEATSHIRTS' then 'GIRLS TOPS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc='SWEATSHIRTS' then 'GIRLS SWEATSHIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='ACCESSORY' then 'WOMENS ACCESSORY' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'CAPRI')>0 then 'WOMENS CAPRIS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='DRESS' then 'WOMENS DRESSES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (b.mdse_class_desc='JACKET' or b.mdse_class_desc='OUTERWEAR' or b.mdse_class_desc='VEST') then 'WOMENS OUTERWEAR'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'PANT')>0 then 'WOMENS PANTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SHORT')>0 then 'WOMENS SHORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKIRT')>0 then 'WOMENS SKIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKORT')>0 then 'WOMENS SKORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SUPPORT TOPS' then 'WOMENS SUPPORT TOPS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SWEATER' then 'WOMENS SWEATERS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (instr(b.mdse_class_desc,'COVER UP')>0 or instr(b.mdse_class_desc,'TOP')>0)  then 'WOMENS TOPS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='FOOTWEAR' then 'WOMENS FOOTWEAR' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc<>'BRAS' then 'WOMENS INTIMATES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc='BRAS' then 'WOMENS BRAS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='BIKINI TOP' then 'WOMENS SWIM BIKINI'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='SWIM BOTTOMS' then 'WOMENS SWIM BOTTOMS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='ONE PIECE' then 'WOMENS SWIM ONE PIECE'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='RASHGUARDS' then 'WOMENS SWIM RASHGUARDS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='TANKINI' then 'WOMENS SWIM TANKINI'
else 'N/A' end) as category 
from ssharma_db.at_sugg_window_run a 
inner join 
mds_new.ods_product_t b 
on a.product_key=b.product_key;

drop table if exists ssharma_db.at_sugg_master_run;
create table ssharma_db.at_sugg_master_run as 
select a.*,b.masterkey
from  ssharma_db.at_sugg_run a 
left outer join 
(select distinct customerkey,masterkey from crmanalytics.customerresolution) b 
on a.customer_key=b.customerkey;

drop table if exists ssharma_db.at_sugg_master_notnull_run;
create table ssharma_db.at_sugg_master_notnull_run as 
select * from ssharma_db.at_sugg_master_run where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_abandon_basket_prod_master_notnull_run;
create table ssharma_db.at_abandon_basket_prod_master_notnull_run as
select * from  ssharma_db.at_abandon_basket_prod_master_run where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_clicks_cat_master_notnull_run;
create table ssharma_db.at_clicks_cat_master_notnull_run as
select * from  ssharma_db.at_clicks_cat_master_run where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_txn_mk_cat_run;
create table ssharma_db.at_txn_mk_cat_run as 
select masterkey,category,sum(item_qty) as items_purch
from ssharma_db.at_sugg_master_notnull_run 
group by masterkey,category;

drop table if exists ssharma_db.at_abandon_mk_cat_run;
create table ssharma_db.at_abandon_mk_cat_run as 
select masterkey,category,count(distinct t_time_info) as items_abandoned
from ssharma_db.at_abandon_basket_prod_master_notnull_run
group by masterkey,category;

drop table if exists ssharma_db.at_clicks_mk_cat_run;
create table ssharma_db.at_clicks_mk_cat_run as 
select masterkey,category,count(distinct t_time_info) as items_browsed
from ssharma_db.at_clicks_cat_master_notnull_run
group by masterkey,category;


drop table if exists ssharma_db.at_browser_run;
create table ssharma_db.at_browser_run as 
select distinct (case when a.masterkey is null then b.masterkey else a.masterkey end) as masterkey,
(case when a.category is null then b.category else a.category end) as category,
coalesce(a.items_abandoned,0) as items_abandoned,coalesce(b.items_browsed,0) as items_browsed
from ssharma_db.at_abandon_mk_cat_run a
full outer join
ssharma_db.at_clicks_mk_cat_run b 
on a.masterkey=b.masterkey and a.category=b.category;

drop table if exists ssharma_db.at_browser_purchaser_run;
create table ssharma_db.at_browser_purchaser_run as 
select distinct (case when a.masterkey is null then b.masterkey else a.masterkey end) as masterkey,
(case when a.category is null then b.category else a.category end) as category,
coalesce(a.items_abandoned,0) as items_abandoned,coalesce(a.items_browsed,0) as items_browsed,
coalesce(b.items_purch,0) as items_purchased
from ssharma_db.at_browser_run a 
full outer join 
ssharma_db.at_txn_mk_cat_run b 
on a.masterkey=b.masterkey and a.category=b.category;



drop table if exists ssharma_db.at_fisrt_qrtr_purch_run;
create table ssharma_db.at_fisrt_qrtr_purch_run as 
select customer_key,item_qty,product_key
from mds_new.ods_orderline_t 
where brand='AT' and country='US' 
and unix_timestamp(transaction_date,'yyyy-MM-dd')  between UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}',-366), 'yyyy-MM-dd') AND 
UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}',-276), 'yyyy-MM-dd')
and order_status in ('R','O') and item_qty>0 and sales_amt>0;

drop table if exists ssharma_db.at_first_qrtr_txn_run;
create table ssharma_db.at_first_qrtr_txn_run as
select a.customer_key,a.item_qty,a.product_key,b.mdse_div_desc,b.mdse_dept_desc,b.mdse_class_desc,
(case when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='CAPRIS' then 'GIRLS CAPRIS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='PANTS' then 'GIRLS PANTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SHORTS' then 'GIRLS SHORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SKIRTS/SKORTS' then 'GIRLS SKIRTS/SKORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS ACCESSORY' then 'GIRLS ACCESSORY'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS INTIMATES' then 'GIRLS INTIMATES'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS SWIMWEAR' then 'GIRLS SWIMWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='OUTERWEAR' then 'GIRLS OUTERWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc<>'SWEATSHIRTS' then 'GIRLS TOPS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc='SWEATSHIRTS' then 'GIRLS SWEATSHIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='ACCESSORY' then 'WOMENS ACCESSORY' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'CAPRI')>0 then 'WOMENS CAPRIS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='DRESS' then 'WOMENS DRESSES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (b.mdse_class_desc='JACKET' or b.mdse_class_desc='OUTERWEAR' or b.mdse_class_desc='VEST') then 'WOMENS OUTERWEAR'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'PANT')>0 then 'WOMENS PANTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SHORT')>0 then 'WOMENS SHORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKIRT')>0 then 'WOMENS SKIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKORT')>0 then 'WOMENS SKORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SUPPORT TOPS' then 'WOMENS SUPPORT TOPS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SWEATER' then 'WOMENS SWEATERS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (instr(b.mdse_class_desc,'COVER UP')>0 or instr(b.mdse_class_desc,'TOP')>0)  then 'WOMENS TOPS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='FOOTWEAR' then 'WOMENS FOOTWEAR' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc<>'BRAS' then 'WOMENS INTIMATES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc='BRAS' then 'WOMENS BRAS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='BIKINI TOP' then 'WOMENS SWIM BIKINI'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='SWIM BOTTOMS' then 'WOMENS SWIM BOTTOMS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='ONE PIECE' then 'WOMENS SWIM ONE PIECE'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='RASHGUARDS' then 'WOMENS SWIM RASHGUARDS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='TANKINI' then 'WOMENS SWIM TANKINI'
else 'N/A' end) as category 
from ssharma_db.at_fisrt_qrtr_purch_run a 
inner join 
mds_new.ods_product_t b 
on a.product_key=b.product_key;

drop table if exists ssharma_db.at_first_master_run;
create table ssharma_db.at_first_master_run as 
select a.*,b.masterkey 
from ssharma_db.at_first_qrtr_txn_run a
left outer join 
(select distinct customerkey,masterkey from crmanalytics.customerresolution) b 
on a.customer_key=b.customerkey;

drop table if exists ssharma_db.at_first_master_notnull_run;
create table ssharma_db.at_first_master_notnull_run as 
select * from ssharma_db.at_first_master_run where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_first_mk_cat_run;
create table ssharma_db.at_first_mk_cat_run as 
select masterkey,category,sum(item_qty) as items_purch_first
from ssharma_db.at_first_master_notnull_run 
group by masterkey,category;

drop table if exists ssharma_db.at_browser_purchaser_first_run;
create table ssharma_db.at_browser_purchaser_first_run as 
select distinct (case when a.masterkey is null then b.masterkey else a.masterkey end) as masterkey,
(case when a.category is null then b.category else a.category end) as category,
coalesce(a.items_abandoned,0) as items_abandoned,coalesce(a.items_browsed,0) as items_browsed,
coalesce(a.items_purchased,0) as items_purchased,
coalesce(b.items_purch_first,0) as items_purch_first
from ssharma_db.at_browser_purchaser_run a 
full outer join 
ssharma_db.at_first_mk_cat_run b 
on a.masterkey=b.masterkey and a.category=b.category;



drop table ssharma_db.at_group_master_first_run;
create table ssharma_db.at_group_master_first_run as 
select masterkey,sum(items_abandoned) as sum_items_abandoned,
sum(items_browsed) as sum_items_browsed,
sum(items_purchased) as sum_items_purchased,
sum(items_purch_first) as sum_items_purch_first
from ssharma_db.at_browser_purchaser_first_run
group by masterkey;

drop table if exists ssharma_db.at_normal_first_run;
create table ssharma_db.at_normal_first_run as 
select a.masterkey,a.category,
(case when b.sum_items_abandoned >0 then a.items_abandoned/b.sum_items_abandoned else 0.0 end) as nrml_items_abandoned,
(case when b.sum_items_browsed >0 then a.items_browsed/b.sum_items_browsed else 0.0 end) as nrml_items_browsed,
(case when b.sum_items_purchased >0 then a.items_purchased/b.sum_items_purchased else 0.0 end) as nrml_items_purchased,
(case when b.sum_items_purch_first >0 then a.items_purch_first/b.sum_items_purch_first else 0.0 end) as nrml_items_purch_first
from ssharma_db.at_browser_purchaser_first_run a 
left outer join 
ssharma_db.at_group_master_first_run b 
on a.masterkey=b.masterkey;

drop table if exists ssharma_db.at_lastmnth_clicks_run;
create table ssharma_db.at_lastmnth_clicks_run as 
select distinct
unk_shop_id,
visit_num,
visid_high,
visid_low,
pagename,
hier1,
prop1,
purchaseid,
prop33,
country,
t_time_info,
product_list,
date_time
from ssharma_db.at_last_qrtr_clicks_run
where unix_timestamp(date_time,'yyyy-MM-dd')  between UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}',-31), 'yyyy-MM-dd') AND 
UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}',-1), 'yyyy-MM-dd');


drop table if exists ssharma_db.at_lastmnth_purch_run;
create table ssharma_db.at_lastmnth_purch_run as 
select customer_key,item_qty,product_key
from mds_new.ods_orderline_t 
where brand='AT' and country='US' 
and unix_timestamp(transaction_date,'yyyy-MM-dd')  between UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}',-31), 'yyyy-MM-dd') AND 
UNIX_TIMESTAMP(DATE_ADD('${hiveconf:DATE_NOW}',-1), 'yyyy-MM-dd')
and order_status in ('R','O') and item_qty>0 and sales_amt>0;

drop table if exists ssharma_db.at_lastmnth_txn_run;
create table ssharma_db.at_lastmnth_txn_run as
select a.customer_key,a.item_qty,a.product_key,b.mdse_div_desc,b.mdse_dept_desc,b.mdse_class_desc,
(case when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='CAPRIS' then 'GIRLS CAPRIS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='PANTS' then 'GIRLS PANTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SHORTS' then 'GIRLS SHORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SKIRTS/SKORTS' then 'GIRLS SKIRTS/SKORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS ACCESSORY' then 'GIRLS ACCESSORY'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS INTIMATES' then 'GIRLS INTIMATES'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS SWIMWEAR' then 'GIRLS SWIMWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='OUTERWEAR' then 'GIRLS OUTERWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc<>'SWEATSHIRTS' then 'GIRLS TOPS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc='SWEATSHIRTS' then 'GIRLS SWEATSHIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='ACCESSORY' then 'WOMENS ACCESSORY' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'CAPRI')>0 then 'WOMENS CAPRIS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='DRESS' then 'WOMENS DRESSES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (b.mdse_class_desc='JACKET' or b.mdse_class_desc='OUTERWEAR' or b.mdse_class_desc='VEST') then 'WOMENS OUTERWEAR'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'PANT')>0 then 'WOMENS PANTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SHORT')>0 then 'WOMENS SHORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKIRT')>0 then 'WOMENS SKIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKORT')>0 then 'WOMENS SKORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SUPPORT TOPS' then 'WOMENS SUPPORT TOPS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SWEATER' then 'WOMENS SWEATERS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (instr(b.mdse_class_desc,'COVER UP')>0 or instr(b.mdse_class_desc,'TOP')>0)  then 'WOMENS TOPS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='FOOTWEAR' then 'WOMENS FOOTWEAR' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc<>'BRAS' then 'WOMENS INTIMATES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc='BRAS' then 'WOMENS BRAS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='BIKINI TOP' then 'WOMENS SWIM BIKINI'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='SWIM BOTTOMS' then 'WOMENS SWIM BOTTOMS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='ONE PIECE' then 'WOMENS SWIM ONE PIECE'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='RASHGUARDS' then 'WOMENS SWIM RASHGUARDS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='TANKINI' then 'WOMENS SWIM TANKINI'
else 'N/A' end) as category 
from ssharma_db.at_lastmnth_purch_run a 
inner join 
mds_new.ods_product_t b 
on a.product_key=b.product_key;

drop table if exists ssharma_db.at_lastmnth_master_run;
create table ssharma_db.at_lastmnth_master_run as 
select a.*,b.masterkey 
from ssharma_db.at_lastmnth_txn_run a
left outer join 
(select distinct customerkey,masterkey from crmanalytics.customerresolution) b 
on a.customer_key=b.customerkey;

drop table if exists ssharma_db.at_lastmnth_master_notnull_run;
create table ssharma_db.at_lastmnth_master_notnull_run as 
select * from ssharma_db.at_lastmnth_master_run where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_lastmnth_mk_cat_run;
create table ssharma_db.at_lastmnth_mk_cat_run as 
select masterkey,category,sum(item_qty) as items_purch_lastmnth
from ssharma_db.at_lastmnth_master_notnull_run 
group by masterkey,category;

drop table if exists ssharma_db.at_lastmnth_sum_run;
create table ssharma_db.at_lastmnth_sum_run as 
select  masterkey,sum(items_purch_lastmnth) as sum_items_purch_lastmnth
from ssharma_db.at_lastmnth_mk_cat_run
group by masterkey;

drop table if exists ssharma_db.at_lastmnth_nrml_run;
create table ssharma_db.at_lastmnth_nrml_run as
select a.masterkey,a.category,
(case when b.sum_items_purch_lastmnth>0 then a.items_purch_lastmnth/b.sum_items_purch_lastmnth else 0.0 end ) as nrml_items_purch_lastmnth
from ssharma_db.at_lastmnth_mk_cat_run a 
left outer join ssharma_db.at_lastmnth_sum_run b 
on a.masterkey=b.masterkey;




drop table if exists ssharma_db.at_prd_lastmnth_clicks_run;
create table ssharma_db.at_prd_lastmnth_clicks_run as 
select *,substr(prop1,1,6) as style_cd from ssharma_db.at_lastmnth_clicks_run
where prop33='product';

drop table if exists ssharma_db.at_prd_lastmnth_clicks_desc_run;
create table ssharma_db.at_prd_lastmnth_clicks_desc_run as 
select a.*,b.mdse_div_desc,b.mdse_dept_desc,b.mdse_class_desc
from ssharma_db.at_prd_lastmnth_clicks_run a 
inner join (select distinct style_cd,mdse_div_desc,mdse_dept_desc,mdse_class_desc from mds_new.ods_product_t where mdse_div_desc is not null and mdse_dept_desc is not null and mdse_class_desc is not null and mdse_corp_desc='ATH DIR') b 
on a.style_cd=b.style_cd
;

drop table if exists ssharma_db.at_lastmnth_clicks_cat_run;
create table ssharma_db.at_lastmnth_clicks_cat_run as 
select *,
(case when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='BOTTOMS' and mdse_class_desc='CAPRIS' then 'GIRLS CAPRIS'
when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='BOTTOMS' and mdse_class_desc='PANTS' then 'GIRLS PANTS'
when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='BOTTOMS' and mdse_class_desc='SHORTS' then 'GIRLS SHORTS'
when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='BOTTOMS' and mdse_class_desc='SKIRTS/SKORTS' then 'GIRLS SKIRTS/SKORTS'
when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='GIRLS ACCESSORY' then 'GIRLS ACCESSORY'
when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='GIRLS INTIMATES' then 'GIRLS INTIMATES'
when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='GIRLS SWIMWEAR' then 'GIRLS SWIMWEAR'
when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='OUTERWEAR' then 'GIRLS OUTERWEAR'
when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='TOPS/SUPPORT/SWEATER' and mdse_class_desc<>'SWEATSHIRTS' then 'GIRLS TOPS'
when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='TOPS/SUPPORT/SWEATER' and mdse_class_desc='SWEATSHIRTS' then 'GIRLS SWEATSHIRTS' 
when mdse_div_desc='WOMENS' and mdse_dept_desc='ACCESSORY' then 'WOMENS ACCESSORY' 
when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and instr(mdse_class_desc,'CAPRI')>0 then 'WOMENS CAPRIS' 
when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and mdse_class_desc='DRESS' then 'WOMENS DRESSES'
when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and (mdse_class_desc='JACKET' or mdse_class_desc='OUTERWEAR' or mdse_class_desc='VEST') then 'WOMENS OUTERWEAR'
when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and instr(mdse_class_desc,'PANT')>0 then 'WOMENS PANTS' 
when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and instr(mdse_class_desc,'SHORT')>0 then 'WOMENS SHORTS' 
when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and instr(mdse_class_desc,'SKIRT')>0 then 'WOMENS SKIRTS' 
when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and instr(mdse_class_desc,'SKORT')>0 then 'WOMENS SKORTS' 
when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and mdse_class_desc='SUPPORT TOPS' then 'WOMENS SUPPORT TOPS' 
when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and mdse_class_desc='SWEATER' then 'WOMENS SWEATERS' 
when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and (instr(mdse_class_desc,'COVER UP')>0 or instr(mdse_class_desc,'TOP')>0)  then 'WOMENS TOPS'
when mdse_div_desc='WOMENS' and mdse_dept_desc='FOOTWEAR' then 'WOMENS FOOTWEAR' 
when mdse_div_desc='WOMENS' and mdse_dept_desc='INTIMATES' and mdse_class_desc<>'BRAS' then 'WOMENS INTIMATES'
when mdse_div_desc='WOMENS' and mdse_dept_desc='INTIMATES' and mdse_class_desc='BRAS' then 'WOMENS BRAS'
when mdse_div_desc='WOMENS' and mdse_dept_desc='SWIMWEAR' and mdse_class_desc='BIKINI TOP' then 'WOMENS SWIM BIKINI'
when mdse_div_desc='WOMENS' and mdse_dept_desc='SWIMWEAR' and mdse_class_desc='SWIM BOTTOMS' then 'WOMENS SWIM BOTTOMS'
when mdse_div_desc='WOMENS' and mdse_dept_desc='SWIMWEAR' and mdse_class_desc='ONE PIECE' then 'WOMENS SWIM ONE PIECE'
when mdse_div_desc='WOMENS' and mdse_dept_desc='SWIMWEAR' and mdse_class_desc='RASHGUARDS' then 'WOMENS SWIM RASHGUARDS'
when mdse_div_desc='WOMENS' and mdse_dept_desc='SWIMWEAR' and mdse_class_desc='TANKINI' then 'WOMENS SWIM TANKINI'
else 'N/A' end) as category
from ssharma_db.at_prd_lastmnth_clicks_desc_run;




drop table if exists ssharma_db.at_lastmnth_bagadd_run;
create table ssharma_db.at_lastmnth_bagadd_run as 
select distinct
unk_shop_id,
visit_num,
visid_high,
visid_low,
pagename,
hier1,
prop1,
purchaseid,
prop33,
country,
t_time_info,
product_list 
from ssharma_db.at_lastmnth_clicks_run
where  prop33='inlineBagAdd';

drop table if exists ssharma_db.at_lastmnth_purchasenull_run;
create table ssharma_db.at_lastmnth_purchasenull_run as 
select unk_shop_id,visit_num
from ssharma_db.at_lastmnth_clicks_run
where unk_shop_id != "" and (PurchaseID IS NULL or PurchaseID=='' or PurchaseID==' ');

drop table if exists ssharma_db.at_lastmnth_abandon_basket_run;
create table ssharma_db.at_lastmnth_abandon_basket_run  as 
select a.* 
from ssharma_db.at_lastmnth_bagadd_run a 
inner join  ssharma_db.at_lastmnth_purchasenull_run b 
on a.unk_shop_id=b.unk_shop_id and a.visit_num=b.visit_num;

drop table if exists ssharma_db.at_lastmnth_abandon_basket_prod_run;
create table ssharma_db.at_lastmnth_abandon_basket_prod_run as 
select a.*,b.mdse_div_desc,b.mdse_dept_desc,b.mdse_class_desc,
(case when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='CAPRIS' then 'GIRLS CAPRIS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='PANTS' then 'GIRLS PANTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SHORTS' then 'GIRLS SHORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='BOTTOMS' and b.mdse_class_desc='SKIRTS/SKORTS' then 'GIRLS SKIRTS/SKORTS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS ACCESSORY' then 'GIRLS ACCESSORY'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS INTIMATES' then 'GIRLS INTIMATES'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='GIRLS SWIMWEAR' then 'GIRLS SWIMWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='OUTERWEAR' then 'GIRLS OUTERWEAR'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc<>'SWEATSHIRTS' then 'GIRLS TOPS'
when b.mdse_div_desc='ATHLETA GIRLS' and b.mdse_dept_desc='TOPS/SUPPORT/SWEATER' and b.mdse_class_desc='SWEATSHIRTS' then 'GIRLS SWEATSHIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='ACCESSORY' then 'WOMENS ACCESSORY' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'CAPRI')>0 then 'WOMENS CAPRIS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='DRESS' then 'WOMENS DRESSES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (b.mdse_class_desc='JACKET' or b.mdse_class_desc='OUTERWEAR' or b.mdse_class_desc='VEST') then 'WOMENS OUTERWEAR'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'PANT')>0 then 'WOMENS PANTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SHORT')>0 then 'WOMENS SHORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKIRT')>0 then 'WOMENS SKIRTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and instr(b.mdse_class_desc,'SKORT')>0 then 'WOMENS SKORTS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SUPPORT TOPS' then 'WOMENS SUPPORT TOPS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and b.mdse_class_desc='SWEATER' then 'WOMENS SWEATERS' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='APPAREL' and (instr(b.mdse_class_desc,'COVER UP')>0 or instr(b.mdse_class_desc,'TOP')>0)  then 'WOMENS TOPS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='FOOTWEAR' then 'WOMENS FOOTWEAR' 
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc<>'BRAS' then 'WOMENS INTIMATES'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='INTIMATES' and b.mdse_class_desc='BRAS' then 'WOMENS BRAS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='BIKINI TOP' then 'WOMENS SWIM BIKINI'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='SWIM BOTTOMS' then 'WOMENS SWIM BOTTOMS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='ONE PIECE' then 'WOMENS SWIM ONE PIECE'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='RASHGUARDS' then 'WOMENS SWIM RASHGUARDS'
when b.mdse_div_desc='WOMENS' and b.mdse_dept_desc='SWIMWEAR' and b.mdse_class_desc='TANKINI' then 'WOMENS SWIM TANKINI'
else 'N/A' end) as category
from ssharma_db.at_lastmnth_abandon_basket_run a 
inner join (select distinct style_cd,mdse_div_desc,mdse_dept_desc,mdse_class_desc from mds_new.ods_product_t where mdse_div_desc is not null and mdse_dept_desc is not null and mdse_class_desc is not null and mdse_corp_desc='ATH DIR') b 
on substr(a.prop1,1,6)=b.style_cd
;


drop table if exists ssharma_db.at_lastmnth_clicks_cat_master_run;
create table ssharma_db.at_lastmnth_clicks_cat_master_run as 
select a.*,b.masterkey
from ssharma_db.at_lastmnth_clicks_cat_run a 
left outer join 
(select distinct unknownshopperid,masterkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;

drop table if exists ssharma_db.at_lastmnth_abandon_basket_prod_master_run;
create table ssharma_db.at_lastmnth_abandon_basket_prod_master_run as 
select a.*,b.masterkey
from  ssharma_db.at_lastmnth_abandon_basket_prod_run a 
left outer join 
(select distinct unknownshopperid,masterkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;

drop table if exists ssharma_db.at_lastmnth_abandon_basket_prod_master_notnull_run;
create table ssharma_db.at_lastmnth_abandon_basket_prod_master_notnull_run as
select * from  ssharma_db.at_lastmnth_abandon_basket_prod_master_run where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_lastmnth_clicks_cat_master_notnull_run;
create table ssharma_db.at_lastmnth_clicks_cat_master_notnull_run as
select * from  ssharma_db.at_lastmnth_clicks_cat_master_run where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_lastmnth_abandon_mk_cat_run;
create table ssharma_db.at_lastmnth_abandon_mk_cat_run as 
select masterkey,category,count(distinct t_time_info) as items_abandoned_lastmnth
from ssharma_db.at_lastmnth_abandon_basket_prod_master_notnull_run
group by masterkey,category;

drop table if exists ssharma_db.at_lastmnth_clicks_mk_cat_run;
create table ssharma_db.at_lastmnth_clicks_mk_cat_run as 
select masterkey,category,count(distinct t_time_info) as items_browsed_lastmnth
from ssharma_db.at_lastmnth_clicks_cat_master_notnull_run
group by masterkey,category;

drop table if exists ssharma_db.at_lastmnth_abandon_sum_run;
create table ssharma_db.at_lastmnth_abandon_sum_run as 
select  masterkey,sum(items_abandoned_lastmnth) as sum_items_abandoned_lastmnth
from ssharma_db.at_lastmnth_abandon_mk_cat_run
group by masterkey;

drop table if exists ssharma_db.at_lastmnth_clicks_sum_run;
create table ssharma_db.at_lastmnth_clicks_sum_run as 
select  masterkey,sum(items_browsed_lastmnth) as sum_items_browsed_lastmnth
from ssharma_db.at_lastmnth_clicks_mk_cat_run
group by masterkey;

drop table if exists ssharma_db.at_lastmnth_abandon_nrml_run;
create table ssharma_db.at_lastmnth_abandon_nrml_run as
select a.masterkey,a.category,
(case when b.sum_items_abandoned_lastmnth>0 then a.items_abandoned_lastmnth/b.sum_items_abandoned_lastmnth else 0.0 end ) as nrml_items_abandoned_lastmnth
from ssharma_db.at_lastmnth_abandon_mk_cat_run a 
left outer join ssharma_db.at_lastmnth_abandon_sum_run b 
on a.masterkey=b.masterkey;

drop table if exists ssharma_db.at_lastmnth_clicks_nrml_run;
create table ssharma_db.at_lastmnth_clicks_nrml_run as
select a.masterkey,a.category,
(case when b.sum_items_browsed_lastmnth>0 then a.items_browsed_lastmnth/b.sum_items_browsed_lastmnth else 0.0 end) as nrml_items_browsed_lastmnth
from ssharma_db.at_lastmnth_clicks_mk_cat_run a 
left outer join ssharma_db.at_lastmnth_clicks_sum_run b 
on a.masterkey=b.masterkey;





drop table if exists ssharma_db.at_abandon_maxdt_run;
create table ssharma_db.at_abandon_maxdt_run as 
select masterkey,category,datediff('${hiveconf:DATE_NOW}',max(date_time))/90 as recency_abandon
from ssharma_db.at_abandon_basket_prod_master_notnull_run
group by masterkey,category;



drop table if exists ssharma_db.at_clicks_maxdt_run;
create table ssharma_db.at_clicks_maxdt_run as 
select masterkey,category,datediff('${hiveconf:DATE_NOW}',max(date_time))/90 as recency_click
from ssharma_db.at_clicks_cat_master_notnull_run
group by masterkey,category;



drop table if exists ssharma_db.at_sugg_maxdt_run;
create table ssharma_db.at_sugg_maxdt_run as 
select masterkey,category,datediff('${hiveconf:DATE_NOW}',max(transaction_date))/365 as recency_purch
from ssharma_db.at_sugg_master_notnull_run
group by masterkey,category;



drop table if exists ssharma_db.at_lastmnth_rec_run;
create table ssharma_db.at_lastmnth_rec_run as 
select a.masterkey,              
a.category,
a.nrml_items_abandoned,    
a.nrml_items_browsed,      
a.nrml_items_purchased,    
a.nrml_items_purch_first, 
coalesce(b.nrml_items_purch_lastmnth,0) as nrml_items_purch_lastmnth,
coalesce(c.nrml_items_abandoned_lastmnth,0) as nrml_items_abandoned_lastmnth,
coalesce(d.nrml_items_browsed_lastmnth,0) as nrml_items_browsed_lastmnth,
coalesce(e.recency_purch,1) as recency_purch,
coalesce(f.recency_abandon,1) as recency_abandon,
coalesce(g.recency_click,1) as recency_click
from 
ssharma_db.at_normal_first_run a
left outer join 
ssharma_db.at_lastmnth_nrml_run b 
on a.masterkey=b.masterkey and a.category=b.category 
left outer join 
ssharma_db.at_lastmnth_abandon_nrml_run c 
on a.masterkey=c.masterkey and a.category=c.category 
left outer join
ssharma_db.at_lastmnth_clicks_nrml_run d
on a.masterkey=d.masterkey and a.category=d.category 
left outer join
ssharma_db.at_sugg_maxdt_run e
on a.masterkey=e.masterkey and a.category=e.category 
left outer join
ssharma_db.at_abandon_maxdt_run f
on a.masterkey=f.masterkey and a.category=f.category 
left outer join
ssharma_db.at_clicks_maxdt_run g 
on a.masterkey=g.masterkey and a.category=g.category; 


