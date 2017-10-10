--drop table if exists ssharma_db.at_transactions;
--create table ssharma_db.at_transactions as
--select distinct customer_key,transaction_num,order_num,order_status,transaction_type_cd,to_date(demand_date) as demand_date, to_date(ship_date) as ship_date,
--(
--case when order_status='O' and demand_date is NOT NULL then to_date(demand_date) else to_date(ship_date) end
--) as transaction_date,gross_sales_amt,discount_amt
--from mds_new.ods_orderheader_t 
--where 
--(
--(order_status='R' and unix_timestamp(to_date(ship_date),'yyyy-MM-dd') between unix_timestamp('2014-01-01','yyyy-MM-dd') and unix_timestamp('2014-12-31','yyyy-MM-dd'))
--or 
--(order_status='O' and unix_timestamp(to_date(demand_date),'yyyy-MM-dd') between unix_timestamp('2014-01-01','yyyy-MM-dd') and unix_timestamp('2014-12-31','yyyy-MM-dd'))
--) 
--and country= 'US' and brand='AT' and gross_sales_amt is NOT NULL 
--and
--((transaction_type_cd = 'S' and item_qty>0) or (transaction_type_cd = 'R' and item_qty<0) or transaction_type_cd='M');

--drop table if exists ssharma_db.at_transactions_line;
--create table ssharma_db.at_transactions_line as 
--select distinct customer_key,transaction_num,line_num,b.order_num,b.order_status,b.transaction_date,sales_amt,
--(case when sales_amt>0  and item_qty >0 then 'S' else 'R' end) as transaction_type_cd,
--item_qty,
--product_key 
--from 
--mds_new.ods_orderline_t a
--inner join 
--ssharma_db.at_transactions b
--on a.customer_key=b.customer_key 
--and a.transaction_num=b.transaction_num;

--drop table if exists ssharma_db.at_transactions_prod;
--create table ssharma_db.at_transactions_prod as 
--select distinct customer_key,transaction_num,line_num,order_num,order_status,transaction_date,sales_amt,
--transaction_type_cd,
--item_qty,
--product_key,
--b.mdse_div_desc,
--b.mdse_dept_desc,
--b.mdse_class_desc 
--from 
--ssharma_db.at_transactions_line a 
--inner join 
--mds_new.ods_product_t b 
--on a.product_key=b.product_key;

--drop table if exists ssharma_db.at_sugg;
--create table ssharma_db.at_sugg as 
--select distinct customer_key,transaction_num,line_num,order_num,order_status,transaction_date,sales_amt,
--transaction_type_cd,
--item_qty,
--product_key,
--mdse_div_desc,
--mdse_dept_desc,
--mdse_class_desc,
--(case when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='BOTTOMS' and mdse_class_desc='CAPRIS' then 'GIRLS CAPRIS'
--when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='BOTTOMS' and mdse_class_desc='PANTS' then 'GIRLS PANTS'
--when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='BOTTOMS' and mdse_class_desc='SHORTS' then 'GIRLS SHORTS'
--when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='BOTTOMS' and mdse_class_desc='SKIRTS/SKORTS' then 'GIRLS SKIRTS/SKORTS'
--when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='GIRLS ACCESSORY' then 'GIRLS ACCESSORY'
--when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='GIRLS INTIMATES' then 'GIRLS INTIMATES'
--when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='GIRLS SWIMWEAR' then 'GIRLS SWIMWEAR'
--when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='OUTERWEAR' then 'GIRLS OUTERWEAR'
--when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='TOPS/SUPPORT/SWEATER' and mdse_class_desc<>'SWEATSHIRTS' then 'GIRLS TOPS'
--when mdse_div_desc='ATHLETA GIRLS' and mdse_dept_desc='TOPS/SUPPORT/SWEATER' and mdse_class_desc='SWEATSHIRTS' then 'GIRLS SWEATSHIRTS' 
--when mdse_div_desc='WOMENS' and mdse_dept_desc='ACCESSORY' then 'WOMENS ACCESSORY' 
--when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and instr(mdse_class_desc,'CAPRI')>0 then 'WOMENS CAPRIS' 
--when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and mdse_class_desc='DRESS' then 'WOMENS DRESSES'
--when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and (mdse_class_desc='JACKET' or mdse_class_desc='OUTERWEAR' or mdse_class_desc='VEST') then 'WOMENS OUTERWEAR'
--when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and instr(mdse_class_desc,'PANT')>0 then 'WOMENS PANTS' 
--when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and instr(mdse_class_desc,'SHORT')>0 then 'WOMENS SHORTS' 
--when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and instr(mdse_class_desc,'SKIRT')>0 then 'WOMENS SKIRTS' 
--when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and instr(mdse_class_desc,'SKORT')>0 then 'WOMENS SKORTS' 
--when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and mdse_class_desc='SUPPORT TOPS' then 'WOMENS SUPPORT TOPS' 
--when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and mdse_class_desc='SWEATER' then 'WOMENS SWEATERS' 
--when mdse_div_desc='WOMENS' and mdse_dept_desc='APPAREL' and (instr(mdse_class_desc,'COVER UP')>0 or instr(mdse_class_desc,'TOP')>0)  then 'WOMENS TOPS'
--when mdse_div_desc='WOMENS' and mdse_dept_desc='FOOTWEAR' then 'WOMENS FOOTWEAR' 
--when mdse_div_desc='WOMENS' and mdse_dept_desc='INTIMATES' and mdse_class_desc<>'BRAS' then 'WOMENS INTIMATES'
--when mdse_div_desc='WOMENS' and mdse_dept_desc='INTIMATES' and mdse_class_desc='BRAS' then 'WOMENS BRAS'
--when mdse_div_desc='WOMENS' and mdse_dept_desc='SWIMWEAR' and mdse_class_desc='BIKINI TOP' then 'WOMENS SWIM BIKINI'
--when mdse_div_desc='WOMENS' and mdse_dept_desc='SWIMWEAR' and mdse_class_desc='SWIM BOTTOMS' then 'WOMENS SWIM BOTTOMS'
--when mdse_div_desc='WOMENS' and mdse_dept_desc='SWIMWEAR' and mdse_class_desc='ONE PIECE' then 'WOMENS SWIM ONE PIECE'
--when mdse_div_desc='WOMENS' and mdse_dept_desc='SWIMWEAR' and mdse_class_desc='RASHGUARDS' then 'WOMENS SWIM RASHGUARDS'
--when mdse_div_desc='WOMENS' and mdse_dept_desc='SWIMWEAR' and mdse_class_desc='TANKINI' then 'WOMENS SWIM TANKINI'
--else 'N/A' end) as category
--from ssharma_db.at_transactions_prod;

--drop table if exists ssharma_db.item_qty_cat;
--create table ssharma_db.item_qty_cat as 
--select category,sum(item_qty) as sum_items
--from ssharma_db.at_sugg 
--group by category;

--drop table if exists ssharma_db.check_na;
--create table ssharma_db.check_na as 
--select a.non_merch_code 
--from mds_new.ods_orderline_t a 
--inner join 
--ssharma_db.at_sugg b 
--on a.customer_key=b.customer_key and a.transaction_num=b.transaction_num and a.line_num=b.line_num
--where b.category='N/A';


drop table if exists ssharma_db.at_last_qrtr_clicks;
create table ssharma_db.at_last_qrtr_clicks as 
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
from omniture.hit_data_t 
where (instr(pagename,'at:browse')>0 or instr(pagename,'mobile:at')>0) and (date_time) between ('2015-10-01') and ('2015-12-31');

drop table if exists ssharma_db.at_prd_clicks;
create table ssharma_db.at_prd_clicks as 
select *,substr(prop1,1,6) as style_cd from ssharma_db.at_last_qrtr_clicks
where prop33='product';

drop table if exists ssharma_db.at_prd_clicks_desc;
create table ssharma_db.at_prd_clicks_desc as 
select a.*,b.mdse_div_desc,b.mdse_dept_desc,b.mdse_class_desc
from ssharma_db.at_prd_clicks a 
inner join (select distinct style_cd,mdse_div_desc,mdse_dept_desc,mdse_class_desc from mds_new.ods_product_t where mdse_div_desc is not null and mdse_dept_desc is not null and mdse_class_desc is not null and mdse_corp_desc='ATH DIR') b 
on a.style_cd=b.style_cd
;

drop table if exists ssharma_db.at_clicks_cat;
create table ssharma_db.at_clicks_cat as 
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
from ssharma_db.at_prd_clicks_desc;




drop table if exists ssharma_db.at_bagadd;
create table ssharma_db.at_bagadd as 
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
from ssharma_db.at_last_qrtr_clicks
where  prop33='inlineBagAdd';

drop table if exists ssharma_db.at_purchasenull;
create table ssharma_db.at_purchasenull as 
select unk_shop_id,visit_num
from ssharma_db.at_last_qrtr_clicks
where unk_shop_id != "" and (PurchaseID IS NULL or PurchaseID=='' or PurchaseID==' ');

drop table if exists ssharma_db.at_abandon_basket;
create table ssharma_db.at_abandon_basket  as 
select a.* 
from ssharma_db.at_bagadd a 
inner join  ssharma_db.at_purchasenull b 
on a.unk_shop_id=b.unk_shop_id and a.visit_num=b.visit_num;

drop table if exists ssharma_db.at_abandon_basket_prod;
create table ssharma_db.at_abandon_basket_prod as 
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
from ssharma_db.at_abandon_basket a 
inner join (select distinct style_cd,mdse_div_desc,mdse_dept_desc,mdse_class_desc from mds_new.ods_product_t where mdse_div_desc is not null and mdse_dept_desc is not null and mdse_class_desc is not null and mdse_corp_desc='ATH DIR') b 
on substr(a.prop1,1,6)=b.style_cd
;


drop table if exists ssharma_db.at_clicks_cat_master;
create table ssharma_db.at_clicks_cat_master as 
select a.*,b.masterkey
from ssharma_db.at_clicks_cat a 
left outer join 
(select distinct unknownshopperid,masterkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;

drop table if exists ssharma_db.at_abandon_basket_prod_master;
create table ssharma_db.at_abandon_basket_prod_master as 
select a.*,b.masterkey
from  ssharma_db.at_abandon_basket_prod a 
left outer join 
(select distinct unknownshopperid,masterkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;


create table ssharma_db.at_sugg
(category string,
brand string,
country string,
customer_key bigint,
product_key bigint,
sales_amt double,
discount_amt double,
transaction_date string, 
order_status string,
item_qty bigint,
mdse_div_desc string,
mdse_dept_desc string,
mdse_class_desc string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/xsa2d2mf/at_sugg.txt' INTO TABLE ssharma_db.at_sugg;




drop table if exists ssharma_db.at_sugg_master;
create table ssharma_db.at_sugg_master as 
select a.*,b.masterkey
from  ssharma_db.at_sugg a 
left outer join 
(select distinct customerkey,masterkey from crmanalytics.customerresolution) b 
on a.customer_key=b.customerkey;

drop table if exists ssharma_db.at_sugg_master_notnull;
create table ssharma_db.at_sugg_master_notnull as 
select * from ssharma_db.at_sugg_master where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_abandon_basket_prod_master_notnull;
create table ssharma_db.at_abandon_basket_prod_master_notnull as
select * from  ssharma_db.at_abandon_basket_prod_master where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_clicks_cat_master_notnull;
create table ssharma_db.at_clicks_cat_master_notnull as
select * from  ssharma_db.at_clicks_cat_master where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_txn_mk_cat;
create table ssharma_db.at_txn_mk_cat as 
select masterkey,category,sum(item_qty) as items_purch
from ssharma_db.at_sugg_master_notnull 
group by masterkey,category;

drop table if exists ssharma_db.at_abandon_mk_cat;
create table ssharma_db.at_abandon_mk_cat as 
select masterkey,category,count(distinct t_time_info) as items_abandoned
from ssharma_db.at_abandon_basket_prod_master_notnull
group by masterkey,category;

drop table if exists ssharma_db.at_clicks_mk_cat;
create table ssharma_db.at_clicks_mk_cat as 
select masterkey,category,count(distinct t_time_info) as items_browsed
from ssharma_db.at_clicks_cat_master_notnull
group by masterkey,category;

drop table if exists ssharma_db.at_pred_window;
create table ssharma_db.at_pred_window as 
select customer_key,item_qty,product_key
from mds_new.ods_orderline_t 
where brand='AT' and country='US' 
and unix_timestamp(transaction_date,'yyyy-MM-dd') between unix_timestamp('2016-01-01','yyyy-MM-dd') and unix_timestamp('2016-03-31','yyyy-MM-dd')
and order_status in ('R','O') and item_qty>0 and sales_amt>0;

drop table if exists ssharma_db.at_pred_prod;
create table ssharma_db.at_pred_prod as
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
from ssharma_db.at_pred_window a 
inner join 
mds_new.ods_product_t b 
on a.product_key=b.product_key;

drop table if exists ssharma_db.at_pred_master;
create table ssharma_db.at_pred_master as 
select a.*,b.masterkey 
from ssharma_db.at_pred_prod a
left outer join 
(select distinct customerkey,masterkey from crmanalytics.customerresolution) b 
on a.customer_key=b.customerkey;

drop table if exists ssharma_db.at_pred_master_notnull;
create table ssharma_db.at_pred_master_notnull as 
select * from ssharma_db.at_pred_master where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_pred_mk_cat;
create table ssharma_db.at_pred_mk_cat as 
select masterkey,category,sum(item_qty) as items_purch_pred
from ssharma_db.at_pred_master_notnull 
group by masterkey,category;

drop table if exists ssharma_db.at_browser;
create table ssharma_db.at_browser as 
select distinct (case when a.masterkey is null then b.masterkey else a.masterkey end) as masterkey,
(case when a.category is null then b.category else a.category end) as category,
coalesce(a.items_abandoned,0) as items_abandoned,coalesce(b.items_browsed,0) as items_browsed
from ssharma_db.at_abandon_mk_cat a
full outer join
ssharma_db.at_clicks_mk_cat b 
on a.masterkey=b.masterkey and a.category=b.category;

drop table if exists ssharma_db.at_browser_purchaser;
create table ssharma_db.at_browser_purchaser as 
select distinct (case when a.masterkey is null then b.masterkey else a.masterkey end) as masterkey,
(case when a.category is null then b.category else a.category end) as category,
coalesce(a.items_abandoned,0) as items_abandoned,coalesce(a.items_browsed,0) as items_browsed,
coalesce(b.items_purch,0) as items_purchased
from ssharma_db.at_browser a 
full outer join 
ssharma_db.at_txn_mk_cat b 
on a.masterkey=b.masterkey and a.category=b.category;

drop table if exists ssharma_db.at_target;
create table ssharma_db.at_target as 
select distinct a.masterkey,a.category,a.items_abandoned,a.items_browsed,a.items_purchased,
coalesce(b.items_purch_pred,0) as items_purch_pred
from ssharma_db.at_browser_purchaser a 
left outer join ssharma_db.at_pred_mk_cat b 
on a.masterkey=b.masterkey and a.category=b.category;

drop table if exists ssharma_db.at_masterkeys_features;
create table ssharma_db.at_masterkeys_features as 
select distinct masterkey from ssharma_db.at_browser_purchaser;


drop table if exists ssharma_db.at_feature_pred;
create table ssharma_db.at_feature_pred as 
select distinct a.masterkey,b.category,b.items_purch_pred
from ssharma_db.at_masterkeys_features a 
inner  join 
ssharma_db.at_pred_mk_cat b 
on a.masterkey=b.masterkey;

drop table if exists ssharma_db.at_target_full;
create table ssharma_db.at_target_full as 
select distinct (case when a.masterkey is null then b.masterkey else a.masterkey end) as masterkey,
(case when a.category is null then b.category else a.category end) as category,
coalesce(a.items_abandoned,0) as items_abandoned,coalesce(a.items_browsed,0) as items_browsed,
coalesce(a.items_purchased,0) as items_purchased,coalesce(b.items_purch_pred,0) as items_purch_pred
from ssharma_db.at_browser_purchaser a 
full outer join 
ssharma_db.at_feature_pred b 
on a.masterkey=b.masterkey and a.category=b.category;



drop table if exists ssharma_db.at_fisrt_qrtr_purch;
create table ssharma_db.at_fisrt_qrtr_purch as 
select customer_key,item_qty,product_key
from mds_new.ods_orderline_t 
where brand='AT' and country='US' 
and unix_timestamp(transaction_date,'yyyy-MM-dd') between unix_timestamp('2015-01-01','yyyy-MM-dd') and unix_timestamp('2015-03-31','yyyy-MM-dd')
and order_status in ('R','O') and item_qty>0 and sales_amt>0;

drop table if exists ssharma_db.at_first_qrtr_txn;
create table ssharma_db.at_first_qrtr_txn as
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
from ssharma_db.at_fisrt_qrtr_purch a 
inner join 
mds_new.ods_product_t b 
on a.product_key=b.product_key;

drop table if exists ssharma_db.at_first_master;
create table ssharma_db.at_first_master as 
select a.*,b.masterkey 
from ssharma_db.at_first_qrtr_txn a
left outer join 
(select distinct customerkey,masterkey from crmanalytics.customerresolution) b 
on a.customer_key=b.customerkey;

drop table if exists ssharma_db.at_first_master_notnull;
create table ssharma_db.at_first_master_notnull as 
select * from ssharma_db.at_first_master where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_first_mk_cat;
create table ssharma_db.at_first_mk_cat as 
select masterkey,category,sum(item_qty) as items_purch_first
from ssharma_db.at_first_master_notnull 
group by masterkey,category;

drop table if exists ssharma_db.at_browser_purchaser_first;
create table ssharma_db.at_browser_purchaser_first as 
select distinct (case when a.masterkey is null then b.masterkey else a.masterkey end) as masterkey,
(case when a.category is null then b.category else a.category end) as category,
coalesce(a.items_abandoned,0) as items_abandoned,coalesce(a.items_browsed,0) as items_browsed,
coalesce(a.items_purchased,0) as items_purchased,
coalesce(b.items_purch_first,0) as items_purch_first
from ssharma_db.at_browser_purchaser a 
full outer join 
ssharma_db.at_first_mk_cat b 
on a.masterkey=b.masterkey and a.category=b.category;

drop table if exists ssharma_db.at_target_first;
create table ssharma_db.at_target_first as 
select distinct a.masterkey,a.category,a.items_abandoned,a.items_browsed,a.items_purchased,a.items_purch_first,
coalesce(b.items_purch_pred,0) as items_purch_pred
from ssharma_db.at_browser_purchaser_first a 
left outer join ssharma_db.at_pred_mk_cat b
on a.masterkey=b.masterkey and a.category=b.category;

drop table if exists ssharma_db.at_first_pred;
create table ssharma_db.at_first_pred as 
select a.masterkey,a.category,a.items_purch_first,
coalesce(b.items_purch_pred,0) as items_purch_pred
from ssharma_db.at_first_mk_cat a 
left outer join ssharma_db.at_pred_mk_cat b 
on a.masterkey=b.masterkey and a.category=b.category;

drop table ssharma_db.at_group_master_first;
create table ssharma_db.at_group_master_first as 
select masterkey,sum(items_abandoned) as sum_items_abandoned,
sum(items_browsed) as sum_items_browsed,
sum(items_purchased) as sum_items_purchased,
sum(items_purch_first) as sum_items_purch_first
from ssharma_db.at_target_first
group by masterkey;

drop table if exists ssharma_db.at_normal_first;
create table ssharma_db.at_normal_first as 
select a.masterkey,a.category,
(case when b.sum_items_abandoned >0 then a.items_abandoned/b.sum_items_abandoned else 0.0 end) as nrml_items_abandoned,
(case when b.sum_items_browsed >0 then a.items_browsed/b.sum_items_browsed else 0.0 end) as nrml_items_browsed,
(case when b.sum_items_purchased >0 then a.items_purchased/b.sum_items_purchased else 0.0 end) as nrml_items_purchased,
(case when b.sum_items_purch_first >0 then a.items_purch_first/b.sum_items_purch_first else 0.0 end) as nrml_items_purch_first,
a.items_purch_pred
from ssharma_db.at_target_first a 
left outer join 
ssharma_db.at_group_master_first b 
on a.masterkey=b.masterkey;

drop table if exists ssharma_db.at_normal_pred_full;
create table ssharma_db.at_normal_pred_full as 
select distinct (case when a.masterkey is null then b.masterkey else a.masterkey end) as masterkey,
(case when a.category is null then b.category else a.category end) as category,
coalesce(a.nrml_items_abandoned,0.0) as nrml_items_abandoned,
coalesce(a.nrml_items_browsed,0.0) as nrml_items_browsed,
coalesce(a.nrml_items_purchased,0.0) as nrml_items_purchased,
coalesce(a.nrml_items_purch_first,0.0) as nrml_items_purch_first,
coalesce(b.items_purch_pred,0) as items_purch_pred
from ssharma_db.at_normal_first a
full outer join 
ssharma_db.at_pred_mk_cat b 
on a.masterkey=b.masterkey and a.category=b.category;

drop table if exists ssharma_db.at_normal_pred_first_full;
create table ssharma_db.at_normal_pred_first_full as 
select distinct (case when a.masterkey is null then b.masterkey else a.masterkey end) as masterkey,
(case when a.category is null then b.category else a.category end) as category,
coalesce(a.nrml_items_abandoned,0.0) as nrml_items_abandoned,
coalesce(a.nrml_items_browsed,0.0) as nrml_items_browsed,
coalesce(a.nrml_items_purchased,0.0) as nrml_items_purchased,
coalesce(a.nrml_items_purch_first,0.0) as nrml_items_purch_first,
coalesce(a.items_purch_pred,0) as items_purch_pred,
coalesce(b.items_purch_first,0) as items_purch_first
from ssharma_db.at_normal_pred_full a 
full outer join 
ssharma_db.at_first_mk_cat b 
on a.masterkey=b.masterkey and a.category=b.category;

drop table if exists ssharma_db.at_12mnth_logic;
create table ssharma_db.at_12mnth_logic as 
select a.masterkey,a.category,a.nrml_items_abandoned,a.nrml_items_browsed,
a.nrml_items_purchased,a.nrml_items_purch_first,a.items_purch_pred,coalesce(b.items_purch,0) as items_purch
from ssharma_db.at_normal_first a 
left outer join ssharma_db.at_txn_mk_cat b 
on a.masterkey=b.masterkey and a.category=b.category;


drop table if exists ssharma_db.at_abandon_logic;
create table ssharma_db.at_abandon_logic as 
select a.masterkey,a.category,a.nrml_items_abandoned,a.nrml_items_browsed,
a.nrml_items_purchased,a.nrml_items_purch_first,a.items_purch_pred,coalesce(b.items_abandoned,0) as items_abandoned
from ssharma_db.at_onrmal_first a 
left outer join ssharma_db.at_abandon_mk_cat b 
on a.masterkey=b.masterkey and a.category=b.category;

drop table if exists ssharma_db.at_click_logic;
create table ssharma_db.at_click_logic as 
select a.masterkey,a.category,a.nrml_items_abandoned,a.nrml_items_browsed,
a.nrml_items_purchased,a.nrml_items_purch_first,a.items_purch_pred,coalesce(b.items_browsed,0) as items_browsed
from ssharma_db.at_normal_first a 
left outer join ssharma_db.at_clicks_mk_cat b 
on a.masterkey=b.masterkey and a.category=b.category;


drop table if exists ssharma_db.at_3mnth_logic;
create table ssharma_db.at_3mnth_logic as 
select a.masterkey,a.category,a.nrml_items_abandoned,a.nrml_items_browsed,
a.nrml_items_purchased,a.nrml_items_purch_first,a.items_purch_pred,coalesce(b.items_purch_first,0) as items_purch_first
from ssharma_db.at_normal_first a 
left outer join ssharma_db.at_first_mk_cat b 
on a.masterkey=b.masterkey and a.category=b.category;

drop table if exists ssharma_db.at_first_rec_purch;
create table ssharma_db.at_first_rec_purch as 
select customer_key,item_qty,product_key
from mds_new.ods_orderline_t 
where brand='AT' and country='US' 
and unix_timestamp(transaction_date,'yyyy-MM-dd') between unix_timestamp('2015-12-01','yyyy-MM-dd') and unix_timestamp('2015-01-07','yyyy-MM-dd')
and order_status in ('R','O') and item_qty>0 and sales_amt>0;

drop table if exists ssharma_db.at_first_rec_txn;
create table ssharma_db.at_first_rec_txn as
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
from ssharma_db.at_first_rec_purch a 
inner join 
mds_new.ods_product_t b 
on a.product_key=b.product_key;

drop table if exists ssharma_db.at_second_rec_purch;
create table ssharma_db.at_second_rec_purch as 
select customer_key,item_qty,product_key
from mds_new.ods_orderline_t 
where brand='AT' and country='US' 
and unix_timestamp(transaction_date,'yyyy-MM-dd') between unix_timestamp('2015-12-08','yyyy-MM-dd') and unix_timestamp('2015-01-14','yyyy-MM-dd')
and order_status in ('R','O') and item_qty>0 and sales_amt>0;

drop table if exists ssharma_db.at_second_rec_txn;
create table ssharma_db.at_second_rec_txn as
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
from ssharma_db.at_second_rec_purch a 
inner join 
mds_new.ods_product_t b 
on a.product_key=b.product_key;

drop table if exists ssharma_db.at_third_rec_purch;
create table ssharma_db.at_third_rec_purch as 
select customer_key,item_qty,product_key
from mds_new.ods_orderline_t 
where brand='AT' and country='US' 
and unix_timestamp(transaction_date,'yyyy-MM-dd') between unix_timestamp('2015-12-15','yyyy-MM-dd') and unix_timestamp('2015-01-21','yyyy-MM-dd')
and order_status in ('R','O') and item_qty>0 and sales_amt>0;

drop table if exists ssharma_db.at_third_rec_txn;
create table ssharma_db.at_third_rec_txn as
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
from ssharma_db.at_third_rec_purch a 
inner join 
mds_new.ods_product_t b 
on a.product_key=b.product_key;

drop table if exists ssharma_db.at_fourth_rec_purch;
create table ssharma_db.at_fourth_rec_purch as 
select customer_key,item_qty,product_key
from mds_new.ods_orderline_t 
where brand='AT' and country='US' 
and unix_timestamp(transaction_date,'yyyy-MM-dd') between unix_timestamp('2015-12-22','yyyy-MM-dd') and unix_timestamp('2015-01-31','yyyy-MM-dd')
and order_status in ('R','O') and item_qty>0 and sales_amt>0;

drop table if exists ssharma_db.at_fourth_rec_txn;
create table ssharma_db.at_fourth_rec_txn as
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
from ssharma_db.at_fourth_rec_purch a 
inner join 
mds_new.ods_product_t b 
on a.product_key=b.product_key;


drop table if exists ssharma_db.at_first_rec_clicks;
create table ssharma_db.at_first_rec_clicks as 
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
from omniture.hit_data_t 
where (instr(pagename,'at:browse')>0 or instr(pagename,'mobile:at')>0) and (date_time) between ('2015-12-01') and ('2015-12-07');

drop table if exists ssharma_db.at_second_rec_clicks;
create table ssharma_db.at_second_rec_clicks as 
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
from omniture.hit_data_t 
where (instr(pagename,'at:browse')>0 or instr(pagename,'mobile:at')>0) and (date_time) between ('2015-12-08') and ('2015-12-14');

drop table if exists ssharma_db.at_third_rec_clicks;
create table ssharma_db.at_third_rec_clicks as 
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
from omniture.hit_data_t 
where (instr(pagename,'at:browse')>0 or instr(pagename,'mobile:at')>0) and (date_time) between ('2015-12-15') and ('2015-12-21');

drop table if exists ssharma_db.at_fourth_rec_clicks;
create table ssharma_db.at_fourth_rec_clicks as 
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
from omniture.hit_data_t 
where (instr(pagename,'at:browse')>0 or instr(pagename,'mobile:at')>0) and (date_time) between ('2015-12-22') and ('2015-12-31');



drop table if exists ssharma_db.at_prd_first_rec_clicks;
create table ssharma_db.at_prd_first_rec_clicks as 
select *,substr(prop1,1,6) as style_cd from ssharma_db.at_first_rec_clicks
where prop33='product';

drop table if exists ssharma_db.at_prd_first_rec_clicks_desc;
create table ssharma_db.at_prd_first_rec_clicks_desc as 
select a.*,b.mdse_div_desc,b.mdse_dept_desc,b.mdse_class_desc
from ssharma_db.at_prd_first_rec_clicks a 
inner join (select distinct style_cd,mdse_div_desc,mdse_dept_desc,mdse_class_desc from mds_new.ods_product_t where mdse_div_desc is not null and mdse_dept_desc is not null and mdse_class_desc is not null and mdse_corp_desc='ATH DIR') b 
on a.style_cd=b.style_cd
;

drop table if exists ssharma_db.at_prd_first_rec_clicks_cat;
create table ssharma_db.at_prd_first_rec_clicks_cat as 
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
from ssharma_db.at_prd_first_rec_clicks_desc;

drop table if exists ssharma_db.at_first_rec_bagadd;
create table ssharma_db.at_first_rec_bagadd as 
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
from ssharma_db.at_first_rec_clicks
where  prop33='inlineBagAdd';

drop table if exists ssharma_db.at_first_rec_purchasenull;
create table ssharma_db.at_first_rec_purchasenull as 
select unk_shop_id,visit_num
from ssharma_db.at_first_rec_clicks
where unk_shop_id != "" and (PurchaseID IS NULL or PurchaseID=='' or PurchaseID==' ');

drop table if exists ssharma_db.at_first_rec_abandon_basket;
create table ssharma_db.at_first_rec_abandon_basket  as 
select a.* 
from ssharma_db.at_first_rec_bagadd a 
inner join  ssharma_db.at_first_rec_purchasenull b 
on a.unk_shop_id=b.unk_shop_id and a.visit_num=b.visit_num;

drop table if exists ssharma_db.at_first_rec_abandon_basket_prod;
create table ssharma_db.at_first_rec_abandon_basket_prod as 
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
from ssharma_db.at_first_rec_abandon_basket a 
inner join (select distinct style_cd,mdse_div_desc,mdse_dept_desc,mdse_class_desc from mds_new.ods_product_t where mdse_div_desc is not null and mdse_dept_desc is not null and mdse_class_desc is not null and mdse_corp_desc='ATH DIR') b 
on substr(a.prop1,1,6)=b.style_cd
;


drop table if exists ssharma_db.at_first_rec_clicks_cat_master;
create table ssharma_db.at_first_rec_clicks_cat_master as 
select a.*,b.masterkey
from ssharma_db.at_prd_first_rec_clicks_cat a 
left outer join 
(select distinct unknownshopperid,masterkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;

drop table if exists ssharma_db.at_first_rec_abandon_basket_prod_master;
create table ssharma_db.at_first_rec_abandon_basket_prod_master as 
select a.*,b.masterkey
from  ssharma_db.at_first_rec_abandon_basket_prod a 
left outer join 
(select distinct unknownshopperid,masterkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;





drop table if exists ssharma_db.at_prd_second_rec_clicks;
create table ssharma_db.at_prd_second_rec_clicks as 
select *,substr(prop1,1,6) as style_cd from ssharma_db.at_second_rec_clicks
where prop33='product';

drop table if exists ssharma_db.at_prd_second_rec_clicks_desc;
create table ssharma_db.at_prd_second_rec_clicks_desc as 
select a.*,b.mdse_div_desc,b.mdse_dept_desc,b.mdse_class_desc
from ssharma_db.at_prd_second_rec_clicks a 
inner join (select distinct style_cd,mdse_div_desc,mdse_dept_desc,mdse_class_desc from mds_new.ods_product_t where mdse_div_desc is not null and mdse_dept_desc is not null and mdse_class_desc is not null and mdse_corp_desc='ATH DIR') b 
on a.style_cd=b.style_cd
;

drop table if exists ssharma_db.at_prd_second_rec_clicks_cat;
create table ssharma_db.at_prd_second_rec_clicks_cat as 
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
from ssharma_db.at_prd_second_rec_clicks_desc;

drop table if exists ssharma_db.at_second_rec_bagadd;
create table ssharma_db.at_second_rec_bagadd as 
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
from ssharma_db.at_second_rec_clicks
where  prop33='inlineBagAdd';

drop table if exists ssharma_db.at_second_rec_purchasenull;
create table ssharma_db.at_second_rec_purchasenull as 
select unk_shop_id,visit_num
from ssharma_db.at_second_rec_clicks
where unk_shop_id != "" and (PurchaseID IS NULL or PurchaseID=='' or PurchaseID==' ');

drop table if exists ssharma_db.at_second_rec_abandon_basket;
create table ssharma_db.at_second_rec_abandon_basket  as 
select a.* 
from ssharma_db.at_second_rec_bagadd a 
inner join  ssharma_db.at_second_rec_purchasenull b 
on a.unk_shop_id=b.unk_shop_id and a.visit_num=b.visit_num;

drop table if exists ssharma_db.at_second_rec_abandon_basket_prod;
create table ssharma_db.at_second_rec_abandon_basket_prod as 
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
from ssharma_db.at_second_rec_abandon_basket a 
inner join (select distinct style_cd,mdse_div_desc,mdse_dept_desc,mdse_class_desc from mds_new.ods_product_t where mdse_div_desc is not null and mdse_dept_desc is not null and mdse_class_desc is not null and mdse_corp_desc='ATH DIR') b 
on substr(a.prop1,1,6)=b.style_cd
;


drop table if exists ssharma_db.at_second_rec_clicks_cat_master;
create table ssharma_db.at_second_rec_clicks_cat_master as 
select a.*,b.masterkey
from ssharma_db.at_prd_second_rec_clicks_cat a 
left outer join 
(select distinct unknownshopperid,masterkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;

drop table if exists ssharma_db.at_second_rec_abandon_basket_prod_master;
create table ssharma_db.at_second_rec_abandon_basket_prod_master as 
select a.*,b.masterkey
from  ssharma_db.at_second_rec_abandon_basket_prod a 
left outer join 
(select distinct unknownshopperid,masterkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;









drop table if exists ssharma_db.at_prd_third_rec_clicks;
create table ssharma_db.at_prd_third_rec_clicks as 
select *,substr(prop1,1,6) as style_cd from ssharma_db.at_third_rec_clicks
where prop33='product';

drop table if exists ssharma_db.at_prd_third_rec_clicks_desc;
create table ssharma_db.at_prd_third_rec_clicks_desc as 
select a.*,b.mdse_div_desc,b.mdse_dept_desc,b.mdse_class_desc
from ssharma_db.at_prd_third_rec_clicks a 
inner join (select distinct style_cd,mdse_div_desc,mdse_dept_desc,mdse_class_desc from mds_new.ods_product_t where mdse_div_desc is not null and mdse_dept_desc is not null and mdse_class_desc is not null and mdse_corp_desc='ATH DIR') b 
on a.style_cd=b.style_cd
;

drop table if exists ssharma_db.at_prd_third_rec_clicks_cat;
create table ssharma_db.at_prd_third_rec_clicks_cat as 
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
from ssharma_db.at_prd_third_rec_clicks_desc;

drop table if exists ssharma_db.at_third_rec_bagadd;
create table ssharma_db.at_third_rec_bagadd as 
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
from ssharma_db.at_third_rec_clicks
where  prop33='inlineBagAdd';

drop table if exists ssharma_db.at_third_rec_purchasenull;
create table ssharma_db.at_third_rec_purchasenull as 
select unk_shop_id,visit_num
from ssharma_db.at_third_rec_clicks
where unk_shop_id != "" and (PurchaseID IS NULL or PurchaseID=='' or PurchaseID==' ');

drop table if exists ssharma_db.at_third_rec_abandon_basket;
create table ssharma_db.at_third_rec_abandon_basket  as 
select a.* 
from ssharma_db.at_third_rec_bagadd a 
inner join  ssharma_db.at_third_rec_purchasenull b 
on a.unk_shop_id=b.unk_shop_id and a.visit_num=b.visit_num;

drop table if exists ssharma_db.at_third_rec_abandon_basket_prod;
create table ssharma_db.at_third_rec_abandon_basket_prod as 
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
from ssharma_db.at_third_rec_abandon_basket a 
inner join (select distinct style_cd,mdse_div_desc,mdse_dept_desc,mdse_class_desc from mds_new.ods_product_t where mdse_div_desc is not null and mdse_dept_desc is not null and mdse_class_desc is not null and mdse_corp_desc='ATH DIR') b 
on substr(a.prop1,1,6)=b.style_cd
;


drop table if exists ssharma_db.at_third_rec_clicks_cat_master;
create table ssharma_db.at_third_rec_clicks_cat_master as 
select a.*,b.masterkey
from ssharma_db.at_prd_third_rec_clicks_cat a 
left outer join 
(select distinct unknownshopperid,masterkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;

drop table if exists ssharma_db.at_third_rec_abandon_basket_prod_master;
create table ssharma_db.at_third_rec_abandon_basket_prod_master as 
select a.*,b.masterkey
from  ssharma_db.at_third_rec_abandon_basket_prod a 
left outer join 
(select distinct unknownshopperid,masterkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;







drop table if exists ssharma_db.at_prd_fourth_rec_clicks;
create table ssharma_db.at_prd_fourth_rec_clicks as 
select *,substr(prop1,1,6) as style_cd from ssharma_db.at_fourth_rec_clicks
where prop33='product';

drop table if exists ssharma_db.at_prd_fourth_rec_clicks_desc;
create table ssharma_db.at_prd_fourth_rec_clicks_desc as 
select a.*,b.mdse_div_desc,b.mdse_dept_desc,b.mdse_class_desc
from ssharma_db.at_prd_fourth_rec_clicks a 
inner join (select distinct style_cd,mdse_div_desc,mdse_dept_desc,mdse_class_desc from mds_new.ods_product_t where mdse_div_desc is not null and mdse_dept_desc is not null and mdse_class_desc is not null and mdse_corp_desc='ATH DIR') b 
on a.style_cd=b.style_cd
;

drop table if exists ssharma_db.at_prd_fourth_rec_clicks_cat;
create table ssharma_db.at_prd_fourth_rec_clicks_cat as 
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
from ssharma_db.at_prd_fourth_rec_clicks_desc;

drop table if exists ssharma_db.at_fourth_rec_bagadd;
create table ssharma_db.at_fourth_rec_bagadd as 
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
from ssharma_db.at_fourth_rec_clicks
where  prop33='inlineBagAdd';

drop table if exists ssharma_db.at_fourth_rec_purchasenull;
create table ssharma_db.at_fourth_rec_purchasenull as 
select unk_shop_id,visit_num
from ssharma_db.at_fourth_rec_clicks
where unk_shop_id != "" and (PurchaseID IS NULL or PurchaseID=='' or PurchaseID==' ');

drop table if exists ssharma_db.at_fourth_rec_abandon_basket;
create table ssharma_db.at_fourth_rec_abandon_basket  as 
select a.* 
from ssharma_db.at_fourth_rec_bagadd a 
inner join  ssharma_db.at_fourth_rec_purchasenull b 
on a.unk_shop_id=b.unk_shop_id and a.visit_num=b.visit_num;

drop table if exists ssharma_db.at_fourth_rec_abandon_basket_prod;
create table ssharma_db.at_fourth_rec_abandon_basket_prod as 
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
from ssharma_db.at_fourth_rec_abandon_basket a 
inner join (select distinct style_cd,mdse_div_desc,mdse_dept_desc,mdse_class_desc from mds_new.ods_product_t where mdse_div_desc is not null and mdse_dept_desc is not null and mdse_class_desc is not null and mdse_corp_desc='ATH DIR') b 
on substr(a.prop1,1,6)=b.style_cd
;


drop table if exists ssharma_db.at_fourth_rec_clicks_cat_master;
create table ssharma_db.at_fourth_rec_clicks_cat_master as 
select a.*,b.masterkey
from ssharma_db.at_prd_fourth_rec_clicks_cat a 
left outer join 
(select distinct unknownshopperid,masterkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;

drop table if exists ssharma_db.at_fourth_rec_abandon_basket_prod_master;
create table ssharma_db.at_fourth_rec_abandon_basket_prod_master as 
select a.*,b.masterkey
from  ssharma_db.at_fourth_rec_abandon_basket_prod a 
left outer join 
(select distinct unknownshopperid,masterkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;



drop table if exists ssharma_db.at_first_rec_abandon_basket_prod_master_notnull;
create table ssharma_db.at_first_rec_abandon_basket_prod_master_notnull as
select * from  ssharma_db.at_first_rec_abandon_basket_prod_master where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_first_rec_clicks_cat_master_notnull;
create table ssharma_db.at_first_rec_clicks_cat_master_notnull as
select * from  ssharma_db.at_first_rec_clicks_cat_master where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_second_rec_abandon_basket_prod_master_notnull;
create table ssharma_db.at_second_rec_abandon_basket_prod_master_notnull as
select * from  ssharma_db.at_second_rec_abandon_basket_prod_master where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_second_rec_clicks_cat_master_notnull;
create table ssharma_db.at_second_rec_clicks_cat_master_notnull as
select * from  ssharma_db.at_second_rec_clicks_cat_master where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_third_rec_abandon_basket_prod_master_notnull;
create table ssharma_db.at_third_rec_abandon_basket_prod_master_notnull as
select * from  ssharma_db.at_third_rec_abandon_basket_prod_master where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_third_rec_clicks_cat_master_notnull;
create table ssharma_db.at_third_rec_clicks_cat_master_notnull as
select * from  ssharma_db.at_third_rec_clicks_cat_master where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_fourth_rec_abandon_basket_prod_master_notnull;
create table ssharma_db.at_fourth_rec_abandon_basket_prod_master_notnull as
select * from  ssharma_db.at_fourth_rec_abandon_basket_prod_master where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_fourth_rec_clicks_cat_master_notnull;
create table ssharma_db.at_fourth_rec_clicks_cat_master_notnull as
select * from  ssharma_db.at_fourth_rec_clicks_cat_master where masterkey is not null and category <> 'N/A';


drop table if exists ssharma_db.at_first_rec_abandon_mk_cat;
create table ssharma_db.at_first_rec_abandon_mk_cat as 
select masterkey,category,count(distinct t_time_info) as items_abandoned
from ssharma_db.at_first_rec_abandon_basket_prod_master_notnull
group by masterkey,category;

drop table if exists ssharma_db.at_first_rec_clicks_mk_cat;
create table ssharma_db.at_first_rec_clicks_mk_cat as 
select masterkey,category,count(distinct t_time_info) as items_browsed
from ssharma_db.at_first_rec_clicks_cat_master_notnull
group by masterkey,category;

drop table if exists ssharma_db.at_second_rec_abandon_mk_cat;
create table ssharma_db.at_second_rec_abandon_mk_cat as 
select masterkey,category,count(distinct t_time_info) as items_abandoned
from ssharma_db.at_second_rec_abandon_basket_prod_master_notnull
group by masterkey,category;

drop table if exists ssharma_db.at_second_rec_clicks_mk_cat;
create table ssharma_db.at_second_rec_clicks_mk_cat as 
select masterkey,category,count(distinct t_time_info) as items_browsed
from ssharma_db.at_second_rec_clicks_cat_master_notnull
group by masterkey,category;

drop table if exists ssharma_db.at_third_rec_abandon_mk_cat;
create table ssharma_db.at_third_rec_abandon_mk_cat as 
select masterkey,category,count(distinct t_time_info) as items_abandoned
from ssharma_db.at_third_rec_abandon_basket_prod_master_notnull
group by masterkey,category;

drop table if exists ssharma_db.at_third_rec_clicks_mk_cat;
create table ssharma_db.at_third_rec_clicks_mk_cat as 
select masterkey,category,count(distinct t_time_info) as items_browsed
from ssharma_db.at_third_rec_clicks_cat_master_notnull
group by masterkey,category;

drop table if exists ssharma_db.at_fourth_rec_abandon_mk_cat;
create table ssharma_db.at_fourth_rec_abandon_mk_cat as 
select masterkey,category,count(distinct t_time_info) as items_abandoned
from ssharma_db.at_fourth_rec_abandon_basket_prod_master_notnull
group by masterkey,category;

drop table if exists ssharma_db.at_fourth_rec_clicks_mk_cat;
create table ssharma_db.at_fourth_rec_clicks_mk_cat as 
select masterkey,category,count(distinct t_time_info) as items_browsed
from ssharma_db.at_fourth_rec_clicks_cat_master_notnull
group by masterkey,category;




drop table if exists ssharma_db.at_first_rec_master;
create table ssharma_db.at_first_rec_master as 
select a.*,b.masterkey 
from ssharma_db.at_first_rec_txn a
left outer join 
(select distinct customerkey,masterkey from crmanalytics.customerresolution) b 
on a.customer_key=b.customerkey;

drop table if exists ssharma_db.at_first_rec_master_notnull;
create table ssharma_db.at_first_rec_master_notnull as 
select * from ssharma_db.at_first_rec_master where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_first_rec_mk_cat;
create table ssharma_db.at_first_rec_mk_cat as 
select masterkey,category,sum(item_qty) as items_purch_first
from ssharma_db.at_first_rec_master_notnull 
group by masterkey,category;


drop table if exists ssharma_db.at_second_rec_master;
create table ssharma_db.at_second_rec_master as 
select a.*,b.masterkey 
from ssharma_db.at_second_rec_txn a
left outer join 
(select distinct customerkey,masterkey from crmanalytics.customerresolution) b 
on a.customer_key=b.customerkey;

drop table if exists ssharma_db.at_second_rec_master_notnull;
create table ssharma_db.at_second_rec_master_notnull as 
select * from ssharma_db.at_second_rec_master where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_second_rec_mk_cat;
create table ssharma_db.at_second_rec_mk_cat as 
select masterkey,category,sum(item_qty) as items_purch_second
from ssharma_db.at_second_rec_master_notnull 
group by masterkey,category;


drop table if exists ssharma_db.at_third_rec_master;
create table ssharma_db.at_third_rec_master as 
select a.*,b.masterkey 
from ssharma_db.at_third_rec_txn a
left outer join 
(select distinct customerkey,masterkey from crmanalytics.customerresolution) b 
on a.customer_key=b.customerkey;

drop table if exists ssharma_db.at_third_rec_master_notnull;
create table ssharma_db.at_third_rec_master_notnull as 
select * from ssharma_db.at_third_rec_master where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_third_rec_mk_cat;
create table ssharma_db.at_third_rec_mk_cat as 
select masterkey,category,sum(item_qty) as items_purch_third
from ssharma_db.at_third_rec_master_notnull 
group by masterkey,category;


drop table if exists ssharma_db.at_fourth_rec_master;
create table ssharma_db.at_fourth_rec_master as 
select a.*,b.masterkey 
from ssharma_db.at_fourth_rec_txn a
left outer join 
(select distinct customerkey,masterkey from crmanalytics.customerresolution) b 
on a.customer_key=b.customerkey;

drop table if exists ssharma_db.at_fourth_rec_master_notnull;
create table ssharma_db.at_fourth_rec_master_notnull as 
select * from ssharma_db.at_fourth_rec_master where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_fourth_rec_mk_cat;
create table ssharma_db.at_fourth_rec_mk_cat as 
select masterkey,category,sum(item_qty) as items_purch_fourth
from ssharma_db.at_fourth_rec_master_notnull 
group by masterkey,category;







drop table if exists ssharma_db.at_merge1;
create table ssharma_db.at_merge1 as 
select distinct (case when a.masterkey is null then b.masterkey else a.masterkey end) as masterkey,
(case when a.category is null then b.category else a.category end) as category,
coalesce(a.items_abandoned,0) as items_abandoned,coalesce(a.items_browsed,0) as items_browsed,
coalesce(a.items_purchased,0) as items_purchased,
coalesce(a.items_purch_first,0) as items_purch_first,
coalesce(b.items_purch_first,0) as items_purch_first_rec
from ssharma_db.at_browser_purchaser_first a 
full outer join 
ssharma_db.at_first_rec_mk_cat b 
on a.masterkey=b.masterkey and a.category=b.category;

drop table if exists ssharma_db.at_merge2;
create table ssharma_db.at_merge2 as 
select distinct (case when a.masterkey is null then b.masterkey else a.masterkey end) as masterkey,
(case when a.category is null then b.category else a.category end) as category,
coalesce(a.items_abandoned,0) as items_abandoned,coalesce(a.items_browsed,0) as items_browsed,
coalesce(a.items_purchased,0) as items_purchased,
coalesce(a.items_purch_first,0) as items_purch_first,
coalesce(a.items_purch_first_rec,0) as items_purch_first_rec,
coalesce(b.items_purch_second,0) as items_purch_second_rec
from ssharma_db.at_merge1 a 
full outer join 
ssharma_db.at_second_rec_mk_cat b 
on a.masterkey=b.masterkey and a.category=b.category;

drop table if exists ssharma_db.at_merge3;
create table ssharma_db.at_merge3 as 
select distinct (case when a.masterkey is null then b.masterkey else a.masterkey end) as masterkey,
(case when a.category is null then b.category else a.category end) as category,
coalesce(a.items_abandoned,0) as items_abandoned,coalesce(a.items_browsed,0) as items_browsed,
coalesce(a.items_purchased,0) as items_purchased,
coalesce(a.items_purch_first,0) as items_purch_first,
coalesce(a.items_purch_first_rec,0) as items_purch_first_rec,
coalesce(a.items_purch_second_rec,0) as items_purch_second_rec,
coalesce(b.items_purch_third,0) as items_purch_third_rec
from ssharma_db.at_merge2 a 
full outer join 
ssharma_db.at_third_rec_mk_cat b 
on a.masterkey=b.masterkey and a.category=b.category;

drop table if exists ssharma_db.at_merge4;
create table ssharma_db.at_merge4 as 
select distinct (case when a.masterkey is null then b.masterkey else a.masterkey end) as masterkey,
(case when a.category is null then b.category else a.category end) as category,
coalesce(a.items_abandoned,0) as items_abandoned,coalesce(a.items_browsed,0) as items_browsed,
coalesce(a.items_purchased,0) as items_purchased,
coalesce(a.items_purch_first,0) as items_purch_first,
coalesce(a.items_purch_first_rec,0) as items_purch_first_rec,
coalesce(a.items_purch_second_rec,0) as items_purch_second_rec,
coalesce(a.items_purch_third_rec,0) as items_purch_third_rec,
coalesce(b.items_purch_fourth,0) as items_purch_fourth_rec
from ssharma_db.at_merge3 a 
full outer join 
ssharma_db.at_fourth_rec_mk_cat b 
on a.masterkey=b.masterkey and a.category=b.category;

drop table if exists ssharma_db.at_merge5;
create table ssharma_db.at_merge5 as 
select distinct (case when a.masterkey is null then b.masterkey else a.masterkey end) as masterkey,
(case when a.category is null then b.category else a.category end) as category,
coalesce(a.items_abandoned,0) as items_abandoned,coalesce(a.items_browsed,0) as items_browsed,
coalesce(a.items_purchased,0) as items_purchased,
coalesce(a.items_purch_first,0) as items_purch_first,
coalesce(a.items_purch_first_rec,0) as items_purch_first_rec,
coalesce(a.items_purch_second_rec,0) as items_purch_second_rec,
coalesce(a.items_purch_third_rec,0) as items_purch_third_rec,
coalesce(a.items_purch_fourth_rec,0) as items_purch_fourth_rec,
coalesce(b.items_abandoned,0) as items_abandoned_first_rec
from ssharma_db.at_merge4 a 
full outer join 
ssharma_db.at_first_rec_abandon_mk_cat b 
on a.masterkey=b.masterkey and a.category=b.category;

drop table if exists ssharma_db.at_merge6;
create table ssharma_db.at_merge6 as 
select distinct (case when a.masterkey is null then b.masterkey else a.masterkey end) as masterkey,
(case when a.category is null then b.category else a.category end) as category,
coalesce(a.items_abandoned,0) as items_abandoned,coalesce(a.items_browsed,0) as items_browsed,
coalesce(a.items_purchased,0) as items_purchased,
coalesce(a.items_purch_first,0) as items_purch_first,
coalesce(a.items_purch_first_rec,0) as items_purch_first_rec,
coalesce(a.items_purch_second_rec,0) as items_purch_second_rec,
coalesce(a.items_purch_third_rec,0) as items_purch_third_rec,
coalesce(a.items_purch_fourth_rec,0) as items_purch_fourth_rec,
coalesce(a.items_abandoned_first_rec,0) as items_abandoned_first_rec,
coalesce(b.items_abandoned,0) as items_abandoned_second_rec
from ssharma_db.at_merge5 a 
full outer join 
ssharma_db.at_second_rec_abandon_mk_cat b 
on a.masterkey=b.masterkey and a.category=b.category;

drop table if exists ssharma_db.at_merge7;
create table ssharma_db.at_merge7 as 
select distinct (case when a.masterkey is null then b.masterkey else a.masterkey end) as masterkey,
(case when a.category is null then b.category else a.category end) as category,
coalesce(a.items_abandoned,0) as items_abandoned,coalesce(a.items_browsed,0) as items_browsed,
coalesce(a.items_purchased,0) as items_purchased,
coalesce(a.items_purch_first,0) as items_purch_first,
coalesce(a.items_purch_first_rec,0) as items_purch_first_rec,
coalesce(a.items_purch_second_rec,0) as items_purch_second_rec,
coalesce(a.items_purch_third_rec,0) as items_purch_third_rec,
coalesce(a.items_purch_fourth_rec,0) as items_purch_fourth_rec,
coalesce(a.items_abandoned_first_rec,0) as items_abandoned_first_rec,
coalesce(a.items_abandoned_second_rec,0) as items_abandoned_second_rec,
coalesce(b.items_abandoned,0) as items_abandoned_third_rec
from ssharma_db.at_merge6 a 
full outer join 
ssharma_db.at_third_rec_abandon_mk_cat b 
on a.masterkey=b.masterkey and a.category=b.category;

drop table if exists ssharma_db.at_merge8;
create table ssharma_db.at_merge8 as 
select distinct (case when a.masterkey is null then b.masterkey else a.masterkey end) as masterkey,
(case when a.category is null then b.category else a.category end) as category,
coalesce(a.items_abandoned,0) as items_abandoned,coalesce(a.items_browsed,0) as items_browsed,
coalesce(a.items_purchased,0) as items_purchased,
coalesce(a.items_purch_first,0) as items_purch_first,
coalesce(a.items_purch_first_rec,0) as items_purch_first_rec,
coalesce(a.items_purch_second_rec,0) as items_purch_second_rec,
coalesce(a.items_purch_third_rec,0) as items_purch_third_rec,
coalesce(a.items_purch_fourth_rec,0) as items_purch_fourth_rec,
coalesce(a.items_abandoned_first_rec,0) as items_abandoned_first_rec,
coalesce(a.items_abandoned_second_rec,0) as items_abandoned_second_rec,
coalesce(a.items_abandoned_third_rec,0) as items_abandoned_third_rec,
coalesce(b.items_abandoned,0) as items_abandoned_fourth_rec
from ssharma_db.at_merge7 a 
full outer join 
ssharma_db.at_fourth_rec_abandon_mk_cat b 
on a.masterkey=b.masterkey and a.category=b.category;




drop table if exists ssharma_db.at_merge9;
create table ssharma_db.at_merge9 as 
select distinct (case when a.masterkey is null then b.masterkey else a.masterkey end) as masterkey,
(case when a.category is null then b.category else a.category end) as category,
coalesce(a.items_abandoned,0) as items_abandoned,coalesce(a.items_browsed,0) as items_browsed,
coalesce(a.items_purchased,0) as items_purchased,
coalesce(a.items_purch_first,0) as items_purch_first,
coalesce(a.items_purch_first_rec,0) as items_purch_first_rec,
coalesce(a.items_purch_second_rec,0) as items_purch_second_rec,
coalesce(a.items_purch_third_rec,0) as items_purch_third_rec,
coalesce(a.items_purch_fourth_rec,0) as items_purch_fourth_rec,
coalesce(a.items_abandoned_first_rec,0) as items_abandoned_first_rec,
coalesce(a.items_abandoned_second_rec,0) as items_abandoned_second_rec,
coalesce(a.items_abandoned_third_rec,0) as items_abandoned_third_rec,
coalesce(a.items_abandoned_fourth_rec,0) as items_abandoned_fourth_rec,
coalesce(b.items_browsed,0) as items_browsed_first_rec
from ssharma_db.at_merge8 a 
full outer join 
ssharma_db.at_first_rec_clicks_mk_cat b 
on a.masterkey=b.masterkey and a.category=b.category;

drop table if exists ssharma_db.at_merge10;
create table ssharma_db.at_merge10 as 
select distinct (case when a.masterkey is null then b.masterkey else a.masterkey end) as masterkey,
(case when a.category is null then b.category else a.category end) as category,
coalesce(a.items_abandoned,0) as items_abandoned,coalesce(a.items_browsed,0) as items_browsed,
coalesce(a.items_purchased,0) as items_purchased,
coalesce(a.items_purch_first,0) as items_purch_first,
coalesce(a.items_purch_first_rec,0) as items_purch_first_rec,
coalesce(a.items_purch_second_rec,0) as items_purch_second_rec,
coalesce(a.items_purch_third_rec,0) as items_purch_third_rec,
coalesce(a.items_purch_fourth_rec,0) as items_purch_fourth_rec,
coalesce(a.items_abandoned_first_rec,0) as items_abandoned_first_rec,
coalesce(a.items_abandoned_second_rec,0) as items_abandoned_second_rec,
coalesce(a.items_abandoned_third_rec,0) as items_abandoned_third_rec,
coalesce(a.items_abandoned_fourth_rec,0) as items_abandoned_fourth_rec,
coalesce(a.items_browsed_first_rec,0) as items_browsed_first_rec,
coalesce(b.items_browsed,0) as items_browsed_second_rec
from ssharma_db.at_merge9 a 
full outer join 
ssharma_db.at_second_rec_clicks_mk_cat b 
on a.masterkey=b.masterkey and a.category=b.category;

drop table if exists ssharma_db.at_merge11;
create table ssharma_db.at_merge11 as 
select distinct (case when a.masterkey is null then b.masterkey else a.masterkey end) as masterkey,
(case when a.category is null then b.category else a.category end) as category,
coalesce(a.items_abandoned,0) as items_abandoned,coalesce(a.items_browsed,0) as items_browsed,
coalesce(a.items_purchased,0) as items_purchased,
coalesce(a.items_purch_first,0) as items_purch_first,
coalesce(a.items_purch_first_rec,0) as items_purch_first_rec,
coalesce(a.items_purch_second_rec,0) as items_purch_second_rec,
coalesce(a.items_purch_third_rec,0) as items_purch_third_rec,
coalesce(a.items_purch_fourth_rec,0) as items_purch_fourth_rec,
coalesce(a.items_abandoned_first_rec,0) as items_abandoned_first_rec,
coalesce(a.items_abandoned_second_rec,0) as items_abandoned_second_rec,
coalesce(a.items_abandoned_third_rec,0) as items_abandoned_third_rec,
coalesce(a.items_abandoned_fourth_rec,0) as items_abandoned_fourth_rec,
coalesce(a.items_browsed_first_rec,0) as items_browsed_first_rec,
coalesce(a.items_browsed_second_rec,0) as items_browsed_second_rec,
coalesce(b.items_browsed,0) as items_browsed_third_rec
from ssharma_db.at_merge10 a 
full outer join 
ssharma_db.at_third_rec_clicks_mk_cat b 
on a.masterkey=b.masterkey and a.category=b.category;

drop table if exists ssharma_db.at_merge12;
create table ssharma_db.at_merge12 as 
select distinct (case when a.masterkey is null then b.masterkey else a.masterkey end) as masterkey,
(case when a.category is null then b.category else a.category end) as category,
coalesce(a.items_abandoned,0) as items_abandoned,coalesce(a.items_browsed,0) as items_browsed,
coalesce(a.items_purchased,0) as items_purchased,
coalesce(a.items_purch_first,0) as items_purch_first,
coalesce(a.items_purch_first_rec,0) as items_purch_first_rec,
coalesce(a.items_purch_second_rec,0) as items_purch_second_rec,
coalesce(a.items_purch_third_rec,0) as items_purch_third_rec,
coalesce(a.items_purch_fourth_rec,0) as items_purch_fourth_rec,
coalesce(a.items_abandoned_first_rec,0) as items_abandoned_first_rec,
coalesce(a.items_abandoned_second_rec,0) as items_abandoned_second_rec,
coalesce(a.items_abandoned_third_rec,0) as items_abandoned_third_rec,
coalesce(a.items_abandoned_fourth_rec,0) as items_abandoned_fourth_rec,
coalesce(a.items_browsed_first_rec,0) as items_browsed_first_rec,
coalesce(a.items_browsed_second_rec,0) as items_browsed_second_rec,
coalesce(a.items_browsed_third_rec,0) as items_browsed_third_rec,
coalesce(b.items_browsed,0) as items_browsed_fourth_rec
from ssharma_db.at_merge11 a 
full outer join 
ssharma_db.at_fourth_rec_clicks_mk_cat b 
on a.masterkey=b.masterkey and a.category=b.category;

drop table if exists ssharma_db.at_sum_all;
create table ssharma_db.at_sum_all as 
select masterkey,
sum(a.items_abandoned) as sum_items_abandoned,sum(a.items_browsed) as sum_items_browsed,
sum(a.items_purchased) as sum_items_purchased,
sum(a.items_purch_first) as sum_items_purch_first,
sum(a.items_purch_first_rec) as sum_items_purch_first_rec,
sum(a.items_purch_second_rec) as sum_items_purch_second_rec,
sum(a.items_purch_third_rec) as sum_items_purch_third_rec,
sum(a.items_purch_fourth_rec) as sum_items_purch_fourth_rec,
sum(a.items_abandoned_first_rec) as sum_items_abandoned_first_rec,
sum(a.items_abandoned_second_rec) as sum_items_abandoned_second_rec,
sum(a.items_abandoned_third_rec) as sum_items_abandoned_third_rec,
sum(a.items_abandoned_fourth_rec) as sum_items_abandoned_fourth_rec,
sum(a.items_browsed_first_rec) as sum_items_browsed_first_rec,
sum(a.items_browsed_second_rec) as sum_items_browsed_second_rec,
sum(a.items_browsed_third_rec) as sum_items_browsed_third_rec,
sum(a.items_browsed) as sum_items_browsed_fourth_rec
from ssharma_db.at_merge12 a 
group by masterkey;


drop table if exists ssharma_db.at_normal_all;
create table ssharma_db.at_normal_all as 
select a.masterkey,a.category,
(case when b.sum_items_abandoned >0 then a.items_abandoned/b.sum_items_abandoned else 0.0 end) as nrml_items_abandoned,
(case when b.sum_items_browsed >0 then a.items_browsed/b.sum_items_browsed else 0.0 end) as nrml_items_browsed,
(case when b.sum_items_purchased >0 then a.items_purchased/b.sum_items_purchased else 0.0 end) as nrml_items_purchased,
(case when b.sum_items_purch_first >0 then a.items_purch_first/b.sum_items_purch_first else 0.0 end) as nrml_items_purch_first,
(case when b.sum_items_purch_first_rec >0 then a.items_purch_first_rec/b.sum_items_purch_first_rec else 0.0 end) as nrml_items_purch_first_rec,
(case when b.sum_items_purch_second_rec >0 then a.items_purch_second_rec/b.sum_items_purch_second_rec else 0.0 end) as nrml_items_purch_second_rec,
(case when b.sum_items_purch_third_rec >0 then a.items_purch_third_rec/b.sum_items_purch_third_rec else 0.0 end) as nrml_items_purch_third_rec,
(case when b.sum_items_purch_fourth_rec >0 then a.items_purch_fourth_rec/b.sum_items_purch_fourth_rec else 0.0 end) as nrml_items_purch_fourth_rec,
(case when b.sum_items_abandoned_first_rec >0 then a.items_abandoned_first_rec/b.sum_items_abandoned_first_rec else 0.0 end) as nrml_items_abandoned_first_rec,
(case when b.sum_items_abandoned_second_rec >0 then a.items_abandoned_second_rec/b.sum_items_abandoned_second_rec else 0.0 end) as nrml_items_abandoned_second_rec,
(case when b.sum_items_abandoned_third_rec >0 then a.items_abandoned_third_rec/b.sum_items_abandoned_third_rec else 0.0 end) as nrml_items_abandoned_third_rec,
(case when b.sum_items_abandoned_fourth_rec >0 then a.items_abandoned_fourth_rec/b.sum_items_abandoned_fourth_rec else 0.0 end) as nrml_items_abandoned_fourth_rec,
(case when b.sum_items_browsed_first_rec >0 then a.items_browsed_first_rec/b.sum_items_browsed_first_rec else 0.0 end) as nrml_items_browsed_first_rec,
(case when b.sum_items_browsed_second_rec >0 then a.items_browsed_second_rec/b.sum_items_browsed_second_rec else 0.0 end) as nrml_items_browsed_second_rec,
(case when b.sum_items_browsed_third_rec >0 then a.items_browsed_third_rec/b.sum_items_browsed_third_rec else 0.0 end) as nrml_items_browsed_third_rec,
(case when b.sum_items_browsed_fourth_rec >0 then a.items_browsed_fourth_rec/b.sum_items_browsed_fourth_rec else 0.0 end) as nrml_items_browsed_fourth_rec
from ssharma_db.at_merge12 a 
left outer join 
ssharma_db.at_sum_all b 
on a.masterkey=b.masterkey;


drop table if exists ssharma_db.at_target_all;
create table ssharma_db.at_target_all as 
select distinct a.masterkey,a.category,a.nrml_items_abandoned,a.nrml_items_browsed,a.nrml_items_purchased,
a.nrml_items_purch_first,a.nrml_items_purch_first_rec,a.nrml_items_purch_second_rec,a.nrml_items_purch_third_rec,a.nrml_items_purch_fourth_rec,
a.nrml_items_abandoned_first_rec,a.nrml_items_abandoned_second_rec,a.nrml_items_abandoned_third_rec,a.nrml_items_abandoned_fourth_rec,
a.nrml_items_browsed_first_rec,a.nrml_items_browsed_second_rec,a.nrml_items_browsed_third_rec,a.nrml_items_browsed_fourth_rec,
coalesce(b.items_purch_pred,0) as items_purch_pred
from ssharma_db.at_normal_all a 
left outer join ssharma_db.at_pred_mk_cat b 
on a.masterkey=b.masterkey and a.category=b.category;


drop table if exists ssharma_db.at_lastmnth_clicks;
create table ssharma_db.at_lastmnth_clicks as 
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
from omniture.hit_data_t 
where (instr(pagename,'at:browse')>0 or instr(pagename,'mobile:at')>0) and (date_time) between ('2015-12-01') and ('2015-12-31');


drop table if exists ssharma_db.at_lastmnth_purch;
create table ssharma_db.at_lastmnth_purch as 
select customer_key,item_qty,product_key
from mds_new.ods_orderline_t 
where brand='AT' and country='US' 
and unix_timestamp(transaction_date,'yyyy-MM-dd') between unix_timestamp('2015-12-01','yyyy-MM-dd') and unix_timestamp('2015-12-31','yyyy-MM-dd')
and order_status in ('R','O') and item_qty>0 and sales_amt>0;

drop table if exists ssharma_db.at_lastmnth_txn;
create table ssharma_db.at_lastmnth_txn as
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
from ssharma_db.at_lastmnth_purch a 
inner join 
mds_new.ods_product_t b 
on a.product_key=b.product_key;

drop table if exists ssharma_db.at_lastmnth_master;
create table ssharma_db.at_lastmnth_master as 
select a.*,b.masterkey 
from ssharma_db.at_lastmnth_txn a
left outer join 
(select distinct customerkey,masterkey from crmanalytics.customerresolution) b 
on a.customer_key=b.customerkey;

drop table if exists ssharma_db.at_lastmnth_master_notnull;
create table ssharma_db.at_lastmnth_master_notnull as 
select * from ssharma_db.at_lastmnth_master where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_lastmnth_mk_cat;
create table ssharma_db.at_lastmnth_mk_cat as 
select masterkey,category,sum(item_qty) as items_purch_lastmnth
from ssharma_db.at_lastmnth_master_notnull 
group by masterkey,category;

drop table if exists ssharma_db.at_lastmnth_sum;
create table ssharma_db.at_lastmnth_sum as 
select  masterkey,sum(items_purch_lastmnth) as sum_items_purch_lastmnth
from ssharma_db.at_lastmnth_mk_cat
group by masterkey;

drop table if exists ssharma_db.at_lastmnth_nrml;
create table ssharma_db.at_lastmnth_nrml as
select a.masterkey,a.category,
(case when b.sum_items_purch_lastmnth>0 then a.items_purch_lastmnth/b.sum_items_purch_lastmnth else 0.0 end ) as nrml_items_purch_lastmnth
from ssharma_db.at_lastmnth_mk_cat a 
left outer join ssharma_db.at_lastmnth_sum b 
on a.masterkey=b.masterkey;




drop table if exists ssharma_db.at_prd_lastmnth_clicks;
create table ssharma_db.at_prd_lastmnth_clicks as 
select *,substr(prop1,1,6) as style_cd from ssharma_db.at_lastmnth_clicks
where prop33='product';

drop table if exists ssharma_db.at_prd_lastmnth_clicks_desc;
create table ssharma_db.at_prd_lastmnth_clicks_desc as 
select a.*,b.mdse_div_desc,b.mdse_dept_desc,b.mdse_class_desc
from ssharma_db.at_prd_lastmnth_clicks a 
inner join (select distinct style_cd,mdse_div_desc,mdse_dept_desc,mdse_class_desc from mds_new.ods_product_t where mdse_div_desc is not null and mdse_dept_desc is not null and mdse_class_desc is not null and mdse_corp_desc='ATH DIR') b 
on a.style_cd=b.style_cd
;

drop table if exists ssharma_db.at_lastmnth_clicks_cat;
create table ssharma_db.at_lastmnth_clicks_cat as 
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
from ssharma_db.at_prd_lastmnth_clicks_desc;




drop table if exists ssharma_db.at_lastmnth_bagadd;
create table ssharma_db.at_lastmnth_bagadd as 
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
from ssharma_db.at_lastmnth_clicks
where  prop33='inlineBagAdd';

drop table if exists ssharma_db.at_lastmnth_purchasenull;
create table ssharma_db.at_lastmnth_purchasenull as 
select unk_shop_id,visit_num
from ssharma_db.at_lastmnth_clicks
where unk_shop_id != "" and (PurchaseID IS NULL or PurchaseID=='' or PurchaseID==' ');

drop table if exists ssharma_db.at_lastmnth_abandon_basket;
create table ssharma_db.at_lastmnth_abandon_basket  as 
select a.* 
from ssharma_db.at_lastmnth_bagadd a 
inner join  ssharma_db.at_lastmnth_purchasenull b 
on a.unk_shop_id=b.unk_shop_id and a.visit_num=b.visit_num;

drop table if exists ssharma_db.at_lastmnth_abandon_basket_prod;
create table ssharma_db.at_lastmnth_abandon_basket_prod as 
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
from ssharma_db.at_lastmnth_abandon_basket a 
inner join (select distinct style_cd,mdse_div_desc,mdse_dept_desc,mdse_class_desc from mds_new.ods_product_t where mdse_div_desc is not null and mdse_dept_desc is not null and mdse_class_desc is not null and mdse_corp_desc='ATH DIR') b 
on substr(a.prop1,1,6)=b.style_cd
;


drop table if exists ssharma_db.at_lastmnth_clicks_cat_master;
create table ssharma_db.at_lastmnth_clicks_cat_master as 
select a.*,b.masterkey
from ssharma_db.at_lastmnth_clicks_cat a 
left outer join 
(select distinct unknownshopperid,masterkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;

drop table if exists ssharma_db.at_lastmnth_abandon_basket_prod_master;
create table ssharma_db.at_lastmnth_abandon_basket_prod_master as 
select a.*,b.masterkey
from  ssharma_db.at_lastmnth_abandon_basket_prod a 
left outer join 
(select distinct unknownshopperid,masterkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;

drop table if exists ssharma_db.at_lastmnth_abandon_basket_prod_master_notnull;
create table ssharma_db.at_lastmnth_abandon_basket_prod_master_notnull as
select * from  ssharma_db.at_lastmnth_abandon_basket_prod_master where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_lastmnth_clicks_cat_master_notnull;
create table ssharma_db.at_lastmnth_clicks_cat_master_notnull as
select * from  ssharma_db.at_lastmnth_clicks_cat_master where masterkey is not null and category <> 'N/A';

drop table if exists ssharma_db.at_lastmnth_abandon_mk_cat;
create table ssharma_db.at_lastmnth_abandon_mk_cat as 
select masterkey,category,count(distinct t_time_info) as items_abandoned_lastmnth
from ssharma_db.at_lastmnth_abandon_basket_prod_master_notnull
group by masterkey,category;

drop table if exists ssharma_db.at_lastmnth_clicks_mk_cat;
create table ssharma_db.at_lastmnth_clicks_mk_cat as 
select masterkey,category,count(distinct t_time_info) as items_browsed_lastmnth
from ssharma_db.at_lastmnth_clicks_cat_master_notnull
group by masterkey,category;

drop table if exists ssharma_db.at_lastmnth_abandon_sum;
create table ssharma_db.at_lastmnth_abandon_sum as 
select  masterkey,sum(items_abandoned_lastmnth) as sum_items_abandoned_lastmnth
from ssharma_db.at_lastmnth_abandon_mk_cat
group by masterkey;

drop table if exists ssharma_db.at_lastmnth_clicks_sum;
create table ssharma_db.at_lastmnth_clicks_sum as 
select  masterkey,sum(items_browsed_lastmnth) as sum_items_browsed_lastmnth
from ssharma_db.at_lastmnth_clicks_mk_cat
group by masterkey;

drop table if exists ssharma_db.at_lastmnth_abandon_nrml;
create table ssharma_db.at_lastmnth_abandon_nrml as
select a.masterkey,a.category,
(case when b.sum_items_abandoned_lastmnth>0 then a.items_abandoned_lastmnth/b.sum_items_abandoned_lastmnth else 0.0 end ) as nrml_items_abandoned_lastmnth
from ssharma_db.at_lastmnth_abandon_mk_cat a 
left outer join ssharma_db.at_lastmnth_abandon_sum b 
on a.masterkey=b.masterkey;

drop table if exists ssharma_db.at_lastmnth_clicks_nrml;
create table ssharma_db.at_lastmnth_clicks_nrml as
select a.masterkey,a.category,
(case when b.sum_items_browsed_lastmnth>0 then a.items_browsed_lastmnth/b.sum_items_browsed_lastmnth else 0.0 end) as nrml_items_browsed_lastmnth
from ssharma_db.at_lastmnth_clicks_mk_cat a 
left outer join ssharma_db.at_lastmnth_clicks_sum b 
on a.masterkey=b.masterkey;




drop table if exists ssharma_db.at_abandon_unixtime;
create table ssharma_db.at_abandon_unixtime as 
select a.*,b.date_time
from ssharma_db.at_abandon_basket_prod_master_notnull a
inner join 
(select * from omniture.hit_data_t where date_time between '2015-10-01' and '2015-12-31') b 
on a.unk_shop_id=b.unk_shop_id and             
a.visit_num=b.visit_num  and        
a.visid_high=b.visid_high  and          
a.visid_low=b.visid_low and               
a.pagename=b.pagename and             
a.hier1=b.hier1 and                   
a.prop1=b.prop1 and               
a.purchaseid=b.purchaseid and         
a.prop33=b.prop33 and                 
a.country=b.country  and               
a.t_time_info=b.t_time_info;

drop table if exists ssharma_db.at_abandon_maxdt;
create table ssharma_db.at_abandon_maxdt as 
select masterkey,category,datediff('2016-01-01',max(date_time))/90 as recency_abandon
from ssharma_db.at_abandon_unixtime
group by masterkey,category;

drop table if exists ssharma_db.at_clicks_unixtime;
create table ssharma_db.at_clicks_unixtime as 
select a.*,b.date_time
from ssharma_db.at_clicks_cat_master_notnull a
inner join 
(select * from omniture.hit_data_t where date_time between '2015-10-01' and '2015-12-31') b 
on a.unk_shop_id=b.unk_shop_id and             
a.visit_num=b.visit_num  and        
a.visid_high=b.visid_high  and          
a.visid_low=b.visid_low and               
a.pagename=b.pagename and             
a.hier1=b.hier1 and                   
a.prop1=b.prop1 and               
a.purchaseid=b.purchaseid and         
a.prop33=b.prop33 and                 
a.country=b.country  and               
a.t_time_info=b.t_time_info;

drop table if exists ssharma_db.at_clicks_maxdt;
create table ssharma_db.at_clicks_maxdt as 
select masterkey,category,datediff('2016-01-01',max(date_time))/90 as recency_click
from ssharma_db.at_clicks_unixtime
group by masterkey,category;

drop table if exists ssharma_db.at_sugg_unixtime;
create table ssharma_db.at_sugg_unixtime as 
select *,to_date(from_unixtime(unix_timestamp(substr(transaction_date,1,9),'ddMMMyyyy'))) as unixdt
from ssharma_db.at_sugg_master_notnull;

drop table if exists ssharma_db.at_sugg_maxdt;
create table ssharma_db.at_sugg_maxdt as 
select masterkey,category,datediff('2016-01-01',max(unixdt))/365 as recency_purch
from ssharma_db.at_sugg_unixtime
group by masterkey,category;



drop table if exists ssharma_db.at_lastmnth_rec;
create table ssharma_db.at_lastmnth_rec as 
select a.masterkey,              
a.category,
a.nrml_items_abandoned,    
a.nrml_items_browsed,      
a.nrml_items_purchased,    
a.nrml_items_purch_first, 
a.items_purch_pred,
coalesce(b.nrml_items_purch_lastmnth,0) as nrml_items_purch_lastmnth,
coalesce(c.nrml_items_abandoned_lastmnth,0) as nrml_items_abandoned_lastmnth,
coalesce(d.nrml_items_browsed_lastmnth,0) as nrml_items_browsed_lastmnth,
coalesce(e.recency_purch,1) as recency_purch,
coalesce(f.recency_abandon,1) as recency_abandon,
coalesce(g.recency_click,1) as recency_click
from 
ssharma_db.at_normal_first a
left outer join 
ssharma_db.at_lastmnth_nrml b 
on a.masterkey=b.masterkey and a.category=b.category 
left outer join 
ssharma_db.at_lastmnth_abandon_nrml c 
on a.masterkey=c.masterkey and a.category=c.category 
left outer join
ssharma_db.at_lastmnth_clicks_nrml d
on a.masterkey=d.masterkey and a.category=d.category 
left outer join
ssharma_db.at_sugg_maxdt e
on a.masterkey=e.masterkey and a.category=e.category 
left outer join
ssharma_db.at_abandon_maxdt f
on a.masterkey=f.masterkey and a.category=f.category 
left outer join
ssharma_db.at_clicks_maxdt g 
on a.masterkey=g.masterkey and a.category=g.category; 