drop table if exists ssharma_db.at_lastyr;
create table ssharma_db.at_lastyr as 
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag
from
(select distinct customer_key,transaction_num,line_num,sales_amt,discount_amt,item_qty,product_key,transaction_date,on_sale_flag from  mds_new.ods_orderline_t 
where brand='AT' and country='US' and item_qty > 0 and sales_amt>0 and transaction_date between '2016-01-01' and '2016-12-31' and order_status in ('R','O')) a ; 

drop table if exists ssharma_db.at_lastyr_disc;
create table ssharma_db.at_lastyr_disc as
select a.customer_key, 
a.transaction_num,
a.line_num,
sum(b.discount_amt) as discount_amt
from (select * from ssharma_db.at_lastyr where transaction_date is NOT NULL) a left outer join mds_new.ods_orderline_discounts_t b
on (a.transaction_num=b.transaction_num
and a.line_num=b.line_num)
group by a.customer_key, a.transaction_num, a.line_num;

drop table if exists ssharma_db.at_lastyr_data;
create table ssharma_db.at_lastyr_data as
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag,
coalesce(b.discount_amt,cast('0.0' as double)) as discount_amt
from (select * from ssharma_db.at_lastyr where transaction_date is NOT NULL) a left outer join ssharma_db.at_lastyr_disc b
on (a.transaction_num=b.transaction_num
and a.line_num=b.line_num
and a.customer_key=b.customer_key);

drop table if exists ssharma_db.at_agg;
create table ssharma_db.at_agg as 
select customer_key,sum(sales_amt+discount_amt) as net_sales,sum(abs(coalesce(discount_amt,0)))/sum(sales_amt) as disc_p,
sum(item_qty) as items_purch,
count(distinct transaction_num) as num_txn,
sum(sales_amt+discount_amt)/count(distinct transaction_num) as avg_ord_sz,
sum(sales_amt+discount_amt)/sum(item_qty) as avg_unt_rtl,
sum(item_qty)/count(distinct transaction_num) as unt_per_txn,
sum(item_qty*(case when on_sale_flag='Y' then 1 else 0 end)) as on_sale_items,
DATEDIFF('2017-01-01',max(transaction_date)) as recency
from ssharma_db.at_lastyr_data
group by customer_key;

drop table if exists ssharma_db.at_prod;
create table ssharma_db.at_prod as 
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag,b.season_desc,
b.mdse_div_desc,b.mdse_dept_desc,b.mdse_class_desc,
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
from 
ssharma_db.at_lastyr a 
inner join 
mds_new.ods_product_t b 
on a.product_key=b.product_key;

drop table if exists ssharma_db.at_cat;
create table ssharma_db.at_cat as 
select customer_key,count(distinct category) as num_cat 
from ssharma_db.at_prod 
group by customer_key;

drop table if exists ssharma_db.at_browse;
create table ssharma_db.at_browse as 
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
UNIX_TIMESTAMP('2016-10-01', 'yyyy-MM-dd') AND 
UNIX_TIMESTAMP('2016-12-31', 'yyyy-MM-dd');

drop table if exists ssharma_db.at_clicks;
create table ssharma_db.at_clicks as 
select *,substr(prop1,1,6) as style_cd from ssharma_db.at_browse
where prop33='product';

drop table if exists ssharma_db.at_clicks_desc_total;
create table ssharma_db.at_clicks_desc_total as 
select a.*,b.season_desc
from ssharma_db.at_clicks a 
inner join (select distinct style_cd,season_desc from mds_new.ods_product_t where  mdse_corp_desc='ATH DIR') b 
on a.style_cd=b.style_cd
;

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
from ssharma_db.at_browse
where  prop33='inlineBagAdd';

drop table if exists ssharma_db.at_purchasenull;
create table ssharma_db.at_purchasenull as 
select unk_shop_id,visit_num
from ssharma_db.at_browse
where unk_shop_id != "" and (PurchaseID IS NULL or PurchaseID=='' or PurchaseID==' ');

drop table if exists ssharma_db.at_abandon_basket;
create table ssharma_db.at_abandon_basket  as 
select a.* 
from ssharma_db.at_bagadd a 
inner join  ssharma_db.at_purchasenull b 
on a.unk_shop_id=b.unk_shop_id and a.visit_num=b.visit_num;

drop table if exists ssharma_db.at_abandon_basket_prod_total;
create table ssharma_db.at_abandon_basket_prod_total as 
select a.*,b.season_desc
from ssharma_db.at_abandon_basket a 
inner join (select distinct style_cd,season_desc from  mds_new.ods_product_t where mdse_corp_desc='ATH DIR') b 
on substr(a.prop1,1,6)=b.style_cd;

drop table if exists ssharma_db.at_clicks_cat_master_total;
create table ssharma_db.at_clicks_cat_master_total as 
select a.*,b.customerkey
from ssharma_db.at_clicks_desc_total a 
left outer join 
(select distinct unknownshopperid,customerkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;

drop table if exists ssharma_db.at_abandon_basket_prod_master_total;
create table ssharma_db.at_abandon_basket_prod_master_total as 
select a.*,b.customerkey
from  ssharma_db.at_abandon_basket_prod_total a 
left outer join 
(select distinct unknownshopperid,customerkey from crmanalytics.customerresolution) b 
on a.unk_shop_id=b.unknownshopperid;

drop table if exists ssharma_db.at_abandon_basket_prod_master_notnull_total;
create table ssharma_db.at_abandon_basket_prod_master_notnull_total as
select * from  ssharma_db.at_abandon_basket_prod_master_total where customerkey is not null ;

drop table if exists ssharma_db.at_clicks_cat_master_notnull_total;
create table ssharma_db.at_clicks_cat_master_notnull_total as
select * from  ssharma_db.at_clicks_cat_master_total where customerkey is not null ;

drop table if exists ssharma_db.at_abandon_mk_cat_total;
create table ssharma_db.at_abandon_mk_cat_total as 
select customerkey,count(distinct t_time_info) as items_abandoned
from ssharma_db.at_abandon_basket_prod_master_notnull_total
group by customerkey;

drop table if exists ssharma_db.at_clicks_mk_cat_total;
create table ssharma_db.at_clicks_mk_cat_total as 
select customerkey,count(distinct t_time_info) as items_browsed
from ssharma_db.at_clicks_cat_master_notnull_total
group by customerkey;

drop table if exists ssharma_db.at_sameprd;
create table ssharma_db.at_sameprd as 
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag
from
(select distinct customer_key,transaction_num,line_num,sales_amt,discount_amt,item_qty,product_key,transaction_date,on_sale_flag from  mds_new.ods_orderline_t 
where brand='AT' and country='US' and item_qty > 0 and sales_amt>0 and transaction_date between '2016-01-01' and '2016-03-31' and order_status in ('R','O')) a;

drop table if exists ssharma_db.at_sameprd_disc;
create table ssharma_db.at_sameprd_disc as 
select a.customer_key, 
a.transaction_num,
a.line_num,
sum(b.discount_amt) as discount_amt
from (select * from ssharma_db.at_sameprd where transaction_date is NOT NULL) a left outer join mds_new.ods_orderline_discounts_t b
on (a.transaction_num=b.transaction_num
and a.line_num=b.line_num)
group by a.customer_key, a.transaction_num, a.line_num;

drop table if exists ssharma_db.at_sameprd_data;
create table ssharma_db.at_sameprd_data as
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag,
coalesce(b.discount_amt,cast('0.0' as double)) as discount_amt
from (select * from ssharma_db.at_sameprd where transaction_date is NOT NULL) a left outer join ssharma_db.at_sameprd_disc b
on (a.transaction_num=b.transaction_num
and a.line_num=b.line_num
and a.customer_key=b.customer_key);

drop table if exists ssharma_db.at_agg_sameprd;
create table ssharma_db.at_agg_sameprd as 
select customer_key,sum(sales_amt+discount_amt) as net_sales,sum(abs(coalesce(discount_amt,0)))/sum(sales_amt) as disc_p,
sum(item_qty) as items_purch,
count(distinct transaction_num) as num_txn,
sum(sales_amt+discount_amt)/count(distinct transaction_num) as avg_ord_sz,
sum(sales_amt+discount_amt)/sum(item_qty) as avg_unt_rtl,
sum(item_qty)/count(distinct transaction_num) as unt_per_txn,
sum(item_qty*(case when on_sale_flag='Y' then 1 else 0 end)) as on_sale_items
from ssharma_db.at_sameprd_data
group by customer_key;

drop table if exists ssharma_db.at_prod_sameprd;
create table ssharma_db.at_prod_sameprd as 
select distinct a.customer_key,a.transaction_num,a.line_num,a.sales_amt,a.item_qty,a.product_key,a.transaction_date,a.on_sale_flag,b.season_desc,
b.mdse_div_desc,b.mdse_dept_desc,b.mdse_class_desc,
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
from 
ssharma_db.at_sameprd a 
inner join 
mds_new.ods_product_t b 
on a.product_key=b.product_key;

drop table if exists ssharma_db.at_cat_sameprd;
create table ssharma_db.at_cat_sameprd as 
select customer_key,count(distinct category) as num_cat 
from ssharma_db.at_prod_sameprd 
group by customer_key;

drop table if exists ssharma_db.at_summary;
create table ssharma_db.at_summary as 
select 
(case when c.customer_key is not null then c.customer_key
when f.customerkey is not null then f.customerkey
when h.customerkey is not null then h.customerkey 
when l.customer_key is not null then l.customer_key 
when e.customer_key is not null then e.customer_key 
when d.customer_key is not null then d.customer_key end) as customer_key,
coalesce(c.net_sales,0) as net_sales,coalesce(c.disc_p,0) as disc_p,
coalesce(c.avg_unt_rtl,0) as avg_unt_rtl,
coalesce(c.unt_per_txn,0) as unt_per_txn,
coalesce(c.on_sale_items,0) as on_sale_items,
coalesce(c.recency,365) as recency,
coalesce(f.items_browsed,0) as items_browsed,
coalesce(h.items_abandoned,0) as items_abandoned,
coalesce(l.num_cat,0) as num_cat,
coalesce(e.net_sales,0) as net_sales_sameprd,coalesce(e.disc_p,0) as disc_p_sameprd,
coalesce(e.num_txn,0) as num_txn_sameprd,
coalesce(e.avg_unt_rtl,0) as avg_unt_rtl_sameprd,
coalesce(e.unt_per_txn,0) as unt_per_txn_sameprd,
coalesce(e.on_sale_items,0) as on_sale_items_sameprd,
'at' as season
from 
ssharma_db.at_agg  c  
full outer join 
ssharma_db.at_clicks_mk_cat_total  f 
on c.customer_key=f.customerkey 
full outer join 
ssharma_db.at_abandon_mk_cat_total  h 
on c.customer_key=h.customerkey
full outer join 
ssharma_db.at_cat  l
on c.customer_key=l.customer_key
full outer join 
ssharma_db.at_agg_sameprd  e 
on c.customer_key=e.customer_key
full outer join 
ssharma_db.at_cat_sameprd  d 
on c.customer_key=d.customer_key;

