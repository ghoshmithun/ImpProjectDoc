set mapred.job.queue.name=aa;  
set hive.cli.print.header=true;
set purch_window_start_date = '2016-07-22';

---pulling product_keys from orderline_t
drop table if exists sruidas.product_key_info;
create table sruidas.product_key_info as
select customer_key, product_key
from mds_new.ods_orderline_t 
where country = 'US' and brand='GP' and transaction_date between ${hiveconf:purch_window_start_date} and date_add(${hiveconf:purch_window_start_date},366)  and sales_amt > 0 and item_qty > 0;

select count(*) from sruidas.product_key_info;
--139235264


--mapping product to final table of GP_time_to_next_purchase
drop table if exists sruidas.product_key_map;
create table sruidas.product_key_map as
select t1.*,t2.product_key 
from sruidas.gp_all_variables_score1 t1
left outer join sruidas.product_key_info t2
on t1.customer_key=t2.customer_key;

select count(*) from sruidas.product_key_map;
--137931082
select count (distinct customer_key) from sruidas.product_key_map;
--8231427

--- Append style to the data
drop table if exists sruidas.product_add_style;
create table sruidas.product_add_style as
select t1.*, t2.style_color_cd 
from sruidas.product_key_map t1 
left outer join mds_new.ods_product_t  t2
on t1.product_key = t2.product_key;

select count(*) from sruidas.product_add_style;
--137931082

select count (distinct customer_key) from sruidas.product_add_style;
--8231427


--- Append product div,dept,class info to the data
drop table if exists sruidas.product_add_div_dep_class;
create table sruidas.product_add_div_dep_class as
select distinct t1.customer_key,t1.style_color_cd, t2.mdse_div_desc, t2.mdse_dept_desc, t2.mdse_cls_desc
from sruidas.product_add_style t1 
left outer join venus.sku_dim t2
on t1.style_color_cd = t2.opr_sty_clr_cd;

select count(*) from sruidas.product_add_div_dep_class;
--88145502

select count (distinct customer_key) from sruidas.product_add_div_dep_class;
--8231427

---Defining and adding categories
drop table if exists sruidas.product_map_cat;
create table sruidas.product_map_cat as
select t1.*,
case
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens woven tops' and lower(t1.mdse_cls_desc) = 'mens red woven tops' then 'mens_tops_woven'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens woven tops' and lower(t1.mdse_cls_desc) = 's/s shirts' then 'mens_tops_woven'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens woven tops' and lower(t1.mdse_cls_desc) = 'l/s fashion' then 'mens_tops_woven'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens woven tops' and lower(t1.mdse_cls_desc) = 'basic haberdashery' then 'mens_tops_woven'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens knits' and lower(t1.mdse_cls_desc) = 'basic tees' then 'mens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens knits' and lower(t1.mdse_cls_desc) = 'l/s knits' then 'mens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens knits' and lower(t1.mdse_cls_desc) = 'mens red knits' then 'mens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens knits' and lower(t1.mdse_cls_desc) = 's/s knits' then 'mens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens knits' and lower(t1.mdse_cls_desc) = 'polo' then 'mens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens knits' and lower(t1.mdse_cls_desc) = 'novety tees' then 'mens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens knits' and lower(t1.mdse_cls_desc) = 'fleece' then 'mens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens fit' and lower(t1.mdse_cls_desc) = 'l/s knits' then 'mens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens fit' and lower(t1.mdse_cls_desc) = 's/s knits' then 'mens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens fit' and lower(t1.mdse_cls_desc) = 'tanks' then 'mens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens outerwear' and lower(t1.mdse_cls_desc) = 'mens red outerwear' then 'mens_outerwear_jacket_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens outerwear' and lower(t1.mdse_cls_desc) = 'wool jackets' then 'mens_outerwear_jacket_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens outerwear' and lower(t1.mdse_cls_desc) = 'vests' then 'mens_outerwear_jacket_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens outerwear' and lower(t1.mdse_cls_desc) = 'leather' then 'mens_outerwear_jacket_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens outerwear' and lower(t1.mdse_cls_desc) = 'jackets' then 'mens_outerwear_jacket_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens outerwear' and lower(t1.mdse_cls_desc) = 'blazers' then 'mens_outerwear_jacket_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens fit' and lower(t1.mdse_cls_desc) = 'outerwear' then 'mens_outerwear_jacket_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens sweaters' and lower(t1.mdse_cls_desc) = 'wool/wool blend' then 'mens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens sweaters' and lower(t1.mdse_cls_desc) = 'mens red sweaters' then 'mens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens sweaters' and lower(t1.mdse_cls_desc) = 'vests' then 'mens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens sweaters' and lower(t1.mdse_cls_desc) = 'cotton' then 'mens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens sweaters' and lower(t1.mdse_cls_desc) = 'other' then 'mens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens outerwear' and lower(t1.mdse_cls_desc) = 'fleece' then 'mens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens fit' and lower(t1.mdse_cls_desc) = 'fleece tops' then 'mens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens denim' and lower(t1.mdse_cls_desc) = '5 pocket' then 'mens_denim'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens denim' and lower(t1.mdse_cls_desc) = 'fashion denim' then 'mens_denim'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens denim' and lower(t1.mdse_cls_desc) = 'mens non-indigo' then 'mens_denim'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens denim' and lower(t1.mdse_cls_desc) = 'shirts' then 'mens_denim'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens denim' and lower(t1.mdse_cls_desc) = 'shorts' then 'mens_denim'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens denim' and lower(t1.mdse_cls_desc) = 'denim jackets' then 'mens_denim'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens denim' and lower(t1.mdse_cls_desc) = 'mens 1969' then 'mens_denim'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens bottoms' and lower(t1.mdse_cls_desc) = 'fashion bottoms' then 'mens_bottom_woven'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens bottoms' and lower(t1.mdse_cls_desc) = 'active bottoms' then 'mens_bottom_woven'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens bottoms' and lower(t1.mdse_cls_desc) = 'basics' then 'mens_bottom_woven'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens fit' and lower(t1.mdse_cls_desc) = 'shorts' then 'mens_bottom_half'
when lower(t1.mdse_div_desc) = 'gap mens' and lower(t1.mdse_dept_desc) = 'mens bottoms' and lower(t1.mdse_cls_desc) = 'shorts' then 'mens_bottom_half'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens accessories' and lower(t1.mdse_cls_desc) = 'mens socks' then 'mens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens accessories' and lower(t1.mdse_cls_desc) = 'shoes' then 'mens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens accessories' and lower(t1.mdse_cls_desc) = 'mens red accessories' then 'mens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens accessories' and lower(t1.mdse_cls_desc) = 'mens belts' then 'mens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens accessories' and lower(t1.mdse_cls_desc) = 'mens hats' then 'mens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens accessories' and lower(t1.mdse_cls_desc) = 'keds' then 'mens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens body' and lower(t1.mdse_cls_desc) = 'boxer brief' then 'mens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens body' and lower(t1.mdse_cls_desc) = 'lounge' then 'mens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens body' and lower(t1.mdse_cls_desc) = 'boxers' then 'mens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens body' and lower(t1.mdse_cls_desc) = 'packaged underwear' then 'mens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'sunglasses' and lower(t1.mdse_cls_desc) = 'mens sunglasses' then 'mens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens accessories' and lower(t1.mdse_cls_desc) = 'mens bags' then 'mens_accessories_other'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens accessories' and lower(t1.mdse_cls_desc) = 'miscellaneous' then 'mens_accessories_other'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'mens accessories' and lower(t1.mdse_cls_desc) = 'seasonal accessories' then 'mens_accessories_other'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens woven tops' and lower(t1.mdse_cls_desc) = 's/s tops' then 'womens_tops_woven'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens woven tops' and lower(t1.mdse_cls_desc) = 'sleeveless' then 'womens_tops_woven'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens woven tops' and lower(t1.mdse_cls_desc) = 'l/s tops' then 'womens_tops_woven'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens knits' and lower(t1.mdse_cls_desc) = 'l/s knits' then 'womens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens knits' and lower(t1.mdse_cls_desc) = 'sleeveless' then 'womens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens knits' and lower(t1.mdse_cls_desc) = 'womens red knits/activ' then 'womens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens knits' and lower(t1.mdse_cls_desc) = 'active tops' then 'womens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens knits' and lower(t1.mdse_cls_desc) = 's/s knits' then 'womens_tops_knits'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens outerwear/items' and lower(t1.mdse_cls_desc) = 'denim' then 'womens_outerwear_jacket_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens outerwear/items' and lower(t1.mdse_cls_desc) = 'jackets' then 'womens_outerwear_jacket_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens outerwear/items' and lower(t1.mdse_cls_desc) = 'vests' then 'womens_outerwear_jacket_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens outerwear/items' and lower(t1.mdse_cls_desc) = 'womens red outr/items' then 'womens_outerwear_jacket_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens sweaters' and lower(t1.mdse_cls_desc) = 'items' then 'womens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens sweaters' and lower(t1.mdse_cls_desc) = 'long sleeve' then 'womens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens sweaters' and lower(t1.mdse_cls_desc) = 'vests' then 'womens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens sweaters' and lower(t1.mdse_cls_desc) = 'sleeveless' then 'womens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens sweaters' and lower(t1.mdse_cls_desc) = 'short sleeve' then 'womens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens sweaters' and lower(t1.mdse_cls_desc) = 'womens red sweaters' then 'womens_outerwear_sweater_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens denim' and lower(t1.mdse_cls_desc) = '1969' then 'womens_denim'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens denim' and lower(t1.mdse_cls_desc) = 'basic 5 pocket' then 'womens_denim'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens denim' and lower(t1.mdse_cls_desc) = 'dress' then 'womens_denim'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens denim' and lower(t1.mdse_cls_desc) = 'fashion denim' then 'womens_denim'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens denim' and lower(t1.mdse_cls_desc) = 'overalls/shortalls' then 'womens_denim'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens denim' and lower(t1.mdse_cls_desc) = 'shorts' then 'womens_denim'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens denim' and lower(t1.mdse_cls_desc) = 'womens red denim' then 'womens_denim'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens denim' and lower(t1.mdse_cls_desc) = 'capris/cropped' then 'womens_denim'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens denim' and lower(t1.mdse_cls_desc) = 'denim jackets' then 'womens_denim'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens denim' and lower(t1.mdse_cls_desc) = 'denim woven tops' then 'womens_denim'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens denim' and lower(t1.mdse_cls_desc) = 'skirts' then 'womens_denim'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens bottoms' and lower(t1.mdse_cls_desc) = 'capris/cropped' then 'womens_bottoms_pant_short_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens bottoms' and lower(t1.mdse_cls_desc) = 'leather/suede' then 'womens_bottoms_pant_short_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens bottoms' and lower(t1.mdse_cls_desc) = 'long pants' then 'womens_bottoms_pant_short_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens bottoms' and lower(t1.mdse_cls_desc) = 'shorts' then 'womens_bottoms_pant_short_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens knits' and lower(t1.mdse_cls_desc) = 'active bottoms' then 'womens_bottoms_pant_short_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens bottoms' and lower(t1.mdse_cls_desc) = 'skirts' then 'womens_bottoms_skirt_like'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens bottoms' and lower(t1.mdse_cls_desc) = 'dresses' then 'womens_dresses'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens knits' and lower(t1.mdse_cls_desc) = 'knit dressing' then 'womens_dresses'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'womens outerwear/items' and lower(t1.mdse_cls_desc) = 'dresses' then 'womens_dresses'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'swim wear' and lower(t1.mdse_cls_desc) = 'swim other' then 'womens_swimwear'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'swim wear' and lower(t1.mdse_cls_desc) = 'swim tops' then 'womens_swimwear'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'swim wear' and lower(t1.mdse_cls_desc) = 'swim 1pc' then 'womens_swimwear'
when lower(t1.mdse_div_desc) = 'gap womens' and lower(t1.mdse_dept_desc) = 'swim wear' and lower(t1.mdse_cls_desc) = 'swim bottoms' then 'womens_swimwear'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'sunglasses' and lower(t1.mdse_cls_desc) = 'womens sunglasses' then 'womens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'womens accessories' and lower(t1.mdse_cls_desc) = 'miscellaneous' then 'womens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'womens accessories' and lower(t1.mdse_cls_desc) = 'seasonal accessories' then 'womens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'womens accessories' and lower(t1.mdse_cls_desc) = 'shoes' then 'womens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'womens accessories' and lower(t1.mdse_cls_desc) = 'women s keds' then 'womens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'womens accessories' and lower(t1.mdse_cls_desc) = 'womens scarves' then 'womens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'womens accessories' and lower(t1.mdse_cls_desc) = 'womens socks' then 'womens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'womens accessories' and lower(t1.mdse_cls_desc) = 'womens tights' then 'womens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'womens accessories' and lower(t1.mdse_cls_desc) = 'womens belts' then 'womens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'womens accessories' and lower(t1.mdse_cls_desc) = 'womens hair' then 'womens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'womens accessories' and lower(t1.mdse_cls_desc) = 'womens hats' then 'womens_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'womens accessories' and lower(t1.mdse_cls_desc) = 'jewelry' then 'womens_accessories_jewelry_bags'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'womens accessories' and lower(t1.mdse_cls_desc) = 'womens bags' then 'womens_accessories_jewelry_bags'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'jewelry' and lower(t1.mdse_cls_desc) = 'hair' then 'womens_accessories_jewelry_bags'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'jewelry' and lower(t1.mdse_cls_desc) = 'bracelets' then 'womens_accessories_jewelry_bags'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'jewelry' and lower(t1.mdse_cls_desc) = 'necklaces' then 'womens_accessories_jewelry_bags'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'denim bottoms' then 'baby_boy_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'woven bottoms' then 'baby_boy_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'knit bottoms' then 'baby_boy_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'tops' then 'baby_boy_tops'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'active/layering tops' then 'baby_boy_tops'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'sweaters' then 'baby_boy_outerwear'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'outerwear' then 'baby_boy_outerwear'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'swim' then 'baby_boy_swim'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'bodysuits' then 'baby_boy_onepiece_like'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'overalls' then 'baby_boy_onepiece_like'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'one pieces' then 'baby_boy_onepiece_like'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'socks' then 'baby_boy_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'shoes' then 'baby_boy_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'hats' then 'baby_boy_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby boy' and lower(t1.mdse_cls_desc) = 'other' then 'baby_boy_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'infant boy hats' then 'baby_boy_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'infant boy other' then 'baby_boy_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'infant boy shoes' then 'baby_boy_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'infant boy socks' then 'baby_boy_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'dresses' then 'baby_girl_dresses'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'tops' then 'baby_girl_tops'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'active/layering tops' then 'baby_girl_tops'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'denim bottoms' then 'baby_girl_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'woven bottoms' then 'baby_girl_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'knit bottoms' then 'baby_girl_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'swim' then 'baby_girl_swim'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'sweaters' then 'baby_girl_outerwear'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'outerwear' then 'baby_girl_outerwear'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'bodysuits' then 'baby_girl_onepiece_like'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'one pieces' then 'baby_girl_onepiece_like'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'socks' then 'baby_girl_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'shoes' then 'baby_girl_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'hats' then 'baby_girl_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'baby girl' and lower(t1.mdse_cls_desc) = 'other' then 'baby_girl_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'infant girl hats' then 'baby_girl_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'infant girl other' then 'baby_girl_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'infant girl shoes' then 'baby_girl_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'infant girl socks' then 'baby_girl_other_wearables'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy knits' and lower(t1.mdse_cls_desc) = 'tops' then 'toddler_boy_tops'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy knits' and lower(t1.mdse_cls_desc) = 'active/layering tops' then 'toddler_boy_tops'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy knits' and lower(t1.mdse_cls_desc) = 'graphics' then 'toddler_boy_tops'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy wovens' and lower(t1.mdse_cls_desc) = 'tops' then 'toddler_boy_tops'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy knits' and lower(t1.mdse_cls_desc) = 'sweaters' then 'toddler_boy_outerwear'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy knits' and lower(t1.mdse_cls_desc) = 'outerwear' then 'toddler_boy_outerwear'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy wovens' and lower(t1.mdse_cls_desc) = 'outerwear' then 'toddler_boy_outerwear'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy wovens' and lower(t1.mdse_cls_desc) = 'overalls' then 'toddler_boy_onepiece_like'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy wovens' and lower(t1.mdse_cls_desc) = 'shortalls' then 'toddler_boy_onepiece_like'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy knits' and lower(t1.mdse_cls_desc) = 'pants' then 'toddler_boy_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy knits' and lower(t1.mdse_cls_desc) = 'shorts' then 'toddler_boy_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy wovens' and lower(t1.mdse_cls_desc) = 'pants' then 'toddler_boy_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy wovens' and lower(t1.mdse_cls_desc) = 'shorts' then 'toddler_boy_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy wovens' and lower(t1.mdse_cls_desc) = 'denim' then 'toddler_boy_denim'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy knits' and lower(t1.mdse_cls_desc) = 'swim' then 'toddler_boy_swim'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy wovens' and lower(t1.mdse_cls_desc) = 'swim' then 'toddler_boy_swim'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy knits' and lower(t1.mdse_cls_desc) = 'other' then 'toddler_boy_other'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler boy wovens' and lower(t1.mdse_cls_desc) = 'other' then 'toddler_boy_other'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'toddler boy socks' then 'toddler_boy_accessories_wearable'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'toddler boy hats' then 'toddler_boy_accessories_wearable'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'toddler boy other' then 'toddler_boy_accessories_wearable'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'toddler boy shoes' then 'toddler_boy_accessories_wearable'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl knits' and lower(t1.mdse_cls_desc) = 'tops' then 'toddler_girl_tops'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl knits' and lower(t1.mdse_cls_desc) = 'active/layering tops' then 'toddler_girl_tops'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl knits' and lower(t1.mdse_cls_desc) = 'graphic/embroidered' then 'toddler_girl_tops'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl wovens' and lower(t1.mdse_cls_desc) = 'tops' then 'toddler_girl_tops'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl knits' and lower(t1.mdse_cls_desc) = 'sweaters' then 'toddler_girl_outerwear'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl knits' and lower(t1.mdse_cls_desc) = 'outerwear' then 'toddler_girl_outerwear'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl wovens' and lower(t1.mdse_cls_desc) = 'outerwear' then 'toddler_girl_outerwear'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl wovens' and lower(t1.mdse_cls_desc) = 'overalls' then 'toddler_girl_onepiece_like'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl wovens' and lower(t1.mdse_cls_desc) = 'shortalls' then 'toddler_girl_onepiece_like'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl knits' and lower(t1.mdse_cls_desc) = 'pants' then 'toddler_girl_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl knits' and lower(t1.mdse_cls_desc) = 'knit short' then 'toddler_girl_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl wovens' and lower(t1.mdse_cls_desc) = 'pants' then 'toddler_girl_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl wovens' and lower(t1.mdse_cls_desc) = 'shorts' then 'toddler_girl_bottoms'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl knits' and lower(t1.mdse_cls_desc) = 'dresses' then 'toddler_girl_dresses_skirts'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl knits' and lower(t1.mdse_cls_desc) = 'skirts/skorts' then 'toddler_girl_dresses_skirts'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl wovens' and lower(t1.mdse_cls_desc) = 'dresses' then 'toddler_girl_dresses_skirts'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl wovens' and lower(t1.mdse_cls_desc) = 'skirts/skorts' then 'toddler_girl_dresses_skirts'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl wovens' and lower(t1.mdse_cls_desc) = 'denim' then 'toddler_girl_denim'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler girl knits' and lower(t1.mdse_cls_desc) = 'swim' then 'toddler_girl_swim'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'toddler girl socks' then 'toddler_girl_accessories_wearable'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'infant girl hats' then 'toddler_girl_accessories_wearable'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'infant girl other' then 'toddler_girl_accessories_wearable'
when lower(t1.mdse_div_desc) = 'baby' and lower(t1.mdse_dept_desc) = 'toddler accessories' and lower(t1.mdse_cls_desc) = 'infant girl shoes' then 'toddler_girl_accessories_wearable'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys active knits' and lower(t1.mdse_cls_desc) = 'vests' then 'boys_tops'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys active knits' and lower(t1.mdse_cls_desc) = 'active tops' then 'boys_tops'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys knts/sweaters' and lower(t1.mdse_cls_desc) = 'l/s knits' then 'boys_tops'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys knts/sweaters' and lower(t1.mdse_cls_desc) = 'screen tees' then 'boys_tops'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys knts/sweaters' and lower(t1.mdse_cls_desc) = 'tanks' then 'boys_tops'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys knts/sweaters' and lower(t1.mdse_cls_desc) = 'vests' then 'boys_tops'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys knts/sweaters' and lower(t1.mdse_cls_desc) = 's/s knits' then 'boys_tops'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys woven items' and lower(t1.mdse_cls_desc) = 'long sleeve' then 'boys_tops'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys woven items' and lower(t1.mdse_cls_desc) = 'short sleeve' then 'boys_tops'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys active knits' and lower(t1.mdse_cls_desc) = 'bottoms' then 'boys_bottoms'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys active knits' and lower(t1.mdse_cls_desc) = 'short' then 'boys_bottoms'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys woven bottoms' and lower(t1.mdse_cls_desc) = 'basic woven pants' then 'boys_bottoms'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys woven bottoms' and lower(t1.mdse_cls_desc) = 'fashion woven pants' then 'boys_bottoms'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys woven bottoms' and lower(t1.mdse_cls_desc) = 'basic woven shorts' then 'boys_bottoms'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys woven bottoms' and lower(t1.mdse_cls_desc) = 'fashion woven shorts' then 'boys_bottoms'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys woven bottoms' and lower(t1.mdse_cls_desc) = 'fashion jeans' then 'boys_denim'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys woven bottoms' and lower(t1.mdse_cls_desc) = 'basic denim shorts' then 'boys_denim'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys woven bottoms' and lower(t1.mdse_cls_desc) = 'basic jean' then 'boys_denim'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys woven bottoms' and lower(t1.mdse_cls_desc) = 'fashion denim shorts' then 'boys_denim'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys knts/sweaters' and lower(t1.mdse_cls_desc) = 'sweaters' then 'boys_outerwear'
when lower(t1.mdse_div_desc) = 'boys' and lower(t1.mdse_dept_desc) = 'boys woven bottoms' and lower(t1.mdse_cls_desc) = 'swim' then 'boys_outerwear'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys accessories' and lower(t1.mdse_cls_desc) = 'bags' then 'boys_accessories_other'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys accessories' and lower(t1.mdse_cls_desc) = 'non-apparel items' then 'boys_accessories_other'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys accessories' and lower(t1.mdse_cls_desc) = 'other' then 'boys_accessories_other'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys accessories' and lower(t1.mdse_cls_desc) = 'apparel items' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys accessories' and lower(t1.mdse_cls_desc) = 'socks' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys accessories' and lower(t1.mdse_cls_desc) = 'belts' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys accessories' and lower(t1.mdse_cls_desc) = 'shoes' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys accessories' and lower(t1.mdse_cls_desc) = 'sunglasses' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys accessories' and lower(t1.mdse_cls_desc) = 'cold weather' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys accessories' and lower(t1.mdse_cls_desc) = 'hats' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys underwear' and lower(t1.mdse_cls_desc) = 'long johns' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys underwear' and lower(t1.mdse_cls_desc) = 'other' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys underwear' and lower(t1.mdse_cls_desc) = 'sleepwear' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys underwear' and lower(t1.mdse_cls_desc) = 'slippers' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys underwear' and lower(t1.mdse_cls_desc) = 'boxers' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys underwear' and lower(t1.mdse_cls_desc) = 'underwear' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'boys underwear' and lower(t1.mdse_cls_desc) = 'underwear tops' then 'boys_accessories_wearables'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls active knits' and lower(t1.mdse_cls_desc) = 'tops' then 'girls_tops'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls knits/sweaters' and lower(t1.mdse_cls_desc) = 'sleeveless' then 'girls_tops'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls knits/sweaters' and lower(t1.mdse_cls_desc) = 'long sleeve tops' then 'girls_tops'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls knits/sweaters' and lower(t1.mdse_cls_desc) = 'short sleeve tops' then 'girls_tops'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls active knits' and lower(t1.mdse_cls_desc) = 'bottoms' then 'girls_tops'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls active knits' and lower(t1.mdse_cls_desc) = 'shorts' then 'girls_tops'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls woven bottoms' and lower(t1.mdse_cls_desc) = 'basic woven pant' then 'girls_bottoms'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls woven bottoms' and lower(t1.mdse_cls_desc) = 'fashion woven capri' then 'girls_bottoms'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls woven bottoms' and lower(t1.mdse_cls_desc) = 'fashion woven pant' then 'girls_bottoms'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls woven bottoms' and lower(t1.mdse_cls_desc) = 'woven shorts' then 'girls_bottoms'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls active knits' and lower(t1.mdse_cls_desc) = 'dresses' then 'girls_dresses_skirts'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls active knits' and lower(t1.mdse_cls_desc) = 'skirts' then 'girls_dresses_skirts'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls woven bottoms' and lower(t1.mdse_cls_desc) = 'basic jean' then 'girls_denim'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls woven bottoms' and lower(t1.mdse_cls_desc) = 'denim capri' then 'girls_denim'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls woven bottoms' and lower(t1.mdse_cls_desc) = 'denim shorts' then 'girls_denim'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls woven bottoms' and lower(t1.mdse_cls_desc) = 'fashion jean' then 'girls_denim'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls knits/sweaters' and lower(t1.mdse_cls_desc) = 'sweaters' then 'girls_outerwear'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls woven bottoms' and lower(t1.mdse_cls_desc) = 'shortalls/overalls' then 'girls_outerwear'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'girls active knits' and lower(t1.mdse_cls_desc) = 'swimwear' then 'girls_swim'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'kids/bby/matrnty red' and lower(t1.mdse_cls_desc) = 'boys red' then 'girls_maternity'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'kids/bby/matrnty red' and lower(t1.mdse_cls_desc) = 'girls red' then 'girls_maternity'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'kids/bby/matrnty red' and lower(t1.mdse_cls_desc) = 'kids red accessories' then 'girls_maternity'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'kids/bby/matrnty red' and lower(t1.mdse_cls_desc) = 'maternity red' then 'girls_maternity'
when lower(t1.mdse_div_desc) = 'girls' and lower(t1.mdse_dept_desc) = 'kids/bby/matrnty red' and lower(t1.mdse_cls_desc) = 'baby red' then 'girls_maternity'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls accessories' and lower(t1.mdse_cls_desc) = 'bags' then 'girls_accessories_other'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls accessories' and lower(t1.mdse_cls_desc) = 'non-apparel items' then 'girls_accessories_other'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls accessories' and lower(t1.mdse_cls_desc) = 'apparel items' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls accessories' and lower(t1.mdse_cls_desc) = ' socks' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls accessories' and lower(t1.mdse_cls_desc) = 'tights' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls accessories' and lower(t1.mdse_cls_desc) = 'belts' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls accessories' and lower(t1.mdse_cls_desc) = 'cold weather' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls accessories' and lower(t1.mdse_cls_desc) = 'hats' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls accessories' and lower(t1.mdse_cls_desc) = 'shoes' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls accessories' and lower(t1.mdse_cls_desc) = 'sunglasses' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls underwear' and lower(t1.mdse_cls_desc) = 'longjohns' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls underwear' and lower(t1.mdse_cls_desc) = 'sleepwear' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls underwear' and lower(t1.mdse_cls_desc) = 'slippers' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls underwear' and lower(t1.mdse_cls_desc) = 'boxers' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls underwear' and lower(t1.mdse_cls_desc) = 'underwear' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'accessories' and lower(t1.mdse_dept_desc) = 'girls underwear' and lower(t1.mdse_cls_desc) = 'underwear tops' then 'girls_accessories_wearables'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'tops' and lower(t1.mdse_cls_desc) = 'wovens' then 'maternity_tops'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'tops' and lower(t1.mdse_cls_desc) = 'active tops' then 'maternity_tops'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'tops' and lower(t1.mdse_cls_desc) = 'knits' then 'maternity_tops'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'active' and lower(t1.mdse_cls_desc) = 'tops' then 'maternity_tops'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'bottoms' and lower(t1.mdse_cls_desc) = 'shorts' then 'maternity_bottoms'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'bottoms' and lower(t1.mdse_cls_desc) = 'denim' then 'maternity_bottoms'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'bottoms' and lower(t1.mdse_cls_desc) = 'pants' then 'maternity_bottoms'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'bottoms' and lower(t1.mdse_cls_desc) = 'overalls' then 'maternity_bottoms'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'bottoms' and lower(t1.mdse_cls_desc) = 'capris' then 'maternity_bottoms'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'bottoms' and lower(t1.mdse_cls_desc) = 'active bottoms' then 'maternity_bottoms'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'active' and lower(t1.mdse_cls_desc) = 'bottoms' then 'maternity_bottoms'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'tops' and lower(t1.mdse_cls_desc) = 'sweaters' then 'maternity_outerwear'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'tops' and lower(t1.mdse_cls_desc) = 'outerwear' then 'maternity_outerwear'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'items' and lower(t1.mdse_cls_desc) = 'outerwear' then 'maternity_outerwear'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'tops' and lower(t1.mdse_cls_desc) = 'dresses' then 'maternity_dresses_skirts'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'items' and lower(t1.mdse_cls_desc) = 'dresses' then 'maternity_dresses_skirts'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'bottoms' and lower(t1.mdse_cls_desc) = 'skirts' then 'maternity_dresses_skirts'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'tops' and lower(t1.mdse_cls_desc) = 'swimwear' then 'maternity_swim'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'active' and lower(t1.mdse_cls_desc) = 'swimwear' then 'maternity_swim'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'body' and lower(t1.mdse_cls_desc) = 'underwear' then 'maternity_body'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'body' and lower(t1.mdse_cls_desc) = 'sleepwear' then 'maternity_body'
when lower(t1.mdse_div_desc) = 'maternity' and lower(t1.mdse_dept_desc) = 'body' and lower(t1.mdse_cls_desc) = 'foundations' then 'maternity_body'
else Null
end as ctg
from sruidas.product_add_div_dep_class t1;

select count(*) from sruidas.product_map_cat;
--88145502

select count (distinct customer_key) from sruidas.product_map_cat;
--8231427


---select custkey and categories where cat is not null
drop table if exists sruidas.final_cat_map;
create table sruidas.final_cat_map as
select t1.customer_key, t1.ctg from sruidas.product_map_cat t1
where t1.ctg is not Null;

select count(*) from sruidas.final_cat_map;
--32876539

select count (distinct customer_key) from sruidas.final_cat_map;
--4368923

---mapping with evergreenattribute gpcatpref model .max date of processing date

select max(processingdate)from evergreenattribute.gpcategorypreferenceattribute; 
--2017-07-27


drop table if exists sruidas.evergreen_gpcatpref1;
create table sruidas.evergreen_gpcatpref1 as
select customer_key,attribute
from evergreenattribute.gpcategorypreferenceattribute
lateral view explode(customerkey) customerkey AS customer_key
where processingdate='2017-07-27' and brand='GP' and market='US';

select count(*) from sruidas.evergreen_gpcatpref1
--11771285


drop table if exists sruidas.evergreen_gpcatpref2;
create table sruidas.evergreen_gpcatpref2 as
select customer_key,attributes
from sruidas.evergreen_gpcatpref1
lateral view explode(attribute) attribute AS attributes;


select count(*) from sruidas.evergreen_gpcatpref2;
--55150898

---mapping attributes to actual purchased category;

drop table if exists sruidas.map_cat_attr;
create table sruidas.map_cat_attr as
select t1.customer_key,coalesce(t1.ctg,'NULL') as ctg,coalesce(t2.attributes,'NULL') as attribute
from sruidas.final_cat_map t1
left outer join sruidas.evergreen_gpcatpref2 t2
on t1.customer_key=t2.customer_key;

select count(*) from sruidas.evergreen_gpcatpref2;
--55150898
select count(distinct customer_key) from sruidas.evergreen_gpcatpref2;
--11771285

drop table if exists sruidas.map_cat_attr_collect;
create table sruidas.map_cat_attr_collect as
select customer_key,collect_set(ctg) as category, collect_set(attribute) as attribute
from sruidas.map_cat_attr
group by customer_key;


