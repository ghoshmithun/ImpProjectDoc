-- Purchase timestamps and amounts
create table nkoul_clk.br_user_purchases_test as
select customer_key, transaction_num, order_num, demand_date, ship_date, ordhdr_trans_date, order_status, item_qty, gross_sales_amt, discount_amt,
transaction_type_cd, brand, country from mds_new.ods_orderheader_t where ordhdr_trans_date between '2016-07-01' and '2016-12-31';

--- Apppend masterkey
create table nkoul_clk.br_user_purcahses_test_m as
select t2.masterkey, t1.* from br_user_purchases_test t1 left outer join
(select distinct unknownshopperid, masterkey, customerkey from crmanalytics.customerresolution) t2
on t1.customer_key = t2.customerkey;

--- Filter for BR
create table nkoul_clk.br_user_purchase_test_ftr as
select * from nkoul_clk.br_user_purcahses_test_m where brand = 'BR' and country = 'US' and masterkey is not null and item_qty is not null;

-- Mkey level browse event times as well as purchase attributes.
create table nkoul_clk.br_user_purchase_test_group as
select masterkey, collect_set(concat_ws('__',ordhdr_trans_date,string(item_qty),string(gross_sales_amt),string(discount_amt))) prch_events
from nkoul_clk.br_user_purchase_test_ftr group by masterkey;

-- Data Export
insert overwrite local directory '/home/xnkoul/br_next_prch_ts' row format delimited select * from nkoul_clk.br_user_purchase_test_group;






