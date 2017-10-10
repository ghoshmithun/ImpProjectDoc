# -*- coding: utf-8 -*-
"""
Created on Fri Mar 31 10:43:22 2017

@author: mghosh
"""

import pandas as pd
import numpy as np

df=pd.read_table(filepath_or_buffer ="//10.8.8.51/lv0/Move to Box/Mithun/projects/9.BR_US_DS_Project/1.data/BR_disc_sens_geo_data.txt",delimiter="|",header=None,names=['customer_key','percent_disc_last_12_mth','br_net_sales_amt_12_mth','flag_employee','addr_zip_code','resp_disc_percent','browse_hit_ind_tot','purchased','br_online_sales12_mth','br_offline_sales_12mth'] ,dtype={'addr_zip_code':'str'})

df["br_retail_percent"]= df["br_offline_sales_12mth"] / df["br_net_sales_amt_12_mth"]

df["br_online_percent"] =  df["br_online_sales12_mth"] / df["br_net_sales_amt_12_mth"]

df=df.replace([np.inf, -np.inf], np.nan)
df["percent_disc_last_12_mth"]=df["percent_disc_last_12_mth"].replace([np.inf, -np.inf], np.nan)
df["percent_disc_last_12_mth"]=df["percent_disc_last_12_mth"].replace(np.nan,0)
#df=df.replace([np.inf, -np.inf], np.nan).dropna( how="all")

df["br_gross_sales_amt"] =  df["br_net_sales_amt_12_mth"] /  df["percent_disc_last_12_mth"].apply(lambda x : 1 - pd.to_numeric(x,errors="ignore"))

pd.Series((1-int(pd.to_numeric(val,errors="ignore"))) for val in df["percent_disc_last_12_mth"])

df["addr_zip_code"].astype("category")

#zip_wise_data = pd.pivot_table(data=df,values=["br_online_sales12_mth","br_offline_sales_12mth","br_net_sales_amt_12_mth","br_gross_sales_amt"],index=["addr_zip_code"] ,aggfunc=np.sum,fill_value =0)

#zip_wise_data = df.groupby(["addr_zip_code"]).sum()

addr_zip_code_column=df["addr_zip_code"].astype("category")

zip_wise_data2 = df.groupby([str("addr_zip_code")],as_index=False)

zip_wise_data3=zip_wise_data2["br_online_sales12_mth","br_offline_sales_12mth","br_net_sales_amt_12_mth"].sum()
