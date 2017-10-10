hadoop fs -ls /user/tghosh/ON_20130101_LTV_CALIB_TRANSACTION_TXT
hadoop fs -copyToLocal /user/tghosh/ON_20130101_LTV_CALIB_TRANSACTION_TXT /home/tghosh/

hadoop fs -ls /user/tghosh/ON_20130101_LTV_VALID_TRANSACTION_TXT
hadoop fs -copyToLocal /user/tghosh/ON_20130101_LTV_VALID_TRANSACTION_TXT /home/tghosh/


hadoop dfs -getmerge /user/tghosh/BR_20140101_LTV_VALID_TRANSACTION_TXT /home/tghosh/BR_20140101_LTV_VALID_TRANSACTION.TXT
hadoop dfs -getmerge /user/tghosh/BR_20140101_LTV_CALIB_TRANSACTION_TXT /home/tghosh/BR_20140101_LTV_CALIB_TRANSACTION.TXT

hadoop dfs -getmerge /user/tghosh/GP_20140101_LTV_VALID_TRANSACTION_TXT /home/tghosh/GP_20140101_LTV_VALID_TRANSACTION.TXT
hadoop dfs -getmerge /user/tghosh/GP_20140101_LTV_CALIB_TRANSACTION_TXT /home/tghosh/GP_20140101_LTV_CALIB_TRANSACTION.TXT

hadoop dfs -getmerge /user/tghosh/ON_20140101_LTV_VALID_TRANSACTION_TXT /home/tghosh/ON_20140101_LTV_VALID_TRANSACTION.TXT
hadoop dfs -getmerge /user/tghosh/ON_20140101_LTV_CALIB_TRANSACTION_TXT /home/tghosh/ON_20140101_LTV_CALIB_TRANSACTION.TXT
