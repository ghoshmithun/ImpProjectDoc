library(dplyr)


df.train.subset <- df.train.score %>% select(masterkey,activity,items_purch_pred_window,score)
df.train.subset <- df.train.subset %>% arrange(masterkey,desc(items_purch_pred_window),activity)
df.train.subset <- df.train.subset %>% group_by(masterkey) %>% filter(sum(items_purch_pred_window) !=0)
df.train.subset <- df.train.subset %>% group_by(masterkey) %>% mutate(idealrank = row_number())
df.train.subset <- df.train.subset %>% arrange(masterkey,desc(score),activity)
df.train.subset <- df.train.subset %>% group_by(masterkey) %>% mutate(scorerank = row_number())
df.train.subset <- df.train.subset %>% mutate(disc.gain = ((2^items_purch_pred_window - 1)/log2(scorerank + 1)),
                                              ideal.disc.gain = ((2^items_purch_pred_window - 1)/log2(idealrank + 1)))
train.dcg <- df.train.subset %>% group_by(masterkey) %>% summarise(dcg = sum(disc.gain),idcg=sum(ideal.disc.gain))
train.ndcg <- train.dcg %>% filter(idcg != 0) %>% mutate(ndcg = dcg/idcg)

mean(train.ndcg$ndcg)   # 0.9023

#saveRDS(df.test.score,file = "df.test.score.RDS")
# Repeat the process for test dataset
df.test.score <- df.test.score %>% group_by(masterkey) %>% filter(sum(items_purch_pred_window) !=0)

df.test.subset <- df.test.score %>% select(masterkey,activity,items_purch_pred_window,score)
df.test.subset <- df.test.subset %>% arrange(masterkey,desc(items_purch_pred_window),activity)
df.test.subset <- df.test.subset %>% group_by(masterkey) %>% mutate(idealrank = row_number())
df.test.subset <- df.test.subset %>% arrange(masterkey,desc(score),activity)
df.test.subset <- df.test.subset %>% group_by(masterkey) %>% mutate(scorerank = row_number())
df.test.subset <- df.test.subset %>% mutate(disc.gain = ((2^items_purch_pred_window - 1)/log2(scorerank + 1)),
                                              ideal.disc.gain = ((2^items_purch_pred_window - 1)/log2(idealrank + 1)))
test.dcg <- df.test.subset %>% group_by(masterkey) %>% summarise(dcg = sum(disc.gain),idcg=sum(ideal.disc.gain))
test.ndcg <- test.dcg %>% filter(idcg != 0) %>% mutate(ndcg = dcg/idcg)
mean(test.ndcg$ndcg)   # 0.9059

# Exact matches section

df.test.subset <- df.test.subset %>% mutate(match = ifelse(idealrank == scorerank,1,0))
test.match <- df.test.subset %>% group_by(masterkey) %>% summarise(activitycount = n(),summatch = sum(match))
test.match <- test.match %>% mutate(exactmatch = ifelse(activitycount==summatch,1,0))
mean(test.match$exactmatch) #57.91%


# Baseline comparisons for 12 month purchases
test.12monthpurch <- df.test.score %>% group_by(masterkey) %>% filter(sum(weighted_purchase) != 0)
test.12monthpurch <- test.12monthpurch  %>% select(masterkey,activity,weighted_purchase,items_purch_pred_window,score)
test.12monthpurch <- test.12monthpurch %>% arrange(masterkey,desc(items_purch_pred_window),activity)
test.12monthpurch <- test.12monthpurch %>% group_by(masterkey)  %>% mutate(idealrank = row_number())

test.12monthpurch <- test.12monthpurch %>% arrange(masterkey,desc(score),activity)
test.12monthpurch <- test.12monthpurch %>% group_by(masterkey)  %>% mutate(scorerank = row_number())

test.12monthpurch <- test.12monthpurch %>% mutate(match = ifelse(idealrank == scorerank,1,0))
test.12monthpurch.match <- test.12monthpurch %>% group_by(masterkey) %>% summarise(activitycount = n(),summatch = sum(match))
test.12monthpurch.match <- test.12monthpurch.match %>% mutate(exactmatch = ifelse(activitycount==summatch,1,0))
mean(test.12monthpurch.match$exactmatch) #40.62%


test.12monthpurch <- test.12monthpurch %>% arrange(masterkey,desc(weighted_purchase),activity)
test.12monthpurch <- test.12monthpurch %>% group_by(masterkey)  %>% mutate(baselinerank = row_number())
test.12monthpurch <- test.12monthpurch %>% mutate(baselinematch = ifelse(idealrank == baselinerank,1,0))
test.12monthpurch.baselinematch <- test.12monthpurch %>% group_by(masterkey) %>% summarise(activitycount = n(),sumbaselinematch = sum(baselinematch))
test.12monthpurch.baselinematch <- test.12monthpurch.baselinematch %>% mutate(exactbaselinematch = ifelse(activitycount==sumbaselinematch,1,0))
mean(test.12monthpurch.baselinematch$exactbaselinematch) #31.61%

# 12 month purchase baseline ndcg
test.12monthpurch <- test.12monthpurch %>% mutate(disc.gain = ((2^items_purch_pred_window - 1)/log2(scorerank + 1)),
                                            baseline.disc.gain = ((2^items_purch_pred_window - 1)/log2(baselinerank + 1)),
                                            ideal.disc.gain = ((2^items_purch_pred_window - 1)/log2(idealrank + 1)))

test.12monthpurch.dcg <- test.12monthpurch %>% group_by(masterkey) %>% summarise(modeldcg = sum(disc.gain),baselinedcg = sum(baseline.disc.gain),idcg=sum(ideal.disc.gain))
test.12monthpurch.ndcg <- test.12monthpurch.dcg %>% filter(idcg != 0) %>% mutate(baselinendcg = baselinedcg/idcg, modelndcg = modeldcg/idcg)
mean(test.12monthpurch.ndcg$baselinendcg)   # 0.8136
mean(test.12monthpurch.ndcg$modelndcg)  # 0.8707

# 12 month purchase baseline partial matches
mean(test.12monthpurch$match) # 47.81%
mean(test.12monthpurch$baselinematch) #39.35%

# Baseline comparisons for 3 month browsing history
test.3monthbrowse <- df.test.score %>% group_by(masterkey) %>% filter(sum(nrml_items_browsed) != 0,sum(items_purch_pred_window)!=0)
test.3monthbrowse <- test.3monthbrowse  %>% select(masterkey,activity,nrml_items_browsed,items_purch_pred_window,score)
test.3monthbrowse <- test.3monthbrowse %>% arrange(masterkey,desc(items_purch_pred_window),activity)
test.3monthbrowse <- test.3monthbrowse %>% group_by(masterkey)  %>% mutate(idealrank = row_number())

test.3monthbrowse <- test.3monthbrowse %>% arrange(masterkey,desc(score),activity)
test.3monthbrowse <- test.3monthbrowse %>% group_by(masterkey)  %>% mutate(scorerank = row_number())

test.3monthbrowse <- test.3monthbrowse %>% mutate(match = ifelse(idealrank == scorerank,1,0))
test.3monthbrowse.match <- test.3monthbrowse %>% group_by(masterkey) %>% summarise(activitycount = n(),summatch = sum(match))
test.3monthbrowse.match <- test.3monthbrowse.match %>% mutate(exactmatch = ifelse(activitycount==summatch,1,0))
mean(test.3monthbrowse.match$exactmatch) #37.25%


test.3monthbrowse <- test.3monthbrowse %>% arrange(masterkey,desc(nrml_items_browsed),activity)
test.3monthbrowse <- test.3monthbrowse %>% group_by(masterkey)  %>% mutate(baselinerank = row_number())
test.3monthbrowse <- test.3monthbrowse %>% mutate(baselinematch = ifelse(idealrank == baselinerank,1,0))
test.3monthbrowse.baselinematch <- test.3monthbrowse %>% group_by(masterkey) %>% summarise(activitycount = n(),sumbaselinematch = sum(baselinematch))
test.3monthbrowse.baselinematch <- test.3monthbrowse.baselinematch %>% mutate(exactbaselinematch = ifelse(activitycount==sumbaselinematch,1,0))
mean(test.3monthbrowse.baselinematch$exactbaselinematch) #33.72%

# 3 month browse baseline ndcg
test.3monthbrowse <- test.3monthbrowse %>% mutate(baseline.disc.gain = ((2^items_purch_pred_window - 1)/log2(baselinerank + 1)),
                                                  ideal.disc.gain = ((2^items_purch_pred_window - 1)/log2(idealrank + 1)),
                                                  disc.gain = ((2^items_purch_pred_window - 1)/log2(scorerank + 1)))

test.3monthbrowse.dcg <- test.3monthbrowse %>% group_by(masterkey) %>% summarise(baselinedcg = sum(baseline.disc.gain),idcg=sum(ideal.disc.gain),modeldcg=sum(disc.gain))
test.3monthbrowse.ndcg <- test.3monthbrowse.dcg %>% filter(idcg != 0) %>% mutate(baselinendcg = baselinedcg/idcg, modelndcg = modeldcg/idcg)
mean(test.3monthbrowse.ndcg$baselinendcg)   # 0.8330
mean(test.3monthbrowse.ndcg$modelndcg)      # 0.8674

# 3 month browse baseline partial matches
mean(test.3monthbrowse$match)  #46.52%
mean(test.3monthbrowse$baselinematch) #43.44%


# Individual rankwise calculations
test.12monthpurch.rank1 <- test.12monthpurch %>% filter(idealrank == 1)
sum(test.12monthpurch.rank1$match) # 224,999
sum(test.12monthpurch.rank1$baselinematch) # 181,467

test.12monthpurch.rank2 <- test.12monthpurch %>% filter(idealrank <= 2)
test.12monthpurch.rank2.match <- test.12monthpurch.rank2 %>% group_by(masterkey) %>%
                                    summarise(activitycount = n(),summatch= sum(match),sumbaselinematch=sum(baselinematch))
test.12monthpurch.rank2.match %>% filter(activitycount == summatch) %>% tally  # 164,613
test.12monthpurch.rank2.match %>% filter(activitycount == sumbaselinematch) %>% tally  # 126,214

test.12monthpurch.rank3 <- test.12monthpurch %>% filter(idealrank <= 3)
test.12monthpurch.rank3.match <- test.12monthpurch.rank3 %>% group_by(masterkey) %>%
                              summarise(activitycount = n(),summatch= sum(match),sumbaselinematch=sum(baselinematch))
test.12monthpurch.rank3.match %>% filter(activitycount == summatch) %>% tally  # 154,411
test.12monthpurch.rank3.match %>% filter(activitycount == sumbaselinematch) %>% tally  # 120,151

test.12monthpurch.match %>% filter(activitycount == summatch) %>% tally # 154,411
test.12monthpurch.baselinematch %>% filter(activitycount == sumbaselinematch) %>% tally #120151

test.12monthpurch.rank1.dcg <- test.12monthpurch.rank1 %>% group_by(masterkey) %>% summarise(baselinedcg = sum(baseline.disc.gain),idcg=sum(ideal.disc.gain),modeldcg=sum(disc.gain))
test.12monthpurch.rank1.ndcg <- test.12monthpurch.rank1.dcg %>% filter(idcg != 0) %>% mutate(baselinendcg = baselinedcg/idcg, modelndcg = modeldcg/idcg)
mean(test.12monthpurch.rank1.ndcg$baselinendcg)   # 0.7745
mean(test.12monthpurch.rank1.ndcg$modelndcg)      # 0.8262

test.12monthpurch.rank2.dcg <- test.12monthpurch.rank2 %>% group_by(masterkey) %>% summarise(baselinedcg = sum(baseline.disc.gain),idcg=sum(ideal.disc.gain),modeldcg=sum(disc.gain))
test.12monthpurch.rank2.ndcg <- test.12monthpurch.rank2.dcg %>% filter(idcg != 0) %>% mutate(baselinendcg = baselinedcg/idcg, modelndcg = modeldcg/idcg)
mean(test.12monthpurch.rank2.ndcg$baselinendcg)   # 0.8072
mean(test.12monthpurch.rank2.ndcg$modelndcg)      # 0.8634


# 3 month browse individual rank calculations
test.3monthbrowse.rank1 <- test.3monthbrowse %>% filter(idealrank == 1)
sum(test.3monthbrowse.rank1$match) # 157,106
sum(test.3monthbrowse.rank1$baselinematch) # 140,353

test.3monthbrowse.rank2 <- test.3monthbrowse %>% filter(idealrank <= 2)
test.3monthbrowse.rank2.match <- test.3monthbrowse.rank2 %>% group_by(masterkey) %>%
  summarise(activitycount = n(),summatch= sum(match),sumbaselinematch=sum(baselinematch))
test.3monthbrowse.rank2.match %>% filter(activitycount == summatch) %>% tally  # 107,999
test.3monthbrowse.rank2.match %>% filter(activitycount == sumbaselinematch) %>% tally  # 96,621

test.3monthbrowse.rank3 <- test.3monthbrowse %>% filter(idealrank <= 3)
test.3monthbrowse.rank3.match <- test.3monthbrowse.rank3 %>% group_by(masterkey) %>%
  summarise(activitycount = n(),summatch= sum(match),sumbaselinematch=sum(baselinematch))
test.3monthbrowse.rank3.match %>% filter(activitycount == summatch) %>% tally  # 98,771
test.3monthbrowse.rank3.match %>% filter(activitycount == sumbaselinematch) %>% tally  # 89,394

test.3monthbrowse.match %>% filter(activitycount == summatch) %>% tally # 98,771
test.3monthbrowse.baselinematch %>% filter(activitycount == sumbaselinematch) %>% tally # 89,394

test.3monthbrowse.rank1.dcg <- test.3monthbrowse.rank1 %>% group_by(masterkey) %>% summarise(baselinedcg = sum(baseline.disc.gain),idcg=sum(ideal.disc.gain),modeldcg=sum(disc.gain))
test.3monthbrowse.rank1.ndcg <- test.3monthbrowse.rank1.dcg %>% filter(idcg != 0) %>% mutate(baselinendcg = baselinedcg/idcg, modelndcg = modeldcg/idcg)
mean(test.3monthbrowse.rank1.ndcg$baselinendcg)   # 0.7967
mean(test.3monthbrowse.rank1.ndcg$modelndcg)      # 0.8252

test.3monthbrowse.rank2.dcg <- test.3monthbrowse.rank2 %>% group_by(masterkey) %>% summarise(baselinedcg = sum(baseline.disc.gain),idcg=sum(ideal.disc.gain),modeldcg=sum(disc.gain))
test.3monthbrowse.rank2.ndcg <- test.3monthbrowse.rank2.dcg %>% filter(idcg != 0) %>% mutate(baselinendcg = baselinedcg/idcg, modelndcg = modeldcg/idcg)
mean(test.3monthbrowse.rank2.ndcg$baselinendcg)   # 0.8273
mean(test.3monthbrowse.rank2.ndcg$modelndcg)      # 0.8605