import numpy as np
import pandas as pd
from datetime import datetime
import ast
import scipy

dataPath = "hashOutput/"
dataPathMapping = "posts/"
outputPath = "hashOutput/"

df = pd.read_csv(dataPath+"codeGroupsIdsMapped.csv", delimiter =",")

######
## Internal root, time order
######

df_timeOrder = df_A[['md5','PostId','p_CreationDate']].append(df_Q[['md5','PostId','p_CreationDate']])

df_timeOrder['p_CreationDate'] = pd.to_datetime(df_timeOrder['p_CreationDate'])

df_innerRoot = df_timeOrder.sort_values('p_CreationDate',ascending=True).groupby('md5').head(1)

df_innerRoot.loc[:, 'innerRoot'] = 1


## not considering the edit date, external root
df = df.join(df_innerRoot['innerRoot'])
df['innerRoot'] = df['innerRoot'].fillna(0)
df['innerRoot'] = df['innerRoot'].astype(int)

df = df.join(df_timeOrder['p_CreationDate'])

df.to_csv(dataPath+"codeGroupsIdsMapped.csv", index = True, header = True)

##########
## All copies from the same post
##########

df = df.groupby(['md5', 'PostId', 'count'])['Id'].apply(list)
df = df.to_frame()
df = df.loc[df.Id.map(len) > 1]


df['BlockCount'] = df['Id'].str.len()
#df.join(df['Id'].str.len().to_frame(), rsuffix = "Count")

## Release multiindex
df.reset_index(inplace = True)

df_samePost = df.loc[(df['BlockCount'] == df['count'])]

df_samePost.groupby('count').count()


df_A = pd.read_csv(dataPath+"df_A.csv", delimiter =",")
df_A['p_CreationDate'] = pd.to_datetime(df_A['p_CreationDate'])

##########
## Post A groups
##########
df_A = pd.read_csv(dataPath+"df_A.csv", delimiter =",", index_col=0)
df_A.groupby('count').size()
df_A.groupby('p_Score').size().to_csv(outputPath+"df_A_scoreGroup.csv", index = True, header = True)

## Creation year-month - exact copies and overall
df_postA = pd.read_csv(dataPathMapping+"postA.csv", delimiter =",", index_col=0)#, dtype = {'p_CreationDate':datetime, 'p_OwnerUserId': int})
df_postA = df_postA.drop_duplicates()
df_postA = df_postA.drop(['p_ViewCount', 'p_FavoriteCount', 'p_Tags'], axis=1)
df_postA['p_CreationDate'] = pd.to_datetime(df_postA['p_CreationDate'])

## Distribution of creation date
df_postA.groupby(df_postA.p_CreationDate.dt.to_period("M")).size().to_csv(outputPath+"df_postA_creationDateGroup.csv", index = True, header = True)
df_A.groupby(df_A.p_CreationDate.dt.to_period("M")).size().to_csv(outputPath+"df_A_creationDateGroup.csv", index = True, header = True)

## Things about the actual copies
## line count and length

df_A = df_A.join(df_codeBlock)
df_codeBlock = pd.read_csv(dataPathMapping+"codeBlock_properties.csv", delimiter =",", index_col=0)


##########
## Post Q
##########
df_Q = pd.read_csv(dataPath+"df_Q.csv", delimiter =",", index_col=0)
df_Q = df_Q.join(df_codeBlock)

## Complement of post A
df_Q.groupby('count').size()

## Are high scores copied or copies from elsewhere
## Maybe after drawing the histogram
df_Q.groupby('p_Score').size().to_csv(outputPath+"df_Q_scoreGroup.csv", index = True, header = True)

## Is this distribution different?
df_Q.groupby('p_commentCount').size().to_csv(outputPath+"df_Q_commentGroup.csv", index = True, header = True)

## Same with high score? 
df_Q.groupby('p_ViewCount').size().to_csv(outputPath+"df_Q_viewGroup.csv", index = True, header = True)

## Tags stack
#df_Q['tags'] = df_Q.p_Tags.apply(ast.literal_eval)

df_Q = pd.read_csv(dataPath+"df_Q.csv", delimiter =",", index_col=0)

df_Q['p_Tags'] = df_Q['p_Tags'].str.replace('\>\<', '\',\'')


df_Q['p_Tags'] = df_Q['p_Tags'].str.replace('\<', '[\'')
df_Q['p_Tags'] = df_Q['p_Tags'].str.replace('\>', '\']')

df_Q['p_Tags'] = df_Q.p_Tags.apply(ast.literal_eval)


df_Q_tags = df_Q.p_Tags.apply(pd.Series).stack().reset_index(['Id'], name='tags')

## mapping from id to tags
df_Q_tags.to_csv(outputPath+"df_Q_tags.csv", index = True, header = True)
## same header files for html?
df_Q_tags.groupby('tags').size().sort_values(ascending=False).to_csv(outputPath+"df_Q_tagGroup.csv", index = True, header = True)



df_Q_tagCount = df_Q_tags.groupby('tags').size().sort_values(ascending=False).to_frame()

df_Q_tagCount.columns = ['copyCount']

### Tag stack for all question posts
df_tag = pd.read_csv(dataPathMapping+"tags.csv", delimiter =",", index_col=0)
df_Q_tagCount_overall = df_Q_tagCount.join(df_tag.set_index('TagName'), how = 'left')
df_Q_tagCount_overall.to_csv(outputPath+"df_Q_tagCount_overall.csv", index = True, header = True)
df_Q_tagCount_overall['copyCount'] = df_Q_tagCount_overall['copyCount'].astype(float)
df_Q_tagCount_overall[((df_Q_tagCount_overall['copyCount']/df_Q_tagCount_overall['Count']) < 0.001) & (df_Q_tagCount_overall['Count'] > 10000.0)]

### Tag stack: Posts containing code



df_postQ['p_Tags'] = df_postQ['p_Tags'].str.replace('\>\<', '\',\'')
df_postQ['p_Tags'] = df_postQ['p_Tags'].str.replace('\<', '[\'')
df_postQ['p_Tags'] = df_postQ['p_Tags'].str.replace('\>', '\']')
df_postQ['p_Tags'] = df_postQ.p_Tags.apply(ast.literal_eval)
df_postQ_tags = df_postQ.p_Tags.apply(pd.Series).stack().reset_index(['Id'], name='tags')

df_postQ_tagsCount = df_postQ_tags.groupby('tags').size().sort_values(ascending=False)
df_postQ_tagsCount.to_csv(outputPath+"df_postQ_tagsCount.csv", index = True, header = True)

## Copies in accepted answers
## join with df_A

df_postQ = pd.read_csv(dataPathMapping+"postQ.csv", delimiter =",", index_col=0)#, dtype = {'p_CreationDate':datetime, 'p_OwnerUserId': int})
df_postQ = df_postQ.drop_duplicates()
df_postQ.isnull().sum()

df_postQ['p_OwnerUserId'] = df_postQ['p_OwnerUserId'].fillna(0)
df_postQ['p_OwnerUserId'] = df_postQ['p_OwnerUserId'].astype(int)
df_postQ['p_AcceptedAnswerId'] = df_postQ['p_AcceptedAnswerId'].fillna(0)
df_postQ['p_AcceptedAnswerId'] = df_postQ['p_AcceptedAnswerId'].astype(int)


df_acceptedA = df_postQ.join(df_A.set_index('PostId'), how='inner',on="p_AcceptedAnswerId", rsuffix="_A")

df_acceptedA.to_csv(outputPath+"df_acceptedA.csv", index = True, header = True)

df_acceptedA.groupby('p_Score_A').size()
df_acceptedA.groupby('innerRoot').size()

#######
## Post A users
#######

## People - number of copies
df_A_user_count = df_A_user.groupby('p_OwnerUserId').size().to_frame()
df_A_user_count.columns = ['userCopyCount']

## People who have a lot of copies
## Are they different from other people, what happened there
## Is account Id the Id used in owneruserId
df_A_user_count.groupby('userCopyCount').size().to_csv(outputPath+"df_A_userCopyCount.csv", index = True, header = True)
df_A_user = df_A_user.join(df_A_user_count, on='p_OwnerUserId')


## where the creation date < all the copies' creation date
df_A_user.groupby('us_Views').size().to_csv(outputPath+"df_A_userViewCount.csv", index = True, header = True)

## Do people repeat themselves

df_A_user.groupby('p_OwnerUserId').size().sort_values()
df_A_user.groupby(['md5','p_OwnerUserId']).size().sort_values(ascending=False)


#df_A_user[df_A_user['p_OwnerUserId'] == 1570000]
#df_A_user[df_A_user['md5'] == '0be87ba81dedc62f11bedd9fc2eb029a']



df_Q_user.groupby(['md5','p_OwnerUserId']).size().sort_values(ascending=False)

#df_Q_user[df_Q_user['p_OwnerUserId'] == 2852424]
#df_Q_user[df_Q_user['md5'] == '99540499f0917cb874228bc9ee0eed58']

df_Q_tags = df_Q_tags.set_index("Id")
df_Q_user_tag = df_Q_tags.join(df_Q_user)
df_Q_user_tag = df_Q_user_tag.groupby(['md5','p_OwnerUserId']).filter(lambda x: len(x)>1)
df_Q_user_tag.to_csv(outputPath+"df_Q_user_tag_sameUser.csv", index = True, header = True)

df_Q_user_tag.groupby(['tags']).size().sort_values(ascending=False).to_csv(outputPath+"df_Q_user_tag_sameUser_group.csv", index = True, header = True)

######
## Post A copies the parent Post Q
######


#df_A[df_A['innerRoot'] == 0].sort_values('p_CreationDate',ascending=False).groupby('md5').head(1).shape
#df_A[df_A['innerRoot'] == 1].shape

df_A_copy_parentQ = pd.merge(df_A, df_Q, how="inner", left_on=['p_ParentId', 'md5'], right_on=['PostId', 'md5'])


######
## Post Q copies previous Post A
######

df_Q_copy_A = pd.merge(df_A, df_Q, how="inner", left_on=['md5'], right_on=['md5'])
df_Q_copy_A['p_CreationDate_y'] = pd.to_datetime(df_Q_copy_prevA['p_CreationDate_y'])
df_Q_copy_prevA = df_Q_copy_A[df_Q_copy_A['p_CreationDate_x'] < df_Q_copy_A['p_CreationDate_y']]
df_Q_copy_prevA['PostId_y'].nunique()
df_Q_copy_prevA_root = df_Q_copy_prevA.groupby('PostId_y').agg({"innerRoot_x": 'sum'})


df_Q_copy_prevA_root[df_Q_copy_prevA_root['innerRoot_x'] == 6]
df_Q_copy_prevA_root.groupby('innerRoot_x').size()



######
## Distribution on root vs non-root
######




df_A_user_rep = df_A_user.groupby('p_OwnerUserId').agg({'innerRoot': 'sum', 'us_Reputation': 'min'})

df_A_user_rep.groupby('innerRoot').size()


innerRoot_A_rep = df_A_user_rep[df_A_user_rep['innerRoot'] >= 1].us_Reputation.values

innerCopy_A_rep = df_A_user_rep[df_A_user_rep['innerRoot'] == 0].us_Reputation.values

## Are they in normal disttributions?
scipy.stats.kstest(innerRoot_A_rep, 'norm')
scipy.stats.kstest(innerCopy_A_rep, 'norm')




import scipy.stats
innerRoot_A_score = df_A[df_A['innerRoot'] == 1].p_Score.values

innerCopy_A_score = df_A[df_A['innerRoot'] == 0].p_Score.values

## Are they in normal disttributions?
scipy.stats.kstest(innerRoot_A_score, 'norm')
scipy.stats.kstest(innerCopy_A_score, 'norm')
## No
# This assumes continious distribution
#scipy.stats.ks_2samp(innerCopy_A_score, innerRoot_A_score)
scipy.stats.mannwhitneyu(innerCopy_A_score, innerRoot_A_score)

df_A[df_A['innerRoot'] == 1].groupby('p_Score').size()
df_A[df_A['innerRoot'] == 0].groupby('p_Score').size()

## pairwise
df_posts = df_A[['md5','PostId','p_CreationDate', 'p_Score', 'innerRoot']].append(df_Q[['md5','PostId','p_CreationDate', 'p_Score', 'innerRoot']])

df_posts['p_CreationDate'] = pd.to_datetime(df_posts['p_CreationDate'])


innerCopy_score = df_posts[df_posts['innerRoot'] == 0].sort_values('p_CreationDate',ascending=False).groupby('md5').head(1).sort_values('md5').p_Score.values

innerRoot_score = df_posts[df_posts['innerRoot'] == 1].sort_values('md5').p_Score.values

scipy.stats.wilcoxon(innerRoot_score, innerCopy_score)
scipy.stats.wilcoxon(innerRoot_score[:50], innerCopy_score[:50])
scipy.stats.wilcoxon(innerRoot_score[::-1][:50], innerCopy_score[::-1][:50])

scipy.stats.ranksums(innerRoot_score, innerCopy_score)



df_A.sort_values('p_CreationDate',ascending=False).groupby('md5').head(1)

df_Q[df_Q['innerRoot'] == 1].groupby('p_ViewCount').size()
df_Q[df_Q['innerRoot'] == 0].groupby('p_ViewCount').size()







