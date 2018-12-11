import numpy as np
import pandas as pd
from datetime import datetime

dataPath = "hashOutput/"
dataPathMapping = "posts/"
outputPath = "hashOutput/"

df_ids = pd.read_csv(dataPath+"codeGroupsIdsStack.csv", delimiter =",", dtype = {'id': int})
df_ids.columns = ['idx', 'md5', 'Id', 'count']
df_ids = df_ids.set_index('Id')

#################
# Mapping from block Id to post Id
#################
df_mapping = pd.read_csv(dataPathMapping+"id_mapping.csv", delimiter =",", index_col=0)
## ['idx', 'md5', 'PostId'], index = blockId
df = df_ids.join(df_mapping)


df.to_csv(outputPath+"codeGroupsIdsMapped.csv", index = True, header = True)

df['Id'] = df.index

## Remove copies in the same post
df_samePost = df.groupby(['md5', 'PostId', 'count'])['Id'].apply(list)
df_samePost = df_samePost.to_frame()
df_samePost = df_samePost.loc[df_samePost.Id.map(len) > 1]

df_samePost['BlockCount'] = df_samePost['Id'].str.len()
#df.join(df['Id'].str.len().to_frame(), rsuffix = "Count")

## Release multiindex
df_samePost.reset_index(inplace = True)
df_samePost = df_samePost.loc[(df_samePost['BlockCount'] == df_samePost['count'])]

df_samePost = df_samePost.drop(['BlockCount', 'Id'], axis = 1)

df_samePost = pd.merge(df, df_samePost, how = 'inner', left_on = ['md5', 'PostId', 'count'], right_on = ['md5', 'PostId', 'count'])

df_samePost.index = df_samePost['Id']

## Denoise
df = pd.concat([df, df_samePost], axis=0).drop_duplicates(keep=False)

##pd.concat([df, df_samePost], axis=0).drop_duplicates(keep=False).join(df_samePost, how = 'inner', rsuffix = "_r")

#################
# Mapping from exact copies to answer post features
##################
df_postA = pd.read_csv(dataPathMapping+"postA.csv", delimiter =",", index_col=0)#, dtype = {'p_CreationDate':datetime, 'p_OwnerUserId': int})
df_postA = df_postA.drop_duplicates()
df_postA.isnull().sum()

df_postA = df_postA.drop(['p_ViewCount', 'p_FavoriteCount', 'p_Tags'], axis=1)
df_postA['p_OwnerUserId'] = df_postA['p_OwnerUserId'].fillna(0)
df_postA['p_OwnerUserId'] = df_postA['p_OwnerUserId'].astype(int)

## Answer posts containing exact match blocks
df_A = df.join(df_postA, how="inner", on="PostId")

#df_postA["p_Id"] = df_postA.index
df_postA_Q = df_postA.set_index('p_ParentId')

df_A_parents = df.join(df_postA_Q, how="inner", on="PostId")
## Only these are meaningful
df_A_parents = df_A_parents.drop(['p_CreationDate','p_OwnerUserId','p_Score','p_CommentCount'], axis=1)
df_A_parents['Id'] = df_A_parents.index

## Question posts containing exact match blocks, while their answer posts also...
## Count: how many answer posts contain exact mach blocks
df_A_parents = df_A_parents.groupby(df_A_parents.columns.tolist(), as_index=False).size().reset_index(name="counts").sort_values(by=['md5','idx'])

## ['idx', 'md5', 'PostId', 'p_CreationDate', 'p_OwnerUserId', 'p_Score', 'p_CommentCount', 'p_ParentId'], index = blockId
df_A.to_csv(outputPath+"df_A.csv", index = True, header = True)
## ['idx', 'md5', 'PostId', 'Id', 'counts']
df_A_parents.to_csv(outputPath+"df_A_parents.csv", index = False, header = True)

#################
# Mapping from exact copies to question post features
##################

df_postQ = pd.read_csv(dataPathMapping+"postQ.csv", delimiter =",", index_col=0)#, dtype = {'p_CreationDate':datetime, 'p_OwnerUserId': int})
df_postQ = df_postQ.drop_duplicates()
df_postQ.isnull().sum()

df_postQ['p_OwnerUserId'] = df_postQ['p_OwnerUserId'].fillna(0)
df_postQ['p_OwnerUserId'] = df_postQ['p_OwnerUserId'].astype(int)
df_postQ['p_AcceptedAnswerId'] = df_postQ['p_AcceptedAnswerId'].fillna(0)
df_postQ['p_AcceptedAnswerId'] = df_postQ['p_AcceptedAnswerId'].astype(int)
#df_postQ['p_FavoriteCount'] = df_postQ['p_FavoriteCount'].fillna(0)
#df_postQ['p_FavoriteCount'] = df_postQ['p_FavoriteCount'].astype(int)

## Answer posts containing exact match blocks
df_Q = df.join(df_postQ, how="inner", on="PostId")
df_Q.isnull().sum()

df_Q[df_Q.p_AcceptedAnswerId != 0].shape


df_Q.to_csv(outputPath+"df_Q.csv", index = True, header = True)

#################
# Add user info to answer post dataframe
##################
"""
df_user = pd.read_table(dataPathMapping+"Users000000000000.csv" , delimiter =",")
for idx in range(1, 4):
    df_append = pd.read_table(dataPathMapping+"Users00000000000%s.csv" %idx, delimiter =",")
    df_user = df_user.append(df_append)
    
df_user.isnull().sum()


df_user = df_user.drop(['Age','WebsiteUrl', 'Location', 'ProfileImageUrl', 'AboutMe', 'EmailHash'], axis=1)


df_user = df_user.set_index('AccountId')
df_A.join(df_user, how="inner", on="p_OwnerUserId")
"""

df_user = pd.read_csv(dataPathMapping+"userInfo.csv", delimiter =",", index_col=0)
df_user = df_user.drop_duplicates()
df_user.isnull().sum()

df_user = df_user.drop(['us_Age','us_Id'], axis=1)
df_user = df_user.set_index('us_AccountId')

#df_A.join(df_user, how="inner", on="p_OwnerUserId")

df_A[df_A.p_OwnerUserId == 0].shape

## Subset of df_A, only use for user analysis
df_A_user = df_A.join(df_user, how="inner", on="p_OwnerUserId")

df_A_user.to_csv(outputPath+"df_A_user.csv", index = True, header = True)

#################
# Add user info to question post dataframe
##################


df_Q[df_Q.p_OwnerUserId == 0].shape

## Subset of df_A, only use for user analysis
df_Q_user = df_Q.join(df_user, how="inner", on="p_OwnerUserId")

df_Q_user.to_csv(outputPath+"df_Q_user.csv", index = True, header = True)

## group by p_OwnerUserId, who appears frequently






