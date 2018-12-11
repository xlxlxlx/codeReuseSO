import numpy as np
import pandas as pd

## Merge result tables

dataPath = "hashOutput/"
outputPath = "hashOutput/"

df_ids = pd.read_table(dataPath+"codeGroupsIds.csv", delimiter =",", names = ["md5","id"])
df_counts = pd.read_table(dataPath+"codeGroupsCounts.csv", delimiter =",", names = ["md5","count"])

#df_counts.dtypes
#df_counts["counts"] = pd.to_numeric(df_counts["counts"])

df_counts_agg = df_counts.groupby('md5').agg({"count": 'sum'})
df_counts_agg = df_counts_agg.loc[df_counts_agg['count'] > 1]

df_counts_agg.to_csv(outputPath+"codeGroupsCountsAgg.csv", index = True, header = True)


#df_ids = df_ids.groupby('md5').agg({"id": 'sum'})
#df_ids = df_ids.groupby('md5')['id'].apply(np.concatenate)
#df_ids = df_ids.groupby('md5').agg({"id": lambda x: ', '.join(x[1:-1])})
df_ids = df_ids.groupby('md5')["id"].sum()
df_ids_dup = df_counts_agg.join(df_ids)


df_ids_dup['id'] = df_ids_dup['id'].str.replace('\]\[', ', ')


df_ids_dup['id'] = df_ids_dup['id'].str.replace('\]', '')
df_ids_dup['id'] = df_ids_dup['id'].str.replace('\[', '')

df_ids_dup.to_csv(outputPath+"codeGroupsIdsAgg.csv", index = True, header = True)

df_ids_dup['md5'] = df_ids_dup.index
df_ids_dup['id'] = df_ids_dup.id.apply(ast.literal_eval)

df_ids_dup = df_ids_dup.set_index(['md5']).id.apply(pd.Series).stack().reset_index(['md5'], name='id')

df_ids_dup['id'] = df_ids_dup['id'].astype(int)

df_ids_dup = df_ids_dup.join(df_counts_agg, on="md5")

df_ids_dup.to_csv(outputPath+"codeGroupsIdsStack.csv", index = True, header = True)

#df_ids_dup.id_list.tolist()
