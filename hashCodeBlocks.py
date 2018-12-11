import numpy as np
import pandas as pd
import hashlib

dataPath = "codeBlocks/"
outputPath = "hashOutput/"

## MD5 hash for each CSV file
for i in range(42):
    idx = format(i, "02d")
    
    df = pd.read_table(dataPath+"codeBlock_10_uniquePost-0000000000%s.csv" %idx, delimiter =",")
    df['md5'] = [hashlib.md5(val.encode('utf-8')).hexdigest() for val in df['Content']]

    df_groups_w_counts = df.groupby("md5").size().reset_index(name="counts")
    df_groups_w_ids = df.groupby("md5")['Id'].apply(list)

    df_groups_w_counts.to_csv(outputPath+"codeGroupsCounts.csv", index = False, header = False, mode = "a")
    df_groups_w_ids.to_csv(outputPath+"codeGroupsIds.csv", index = True, mode = "a")



