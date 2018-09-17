import numpy as np
import pandas as pd
import glob
import csv
import sys
import os

in_dir = '/home/jporcaro/cpol_analysis/cpol_out_allyears1/'
allFiles = glob.glob(in_dir + "*.csv")
df = pd.DataFrame()
mylist = []
out_dir = '/home/jporcaro/cpol_analysis/cpol_out_allyears1_df/'

for i in allFiles:
    try:
        df = pd.read_csv(i, index_col=None, header=0)
        mylist.append(df)
    except:
        continue

fn = 'allyears_df.csv'
frame = pd.concat(mylist)
frame.to_csv(fn)
os.path.join(out_dir, fn)
print('task complete')

