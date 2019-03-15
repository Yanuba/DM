import numpy as np
import pandas as pd

from collections import Counter

def read_csv(path, header = None, group = True, group_index = 0):
    # read csv into pandas dataframe, grouping by id

    df = pd.read_csv(path, header = header)
    if group is True:
        sensors = df.groupby(group_index)
        return [x for x in sensors]
    else:
        return df


def time_shift(df):
    # add column to dataframe with the time distance from the previous measurement 
    
    ts = df.iloc[:,1]

    ts1 = ts[1:].values - ts[0:-1].values
    ts1 = np.concatenate([[np.nan], ts1])
    ts1 = ts1/1000
    ts1 = ts1.round()
    
    ret = df.copy()
    ret[3] = ts1
    
    return ret


def windows_generator(df, delta = np.timedelta64(60,'m'), ts_index = 1):
    # generate windows of given time interval
    start = df.iloc[0,ts_index]\
        .astype('datetime64[ms]')\
        .astype('datetime64[h]')\
        .astype('datetime64[ms]')

    k = 0

    while k < len(df.values):
        
        window = []
        
        end = start + delta

        while k < len(df.values) and df.iloc[k,ts_index] < end.astype('int64'):
            item = df.iloc[k,:]
            window.append(item)
            k += 1
        
        yield start, end, pd.DataFrame(window)

        start = end

def get_transmission_frequencies(df, delta = np.timedelta64(1,'h')):
    
    df = time_shift(df)
    ret = {}
    
    gen = windows_generator(df, delta)

    for start, end, window in gen:
        if len(window) == 0:
            ret[(start, end)] = np.array([])
        else:
            ret[(start, end)] = window[3].values

    return ret

def plot_bar(window_vals, ax, label = ''):
    cnt = Counter(window_vals)
    x = []
    y = []
    for k in cnt:
        x.append(k)
        y.append(cnt[k])
    ax.bar(x, y, label = label)
    # count freq in interval
