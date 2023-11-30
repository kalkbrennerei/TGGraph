import pandas as pd
import numpy as np

def clean_filter2(x):
    if x.startswith('t.me/'):
        x = x[5:]
    elif x.startswith('telegram.me/'):
        x = x[12:]
    if x.startswith('s/'):
        x = x[2:]
    return x

def get_dst(x):
    l = x.split('/')
    dst = np.nan
    if len(l) <= 2:
        dst = l[0].split('?')[0]
    return dst

def get_note(x):
    l = x.split('/')
    note = np.nan
    if len(l) == 2:
        try: 
            note = int(l[1].split('?')[0])
        except:
            pass
    return note

def main():
    # Load data
    df = pd.read_csv(data_path)
    # Filter urls to keep only t.me and telegram.me
    tme = df[(df['url_filter3']=='t.me') | (df['url_filter3']=='telegram.me')]
    # Drop columns
    tme = tme.drop(['url', 'url_filter1', 'url_filter3', 'url_filter4', 'url_filter5', 'extension', 'domain', 'date', 'is_forwarded', 'forwarded_from_id', 'forwarded_message_date'], axis=1)
    # Drop t.me and telegram.me from url_filter2
    tme = tme[(tme['url_filter2']!='t.me') & (tme['url_filter2']!='telegram.me')]
    # Drop joinchat
    tme = tme[~tme['url_filter2'].str.startswith('telegram.me/joinchat/')]
    tme = tme[~tme['url_filter2'].str.startswith('t.me/joinchat/')]
    # Drop private tme links 
    tme = tme[~tme['url_filter2'].str.startswith('t.me/+')]
    tme = tme[~tme['url_filter2'].str.startswith('telegram.me/+')]
    # Drop t.me/iv?url=
    tme = tme[~tme['url_filter2'].str.startswith('t.me/iv?url=')]
    tme = tme[~tme['url_filter2'].str.startswith('telegram.me/iv?url=')]
    # clean url_filter2
    tme['url_filter2'] = tme['url_filter2'].apply(clean_filter2)
    # compute dst and note
    tme['dst'] = tme['url_filter2'].apply(get_dst)
    tme['note'] = tme['url_filter2'].apply(get_note)
    # Drop url_filter2
    tme = tme.drop(['url_filter2'], axis=1)
    # Drop rows with nan dst
    tme = tme[~tme['dst'].isna()]
    # Rename columns and save
    tme = tme.rename(columns={'username':'src'})
    # Drop self loops
    tme = tme[tme['src']!=tme['dst']]
    # Save
    tme.to_csv('tme.csv', index=False)
    return

if __name__ == '__main__':
    data_path = '../../00_Data/urls_data.csv'
    main()
