import pandas as pd
import db_utilities as dbutil
import links_utilities as linkutil
from multiprocessing import Pool
from tqdm import tqdm
import numpy as np
import csv

def foo(args):
    ids, result_csv, tlds, extensions = args
    with open(result_csv, 'a') as f:
        for ch_id in ids:
            ch_info = dbutil.get_channel_by_id(ch_id, db_name, with_text_msgs=True, with_media_msgs=False)
            msg_df = pd.DataFrame.from_dict(ch_info['text_messages'], orient='index').reset_index().rename(columns={'index':'msg_id'})
            if msg_df.empty:
                continue
            msg_df['ch_id'] = ch_id
            #msg_df['date'] = pd.to_datetime(msg_df['date'], unit='s')
            msg_df['urls'] = msg_df['message'].apply(linkutil.get_links, args=(tlds,))
            msg_df.fillna(np.nan, inplace=True) 
            writer = csv.writer(f)
            for _, row in msg_df.iterrows():
                for url in row.urls:
                    url_extension = linkutil.get_extension(url, extensions)
                    url_filter1 = linkutil.filter1(url)
                    url_filter2 = linkutil.filter2(url_filter1)
                    url_filter3 = linkutil.filter3(url_filter2)
                    url_filter4 = linkutil.filter4(url_filter3)
                    url_filter5 = linkutil.filter5(url_filter4)
                    url_domain = url_filter3.split('.')[-1]
                    writer.writerow([row.ch_id, row.msg_id, ch_info['username'], row.date, row.is_forwarded, row.forwarded_from_id, row.forwarded_message_date, url, url_filter1, url_filter2, url_filter3, url_filter4, url_filter5, url_extension, url_domain])
    return

def main(db_name, chunksize, n_cores, result_csv, tlds_path, extentions_path):
    extensions = pd.read_csv(extentions_path)['extension'].to_list()
    extensions = sorted(extensions, key=len, reverse=True)
    ids = dbutil.get_channel_ids(db_name)
    tlds = pd.read_csv(tlds_path, header=None)[0].tolist()
    l = [(ids[i:i + chunksize], result_csv, tlds, extensions) for i in range(0, len(ids), chunksize)]
    with open(result_csv, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['ch_id', 'msg_id', 'username', 'date', 'is_forwarded', 'forwarded_from_id', 'forwarded_message_date', 'url', 'url_filter1', 'url_filter2', 'url_filter3', 'url_filter4', 'url_filter5', 'extension', 'domain'])
    with Pool(n_cores) as p:
        for _ in tqdm(p.imap(foo, l), total=len(l)):
            pass
    return

if __name__ == '__main__':
    db_name = 'Telegram_test'
    chunksize = 100
    n_cores = 32
    result_csv = 'urls_data.csv'
    tlds_path = 'tlds.csv'
    extentions_path = 'extensions.csv'
    main(db_name, chunksize, n_cores, result_csv, tlds_path, extentions_path)
