import multiprocessing as mp
import os.path
import shutil
import urllib.request

import pandas as pd
from tqdm import tqdm

PROCESS = 32

def file2url(txt_pth:str, directory:str):
    with open(txt_pth, 'r') as fh:
        for info in tqdm(fh.readlines()):
            info = info.strip().split('#####')

            if len(info) > 4:
                id, des, url1, url2, labels = info[0], info[1], info[2], info[3], info[4]
            else:
                id, des, url1, url2, labels = info[0], info[1], info[2], info[3], None

            sub_dir = url1.split('/')[4]
            if len(sub_dir) != 2:
                sub_dir = 'extra'
                for info in url1.split('/'):
                    if len(info) == 2 and info[0] == 'i':
                        sub_dir = info
                        break
    
            if not os.path.exists(os.path.join(directory, sub_dir)):
                os.mkdir(os.path.join(directory, sub_dir))

            pth = os.path.join(directory, sub_dir, f'{id}.jpg')

            yield url1, url2, pth, des, labels


def url2file(args):
    url1, url2, pth, des, labels = args
    try:
        urllib.request.urlretrieve(url1, pth)
        return pth, des, labels
    except:
        try:
            urllib.request.urlretrieve(url2, pth)
            return pth, des, labels
        except:
            return None


if __name__ == '__main__':
    
    
    flst = ['product1m_dev_ossurl_v2.txt', 'product1m_gallery_ossurl_v2.txt', 'product1m_test_ossurl_v2.txt', 'product1m_train_ossurl_v2.txt']

    # mp pool
    pool = mp.Pool(PROCESS)


    for f in flst:
        name = f.split('_')[1]
        directory = os.path.join('data', name)
        
        if os.path.exists(directory):
            shutil.rmtree(directory)
        os.mkdir(directory)

        # download
        PATHS = []
        CAPTIONS = []
        LABELS = []

        print(f, directory)
        for rst in pool.imap(url2file, file2url(f, directory)):
            if rst is not None:
                pth, des, labels = rst
                PATHS.append(pth)
                CAPTIONS.append(des)
                if labels is not None: LABELS.append(labels) 
    
        # save meta data
        if len(LABELS) > 0:
            df = pd.DataFrame({'path': PATHS, 'captions': CAPTIONS, 'labels': LABELS})
        else:
            df = pd.DataFrame({'path': PATHS, 'captions': CAPTIONS})
        df.to_csv(f'meta/{name}.csv', index=None)
