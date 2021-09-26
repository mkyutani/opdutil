#!/usr/bin/env python3

import csv
import json
import os
import re
import requests
import sys
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

def csv2json(url, has_header=False, trial_encoding=None):

    res = None
    try:
        res = requests.get(url)
    except requests.exceptions.RequestException as e:
        print(f'failed to fetch {url}', file=sys.stderr)
        return None
    except Exception as e:
        print(f'failed to fetch {url}', file=sys.stderr)
        return None

    if res.status_code >= 400:
        print(f'failed to fetch {url}. Status code={res.status_code}', file=sys.stderr)
        return None

    reason = None
    text = None
    if res.encoding != 'ISO-8859-1':
        enc = res.encoding
    else:
        enc = res.apparent_encoding
    if enc is None or enc.lower() == 'shift_jis':
        enc = 'cp932'
    try:
        text = res.content.decode(enc)
    except Exception as e:
        reason = e

    if text is None:
        if trial_encoding is None:
            print(f'failed to decode {url}.', file=sys.stderr)
            print(f'  tried encoding: {enc}. Reason={reason}', file=sys.stderr)
            print(f'  requests.encoding: {res.encoding}', file=sys.stderr)
            print(f'  requests.apparent_encoding: {res.apparent_encoding}', file=sys.stderr)
            return None
        else:
            reason2 = None
            enc2 = trial_encoding
            try:
                text = res.content.decode(enc2)
            except Exception as e:
                reason2 = e
                print(f'failed to decode {url}.', file=sys.stderr)
                print(f'  tried encoding: {enc}. Reason={reason}', file=sys.stderr)
                print(f'  tried encoding: {enc2}. Reason={reason2}', file=sys.stderr)
                print(f'  requests.encoding: {res.encoding}', file=sys.stderr)
                print(f'  requests.apparent_encoding: {res.apparent_encoding}', file=sys.stderr)
                return None

    rows = csv.reader(text.splitlines())
    meta = {}
    data = {}
    if has_header:
        lno = -1
    else:
        lno = 0
    for row in rows:
        cno = 0
        if lno < 0:
            header = {}
            for column in row:
                header[str(cno)] = column
                cno = cno + 1
            meta['header'] = header
        else:
            entity = {}
            empty = True
            for column in row:
                entity[str(cno)] = column
                if len(column) > 0:
                    empty = False
                cno = cno + 1
            if empty is False:
                data[str(lno)] = entity
        lno = lno + 1
    content = {
        'meta': meta,
        'data': data
    }

    return json.dumps(content, ensure_ascii=False, indent=1)
                
def list_csvs(url):

    res = None
    try:
        res = requests.get(url)
    except requests.exceptions.RequestException as e:
        print(f'failed to fetch {url}', file=sys.stderr)
        return None
    except Exception as e:
        print(f'failed to fetch {url}', file=sys.stderr)
        return None

    if res.status_code >= 400:
        print(f'failed to fetch {url}. Status code={res.status_code}', file=sys.stderr)
        return None

    url_object = urlparse(url)
    url_netloc = url_object.netloc.lower()

    selected = []

    bs = BeautifulSoup(res.content, 'html.parser')
    for t in bs.find_all('a'):
        ref = t.get('href')
        if ref is not None:
            ref = ''.join(filter(lambda c: c >= ' ', ref))
            ref = re.sub('<.*?>', '', ref)
            ref = ref.strip()

            ref_full = urljoin(url, ref)
            ref_object = urlparse(ref_full)

            scheme = ref_object.scheme.lower()
            netloc = ref_object.netloc.lower()
            path = ref_object.path.lower()
            if scheme == 'http' or scheme == 'https':
                if netloc == url_netloc and path.endswith('.csv'):
                    selected.append(ref_full)

    return selected

def create_dataset(url, dir=None, has_header=False, trial_encoding=None):

    if dir == None:
        dir = ''
    else:
        os.makedirs(dir, mode=0o777, exist_ok=True)
        if not dir.endswith('/'):
            dir = dir + '/'

    csvs = list_csvs(url)
    for c in csvs:
        json = csv2json(c, has_header=has_header, trial_encoding=trial_encoding)
        if json is not None:
            fn = re.sub('/', '--', urlparse(c).path)
            fn = re.sub('^--', '', fn)
            fn = re.sub('.csv$', '', fn)
            fn = fn + '.json'
            fpath = dir + fn
            with open(fpath, 'w') as fd:
                fd.write(json)

if __name__ == '__main__':

    def usage():
        print('parameters: method url ...', file=sys.stderr)
        print('  method: create_dataset, csv2json, list_csvs', file=sys.stderr)

    if len(sys.argv) < 3:
        usage()
        exit(1)

    method = sys.argv[1]
    url = sys.argv[2]
    if method == 'create_dataset':
        if len(sys.argv) > 3:
            dir = sys.argv[3]
        else:
            dir = None
        has_header = False
        if len(sys.argv) > 4 and sys.argv[4].lower() == 'true':
            has_header = True
        create_dataset(url, dir=dir, has_header=has_header, trial_encoding='cp932')
    elif method == 'csv2json':
        has_header = False
        if len(sys.argv) > 3 and sys.argv[3].lower() == 'true':
            has_header = True
        json = csv2json(url, has_header=has_header, trial_encoding='cp932')
        print(json)
    elif method == 'list_csvs':
        csvs = list_csvs(url)
        for c in csvs:
            print(c)
        print(len(csvs))
    else:
        usage()
        exit(1)
