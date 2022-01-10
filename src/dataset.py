#!/usr/bin/env python3

import csv
import hashlib
import json
import os
import re
import requests
import sys
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

def create_dataset(dataset_profile, trial_encoding=None):

    id = dataset_profile['id']
    name = dataset_profile['name']
    url = dataset_profile['url']

    content = {
        'meta': {
            'id': id,
            'url': url,
            'name': name
        },
        'data': {}
    }

    res = None
    try:
        res = requests.get(url)
    except requests.exceptions.RequestException as e:
        print(f'Failed to fetch {url}', file=sys.stderr)
        return content
    except Exception as e:
        print(f'Failed to fetch {url}', file=sys.stderr)
        return content

    if res.status_code >= 400:
        print(f'Failed to fetch {url}. Status code={res.status_code}', file=sys.stderr)
        return content

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
            print(f'Failed to decode {url}.', file=sys.stderr)
            print(f'  tried encoding: {enc}. Reason={reason}', file=sys.stderr)
            print(f'  requests.encoding: {res.encoding}', file=sys.stderr)
            print(f'  requests.apparent_encoding: {res.apparent_encoding}', file=sys.stderr)
            return content
        else:
            reason2 = None
            enc2 = trial_encoding
            try:
                text = res.content.decode(enc2)
            except Exception as e:
                reason2 = e
                print(f'Failed to decode {url}.', file=sys.stderr)
                print(f'  tried encoding: {enc}. Reason={reason}', file=sys.stderr)
                print(f'  tried encoding: {enc2}. Reason={reason2}', file=sys.stderr)
                print(f'  requests.encoding: {res.encoding}', file=sys.stderr)
                print(f'  requests.apparent_encoding: {res.apparent_encoding}', file=sys.stderr)
                return content

    rows = csv.reader(text.splitlines())
    data = {}
    lno = 0
    for row in rows:
        cno = 0
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

    print(f'Read {lno} data from dataset in {url}.')

    content['data'] = data

    return content

def generate_dataset_name(url):
    parsed = urlparse(url)
    netloc_hierarchy = parsed.netloc.split('.')
    netloc_hierarchy.reverse()
    path_hierarchy = parsed.path.split('/')
    path_hierarchy.pop(0)
    path_hierarchy[-1] = re.sub('.csv$', '', path_hierarchy[-1])
    name = '.'.join(netloc_hierarchy) + '.' + '.'.join(path_hierarchy)
    return name

def create_dataset_profile_list(url):

    res = None
    try:
        res = requests.get(url)
    except requests.exceptions.RequestException as e:
        print(f'Failed to fetch {url}', file=sys.stderr)
        return None
    except Exception as e:
        print(f'Failed to fetch {url}', file=sys.stderr)
        return None

    if res.status_code >= 400:
        print(f'Failed to fetch {url}. Status code={res.status_code}', file=sys.stderr)
        return None

    url_object = urlparse(url)
    url_netloc = url_object.netloc.lower()

    dataset_profile_list = []

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
                    dataset_url = ref_full
                    dataset_name = generate_dataset_name(dataset_url)
                    dataset_id = hashlib.md5(dataset_name.encode()).hexdigest()
                    dataset_profile_list.append({
                        'id': dataset_id,
                        'name': dataset_name,
                        'url': dataset_url
                    })
                    print(f'Found {dataset_name}', file=sys.stderr)

    print(f'Generated {len(dataset_profile_list)} dataset set.', file=sys.stderr)

    return dataset_profile_list

def generate(url, dir=None, trial_encoding=None):

    dataset_profile_list = create_dataset_profile_list(url)
    ds_all = {}
    for dsp in dataset_profile_list:
        ds = create_dataset(dsp, trial_encoding=trial_encoding)
        ds_all.update({
            dsp['id']: ds
        })

    if dir == None:
        dir = ''
    else:
        os.makedirs(dir, mode=0o777, exist_ok=True)
    dump = json.dumps(ds_all, ensure_ascii=False, indent=1)
    fpath = os.path.join(dir, 'datasets.json')
    with open(fpath, 'w') as fd:
        fd.write(dump)

if __name__ == '__main__':

    def usage():
        print('parameters: method url ...', file=sys.stderr)
        print('  method: create_dataset', file=sys.stderr)

    if len(sys.argv) < 3:
        usage()
        exit(1)

    method = sys.argv[1]
    url = sys.argv[2]
    if method == 'generate':
        if len(sys.argv) > 3:
            dir = sys.argv[3]
        else:
            dir = None
        generate(url, dir=dir, trial_encoding='cp932')
    else:
        usage()
        exit(1)
