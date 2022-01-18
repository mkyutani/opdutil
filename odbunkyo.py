#!/usr/bin/env python3

import csv
import errno
import hashlib
import io
import json
import os
import re
import requests
import sys
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

def columnexp2number(c):
    return int(ord(c.upper()) - 0x41)

def columnnumber2exp(n):
    return chr(n + 0x41)

def columnvalue2columnstruct(x):
    m = re.match('^(.+)\(([A-Za-z]+)\)$', x)
    if m:
        return { 'n': columnexp2number(m.group(1)), 't': m.group(2).lower() }
    else:
        return { 'n': columnexp2number(x), 't': None }

def create_dataset(csv_path, prefix=None, encoding=None):

    if csv_path is None:
        csv_text = sys.stdin.read()
    else:
        with open(csv_path, encoding=encoding) as fd:
            csv_text = fd.read()

    if prefix is None:
        prefix = hashlib.md5(csv_path.encode()).hexdigest()
    prefix = re.sub('-', '_', prefix)

    meta = {
        'id': prefix
    }

    rows = csv.reader(csv_text.splitlines())
    data = {}
    lno = 1
    for row in rows:
        record = []
        for column in row:
            record.append(column)
        data.update({f'{prefix}-{lno}': record})
        lno = lno + 1

    return {
        'meta': meta,
        'data': data
    }

def remove_invalid_records(ds):

    removing_keys = []
    for id, record in ds['data'].items():
        lno = id.split('-')[1]
        if len(record) == 0:
            print(f'CSV #{lno}: No columns', file=sys.stderr)
            removing_keys.append(id)
            continue

        if record[0].startswith('#'):
            print(f'CSV #{lno}: Comment record', file=sys.stderr)
            removing_keys.append(id)
            continue

        for column in record:
            if len(column) > 0:
                break
        else:
            print(f'CSV #{lno}: Empty record', file=sys.stderr)
            removing_keys.append(id)
            continue

    for id in removing_keys:
        ds['data'].pop(id)

    return ds

def select_columns(ds_old, column_list, strict=False):

    if column_list is None:
        return ds_old

    columns = list(map(columnvalue2columnstruct, column_list.split(',')))

    ds = {}
    ds['meta'] = ds_old['meta']
    ds['data'] = {}
    for id, record_old in ds_old['data'].items():
        len_old = len(record_old)
        lno = id.split('-')[1]
        record = []
        for c in columns:
            cn = c['n']
            ct = c['t']
            if cn > len_old:
                print(f'CSV #{lno}: No such a column {columnnumber2exp(cn)}', file=sys.stderr)
                break
            elif strict is True and len(record_old[cn]) == 0:
                print(f'CSV #{lno}: No content in column {columnnumber2exp(cn)}', file=sys.stderr)
                break
            elif ct is not None:
                try:
                    if ct == 'int' and int(record_old[cn]):
                        pass
                    elif ct == 'float' and float(record_old[cn]):
                        pass
                except ValueError:
                    print(f'CSV #{lno}: Unmatched type of column {columnnumber2exp(cn)}', file=sys.stderr)
                    break
            record.append(record_old[cn])
        else:
            ds['data'].update({id: record})
            continue

    return ds

def select(csv, prefix=None, column_list=None, encoding=None, output_format=None, strict=False, delimiter=','):

    ds = create_dataset(csv, prefix=prefix, encoding=encoding)
    ds = remove_invalid_records(ds)
    ds = select_columns(ds, column_list, strict)

    if output_format == 'csv':
        for id, record in ds['data'].items():
            print(delimiter.join([ds['meta']['id'], id] + record))
    else:
        dump = json.dumps(ds, ensure_ascii=False, indent=1)
        print(dump)

    return 0

def header(csv, line_number, encoding=None):

    ds = create_dataset(csv, prefix=None, encoding=encoding)
    dataset_id = ds['meta']['id']
    record = ds['data'].get(f'{dataset_id}-{line_number}')
    if record is None:
        print(f'CSV #{line_number}: No such a record', file=sys.stderr)
        return errno.ENOENT

    column = 0
    for c in record:
        print(f'{csv} {columnnumber2exp(column)} {c}')
        column = column + 1

    return 0

def generate_dataset_name(bs_tag, url):
    parent = bs_tag.parent
    if parent is not None:
        name = parent.get_text(strip=True)
        if name is not None:
            name = re.sub('[（(]Excelファイル.*CSVファイル.*[）)]', '', name)
            if len(name) > 0:
                return name

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
                    dataset_name = generate_dataset_name(t, ref_full)
                    dataset_profile_list.append({
                        'name': dataset_name,
                        'url': dataset_url
                    })

    return dataset_profile_list

def list_datasets(url, delimiter=',', dim_install_command=False):

    dataset_profile_list = create_dataset_profile_list(url)
    for dsp in dataset_profile_list:
        line = delimiter.join([dsp['url'], dsp['name']])
        print(line)

    return 0

if __name__ == '__main__':

    sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

    import argparse
    from argparse import HelpFormatter
    from operator import attrgetter
    class SortingHelpFormatter(HelpFormatter):
        def add_arguments(self, actions):
            actions = sorted(actions, key=attrgetter('option_strings'))
            super(SortingHelpFormatter, self).add_arguments(actions)

    parser = argparse.ArgumentParser(description='Open dataset utilty', formatter_class=SortingHelpFormatter)
    sps = parser.add_subparsers(dest='subparser_name', title='action arguments')
    sp_list = sps.add_parser('list', help='List links')
    sp_list.add_argument('url', nargs=1, metavar='URL', help='open data portal url')
    sp_list.add_argument('-d', '--delimiter', nargs=1, default=',', help='delimiter')
    sp_select = sps.add_parser('select', help='Select columns from csv file')
    sp_select.add_argument('path', nargs=1, metavar='CSVPATH', help='open data csv path')
    sp_select.add_argument('-d', '--delimiter', nargs=1, default=',', help='delimiter')
    sp_select.add_argument('--encoding', nargs=1, metavar='ENCODING', help='code page')
    sp_select.add_argument('--prefix', nargs=1, metavar='ENCODING', help='record id prefix')
    sp_select.add_argument('--columns', nargs=1, metavar='COLUMNS', help='select columns (A,B,...)')
    sp_select.add_argument('--strict', action='store_true', help='Not allow no content columns')
    sp_select.add_argument('--csv', action='store_true', help='csv output')
    sp_header = sps.add_parser('header', help='Print header')
    sp_header.add_argument('path', nargs=1, metavar='CSVPATH', help='open data csv path')
    sp_header.add_argument('--encoding', nargs=1, metavar='ENCODING', help='code page')
    sp_header.add_argument('-n', '--line-number', nargs=1, default='1', help='delimiter')

    if len(sys.argv) == 1:
        print(parser.format_usage(), file=sys.stderr)
        exit(1)

    args = parser.parse_args()
    method = args.subparser_name

    if method == 'list':
        ret = list_datasets(args.url[0], delimiter=args.delimiter[0])
    elif method == 'select':
        csv_path = None if args.path[0] == '-' else args.path[0]
        encoding = args.encoding[0] if args.encoding is not None else None
        if args.prefix is None:
            prefix = None
        elif args.prefix[0] == '-':
            prefix = os.path.splitext(os.path.basename(csv_path))[0]
        else:
            prefix = args.prefix[0]
        column_list = args.columns[0] if args.columns is not None else None
        output_format = 'csv' if args.csv is True else None
        strict = args.strict
        delimiter=args.delimiter[0]
        ret = select(csv_path, column_list=column_list, prefix=prefix, encoding=encoding, output_format=output_format, strict=strict, delimiter=delimiter)
    elif method == 'header':
        csv_path = None if args.path[0] == '-' else args.path[0]
        encoding = args.encoding[0] if args.encoding is not None else None
        line_number = args.line_number
        ret = header(csv_path, line_number[0], encoding=encoding)

    exit(ret)
