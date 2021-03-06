#!/usr/bin/env python3

import csv
import errno
import io
import json
import os
import re
import sys
from bs4 import BeautifulSoup
from importlib import import_module
from itertools import zip_longest

def columnexp2number(c):
    if len(c) == 1:
        n0 = int(ord(c.upper()) - 0x41)
        if n0 < 0 or n0 >= 26:
            return None
        else:
            return n0
    elif len(c) == 2:
        n0 = int(ord(c[0].upper()) - 0x41)
        n1 = int(ord(c[1].upper()) - 0x41)
        if n0 < 0 or n0 >= 26 or n1 < 0 or n1 >= 26:
            return None
        else:
            return (n0 + 1) * 26 + n1
    else:
        return None

def columnnumber2exp(n):
    if n >= 26:
        return str(chr(int(n // 26) + 0x40)) + str(chr(int(n % 26) + 0x41))
    elif n >= 0:
        return chr(n + 0x41)
    else:
        return None

def columnvalue2columnstruct(x):
    m = re.match('^(.+)\(([A-Za-z]+)\)$', x)
    if m:
        return { 'n': columnexp2number(m.group(1)), 't': m.group(2).lower() }
    else:
        return { 'n': columnexp2number(x), 't': None }

def encode_record_id(dataset_id, number):
    return f'{dataset_id}-{number:08}'

def decode_record_id(record_id):
    k = record_id.split('-')
    return k[0], k[1]

def create_dataset(csv_name, csv_path, prefix=None, encoding=None):

    with open(csv_path, encoding=encoding) as fd:
        csv_text = fd.read()

    csv_filename = os.path.basename(csv_path)
    csv_basename = os.path.splitext(csv_filename)[0]

    if prefix is None:
        prefix = csv_basename
        prefix = re.sub('-', '_', prefix)

    meta = {
        'id': prefix,
        'name': csv_name if csv_name is not None else csv_basename,
        'path': csv_path,
        'filename': csv_filename,
        'basename': csv_basename
    }

    rows = csv.reader(csv_text.splitlines())
    data = {}
    lno = 1
    for row in rows:
        record = []
        for column in row:
            record.append(column)
        data.update({encode_record_id(prefix, lno): record})
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

def get_number_list(number_list_exp):

    number_list = []

    number_word_list = number_list_exp.split(',')
    for number_word in number_word_list:
        number = number_word.strip().split('-')
        try:
            if len(number) > 2:
                return None
            elif len(number) == 2:
                min = int(number[0])
                max = int(number[1]) + 1
                number_list.extend(range(min, max))
            else:
                number_list.append(int(number[0]))
        except:
            return None

    return list(set(number_list))

def get_record_ids(ds, headers):

    record_ids = []
    if headers is None:
        record_ids = sorted(list(ds['data'].keys()))
    else:
        header_line_list = get_number_list(headers)
        if header_line_list is None:
            return None
        for header_line_number in header_line_list:
            record_ids.append(encode_record_id(ds['meta']['id'], header_line_number))
        record_ids = sorted(record_ids)

    return record_ids

def detect_header_columns(record, hint_values):

    hints = hint_values.split(',')
    detecteds = []
    for h in hints:
        if h[0] == '*':
            c = columnvalue2columnstruct(h[1:])
            if c['n'] >= len(record):
                return None
            else:
                detecteds.append(c['n'])
        else:
            for cno in range(0, len(record)):
                if re.search(h, record[cno]):
                    detecteds.append(cno)
                    break
            else:
                return None
    else:
        return detecteds

def get_hints(hint):

    hint_headers = None
    hint_values = None
    if hint is not None:
        hint_exp_list = hint.split(':', 1)
        if len(hint_exp_list) == 1:
            if len(hint_exp_list[0]) > 0:
                hint_values = hint_exp_list[0]
        else:
            if len(hint_exp_list[0]) > 0:
                hint_headers = hint_exp_list[0]
            if len(hint_exp_list[1]) > 0:
                hint_values = hint_exp_list[1]

    return hint_headers, hint_values

def get_header_line(ds, record_id, column_numbers):

    record = ds['data'].get(record_id)
    _, lno = decode_record_id(record_id)

    line_number = str(int(lno))
    line = {
        'filename': ds['meta']['filename'],
        'line_number': line_number,
        'columns': [],
        'items': [ ds['meta']['filename'], line_number ]
    }
    for cno in column_numbers:
        line['columns'].append(cno)
        line['items'].append(f'{columnnumber2exp(cno)}:{record[cno]}')
    return line

def detect(csv_paths, encoding=None, prefix=None, hint=None):

    if csv_paths is None or len(csv_paths) == 0:
        def analyze(line):
            v = line.split(' ', 1)
            if len(v) >=2:
                return { 'name': v[0].strip(), 'path': v[1].strip() }
            elif len(v) == 1:
                return { 'name': None, 'path': v[0].strip() }
            else:
                return None
        csv_lines = sys.stdin.readlines()
        csv_objects = list(map(analyze, csv_lines))
    else:
        csv_objects = list(map(lambda x: { 'name': None, 'path': x }, csv_paths))

    collection = []
    for csv_object in csv_objects:

        if csv_object['path'] is None:
            contine

        ds = create_dataset(csv_object['name'], csv_object['path'], prefix=prefix, encoding=encoding)
        ds = remove_invalid_records(ds)

        collected = {
            'dataset': ds,
            'header': None,
            'selection': None,
            'status': 0
        }

        hint_headers, hint_values = get_hints(hint)

        detected = None
        record_ids = get_record_ids(ds, hint_headers)
        if record_ids is None:
            filename = ds['meta']['filename']
            print(f'{filename}: Invalid expression in line numbers', file=sys.stderr)
            collected['status'] = errno.EINVAL
            collection.append(collected)
        else:
            for record_id in record_ids:
                record = ds['data'].get(record_id)
                _, line_number = decode_record_id(record_id)

                if record == None:
                    filename = ds['meta']['filename']
                    print(f'{filename}#{int(line_number)}: No such a record', file=sys.stderr)
                else:
                    if hint_values is not None:
                        header_columns = detect_header_columns(record, hint_values)
                        if header_columns is not None:
                            collected['header'] = get_header_line(ds, record_id, header_columns)
                            collected['status'] = 0
                            collection.append(collected)
                            break
            else:
                if hint_values is not None:
                    filename = ds['meta']['filename']
                    print(f'{filename}: No records like hints', file=sys.stderr)
                    collected['status'] = errno.EINVAL
                collection.append(collected)

    return collection

def get_post_process(args):

    if __package__ is None:
        package = ''
    else:
        package = f'{__package__}'

    if args.post_process is not None:
        name = args.post_process[0]
    else:
        name = 'print'

    module = import_module(f'.o_{name}', f'{package}.modules')
    post_process = module.PostProcess(args)
    return post_process

def main():

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
    parser.add_argument('path', nargs='*', metavar='CSVPATH', help='open data csv path')
    parser.add_argument('-d', '--delimiter', nargs=1, default=',', help='delimiter')
    parser.add_argument('--encoding', nargs=1, metavar='CODEPAGE', help='input encoding')
    parser.add_argument('--hint', nargs=1, metavar='HINTS', help='header record hint as \'RANGE:VALUES\',eg. \'1-5:*A,[Nn]ame\'')
    parser.add_argument('--csv', action='store_true', help='csv output')
    parser.add_argument('--post-process', nargs=1, metavar='module', help='call post process module')
    parser.add_argument('--post-process-args', nargs='*', metavar='NAME=VALUE', help='post process module arguments')

    if len(sys.argv) == 1:
        print(parser.format_usage(), file=sys.stderr)
        exit(1)

    args = parser.parse_args()

    csv_path = args.path if args.path is not None else None
    encoding = args.encoding[0] if args.encoding is not None else None
    hint = args.hint[0] if args.hint is not None else None
    
    collection = detect(csv_path, encoding=encoding, hint=hint)
    post_process = get_post_process(args)
    return post_process.detected(collection)

if __name__ == '__main__':
    exit(main())
