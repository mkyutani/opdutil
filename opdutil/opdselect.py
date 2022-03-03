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

from opdutil.opddetect import detect

def columnnumber2exp(n):
    if n >= 26:
        return str(chr(int(n // 26) + 0x40)) + str(chr(int(n % 26) + 0x41))
    elif n >= 0:
        return chr(n + 0x41)
    else:
        return None

def select_columns(ds_old, column_numbers, column_filter_list, strict=False):

    if column_filter_list is None:
        column_filter = []
    else:
        column_filter = column_filter_list.split(',')

    ds = {}
    ds['meta'] = ds_old['meta']
    ds['data'] = {}
    for id, record_old in ds_old['data'].items():
        len_old = len(record_old)
        lno = id.split('-')[1]
        record = []
        record_column_numbers = column_numbers if column_numbers is not None else range(0, len_old)
        for cno, ctype in zip_longest(record_column_numbers, column_filter):
            if cno is None:
                continue
            if ctype == '':
                ctype = None

            if cno > len_old:
                filename = ds_old['meta']['filename']
                print(f'{filename}#{lno}: No such a column {columnnumber2exp(cno)}', file=sys.stderr)
                break
            elif strict is True and len(record_old[cno]) == 0:
                filename = ds_old['meta']['filename']
                print(f'{filename}#{lno}: No content in column {columnnumber2exp(cno)}', file=sys.stderr)
                break
            elif ctype is not None:
                try:
                    if ctype == 'int' and int(record_old[cno]):
                        pass
                    elif ctype == 'float' and float(record_old[cno]):
                        pass
                except ValueError:
                    filename = ds_old['meta']['filename']
                    print(f'{filename}#{lno}: Unmatched type of column {columnnumber2exp(cno)}', file=sys.stderr)
                    break
            record.append(record_old[cno])
        else:
            ds['data'].update({id: record})
            continue

    return ds

def select(csv, prefix=None, encoding=None, hint=None, filter=None, strict=False):

    collection = detect(csv, encoding, prefix=prefix, hint=hint)
    for collected in collection:
        if collected['status'] == 0:
            ds = collected['dataset']
            header = collected['header']
            column_numbers = header['columns'] if header is not None else None
            ds = select_columns(ds, column_numbers, filter, strict)

            selecteds = []
            for id, record in ds['data'].items():
                selecteds.append([ds['meta']['id'], id] + record)

            collected['selection'] = selecteds

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
    parser.add_argument('--prefix', nargs=1, metavar='NAME', help='record id prefix')
    parser.add_argument('--hint', nargs=1, metavar='HINTS', help='header record hint as \'RANGE:VALUES\', eg. \'1-5:*A,[Nn]ame\'')
    parser.add_argument('--filter', nargs=1, metavar='FILTER', help='column filter (\'int\' or \'float\')')
    parser.add_argument('--strict', action='store_true', help='not allow no content columns')
    parser.add_argument('--csv', action='store_true', help='csv output')
    parser.add_argument('--post-process', nargs=1, metavar='module', help='call post process module')
    parser.add_argument('--post-process-args', nargs='*', metavar='NAME=VALUE', help='post process module arguments')

    if len(sys.argv) == 1:
        print(parser.format_usage(), file=sys.stderr)
        exit(1)

    args = parser.parse_args()

    csv_path = args.path if args.path is not None else None
    encoding = args.encoding[0] if args.encoding is not None else None
    prefix = args.prefix[0] if args.prefix is not None else None
    hint = args.hint[0] if args.hint is not None else None
    filter = args.filter[0] if args.filter is not None else None
    strict = args.strict
    collection = select(csv_path, prefix=prefix, encoding=encoding, hint=hint, filter=filter, strict=strict)
    post_process = get_post_process(args)
    return post_process.selected(collection)

if __name__ == '__main__':
    exit(main())
