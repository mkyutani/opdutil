#!/usr/bin/env python3

import json

def print_items(matrix, output_format=None, delimiter=','):

    if output_format == 'csv':
        for line in matrix:
            print(delimiter.join(line))
    else:
        dump = json.dumps(matrix, ensure_ascii=False, indent=2)
        print(dump)

def selected_post_process(collection, args):

    output_format = 'csv' if args.csv is True else None
    delimiter=args.delimiter[0]

    ret = 0
    for collected in collection:
        if ret == 0:
            ret = collected['status']
        if collected['selection'] is not None:
            print_items(collected['selection'], output_format=output_format, delimiter=delimiter)

    return ret

def detected_post_process(collection, args):

    output_format = 'csv' if args.csv is True else None
    delimiter=args.delimiter[0]

    ret = 0
    for collected in collection:
        if ret == 0:
            ret = collected['status']
        if collected['header'] is not None:
            print_items([collected['header']['items']], output_format=output_format, delimiter=delimiter)

    return ret
