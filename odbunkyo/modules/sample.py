#!/usr/bin/env python3

import pprint

def selected_post_process(collection, args):
    print('-------- sample module: selected_post_process function --------')
    print('-------- collection --------')
    pprint.pprint(collection)
    print('-------- args --------')
    pprint.pprint(args)

def detected_post_process(collection, args):
    print('-------- sample module: detected_post_process function --------')
    print('-------- collection --------')
    pprint.pprint(collection)
    print('-------- args --------')
    pprint.pprint(args)
