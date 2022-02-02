#!/usr/bin/env python3

import json
from modules.base import BasePostProcess

class PostProcess(BasePostProcess):

    def __init__(self, args):
        super().__init__(args)

    def list_argument_names(self):
        return []

    def print_items(self, matrix):

        output_format = 'csv' if self.args.csv is True else None
        delimiter=self.args.delimiter[0]

        if output_format == 'csv':
            for line in matrix:
                print(delimiter.join(line))
        else:
            dump = json.dumps(matrix, ensure_ascii=False, indent=2)
            print(dump)

    def selected(self, collection):

        ret = 0
        for collected in collection:
            if ret == 0:
                ret = collected['status']
            if collected['selection'] is not None:
                self.print_items(collected['selection'])

        return ret

    def detected(self, collection):

        ret = 0
        for collected in collection:
            if ret == 0:
                ret = collected['status']
            if collected['header'] is not None:
                self.print_items([collected['header']['items']])

        return ret
