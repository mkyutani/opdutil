#!/usr/bin/env python3

import pprint
from odbunkyo.modules.base import BasePostProcess

class PostProcess(BasePostProcess):

    def __init__(self, args):
        super().__init__(args)

    def list_argument_names(self):
        return []

    def print(self, collection):
        print('-------- sample module: selected_post_process function --------')
        print('-------- collection --------')
        pprint.pprint(collection)
        print('-------- args --------')
        pprint.pprint(self.args)

    def selected(self, collection):
        self.print(collection)

    def detected(self, collection):
        self.print(collection)
