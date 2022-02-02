#!/usr/bin/env python3

import json

class BasePostProcess():

    def __init__(self, args):
        self.args = self.extend_arguments(args)

    def extend_arguments(self, args):
        pparg_names = self.list_argument_names()
        for pparg_name in pparg_names:
            args.__dict__[pparg_name] = None

        if args.post_process_args is not None:
            for pparg in args.post_process_args:
                kv = pparg.split('=', 1)
                if len(kv) == 2:
                    k = kv[0].strip()
                    v = kv[1].strip()
                    args.__dict__[k] = v
                elif len(kv) == 1 and len(kv[0]) > 0:
                    k = kv[0].strip()
                    args.__dict__[k] = ''
                else:
                    pass

        return args

    def list_argument_names(self):
        return []

    def selected(self, collection):
        pass

    def detected(self, collection):
        pass
