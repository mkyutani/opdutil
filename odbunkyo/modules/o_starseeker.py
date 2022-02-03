#!/usr/bin/env python3

import re
import sys
from odbunkyo.modules.base import BasePostProcess

class PostProcess(BasePostProcess):

    def __init__(self, args):
        super().__init__(args)
        self.base = int(args.base) if args.base is not None else 100000
        self.name = args.name if args.name is not None else 'opendata'
        self.color = args.color if args.color is not None else 'gold'
        self.order = int(args.order) if args.order is not None else 10
        self.attributes = self.create_attribute_objects(args.attributes) if args.attributes is not None else None
        self.category_file = args.category_file if args.category_file is not None else 'category.csv'
        self.dataset_file = args.dataset_file if args.dataset_file is not None else 'dataset.csv'
        self.data_file = args.data_file if args.data_file is not None else 'data.csv'
        self.seq_no = self.base

    def list_argument_names(self):

        return [
            'base', 'name', 'color', 'order',
            'attributes',
            'category_file', 'dataset_file', 'data_file'
        ]

    def seq(self):
        seq_no = self.seq_no
        self.seq_no = self.seq_no + 1
        return seq_no

    def seq_offset(self):
        return self.seq_no - self.base

    def create_attribute_objects(self, attributes):

        attribute_objects = []

        attribute_list = attributes.split(',')
        for order, attribute in enumerate(attribute_list):
            m = re.match('^(\w+)(\(([\w:]+)\))?(:(.+))?$', attribute)
            if m:
                id = m.group(1)
                id_type = m.group(3)
                name = m.group(5)
                if id_type is None:
                    id_type = 'text'
                else:
                    id_type = id_type.lower()
                if name is None:
                    name = id

                id_lower = id.lower()
                if id_lower == 'location' or id_lower == 'time':
                    builtin = True
                else:
                    builtin = False

                if re.match('image', id_lower):
                    data_type = 1
                else:
                    data_type = 0

                attribute_objects.append({
                    'id': id,
                    'id_type': id_type,
                    'name': name,
                    'order': order + 1,
                    'type': data_type,
                    'builtin': builtin
                })
            else:
                print(f'Invalid StarSeeker attribute: {attribute}')

        return attribute_objects

    def generate_starseeker_category_file(self, fd, collected):

        category_id = self.seq()

        try:
            fd.write(f'{category_id},{self.name},{self.color},{self.order},○\n')
        except Exception as e:
            print(e, file=sys.stderr)

        return category_id

    def generate_starseeker_dataset_file(self, fd, collected, category_id):

        color_palette = [
            'red', 'indigo', 'green', 'orange', 'pink',
            'blue', 'lightgreen', 'deeporange', 'lightblue', 'lime', 
            'brown', 'darkred', 'yellow', 'purple', 'cyan', 'grey',
            '#9e5827', '#98da1d', '#fc3ebf', '#28dce1', '#135eb0',
            '#fcc1fb', '#f6e00', '#91207b', '#c9d9c1', '#7c8869',
            '#3c4c1e', '#4959ea', '#e1d923', '#38e515', '#69affc',
            '#d16dbe', '#eea979', '#9a31e2', '$5a3e4f', '#ca2c17'
        ]

        ds_id = self.seq()
        ds_name = collected['dataset']['meta']['name']
        ds_entity_type_id = collected['dataset']['meta']['id']
        ds_color = color_palette[self.seq_offset() % len(color_palette)]

        try:
            fd.write(f'{ds_id},{category_id},{self.name},{ds_name},{ds_color},{ds_entity_type_id},○')
            fd.write(',location')
            fd.write(',time')
            for attribute in self.attributes:
                if attribute['builtin'] is False:
                    id = attribute['id']
                    name = attribute['name']
                    order = attribute['order']
                    type = attribute['type']
                    fd.write(f',{name},{id},{type},{order}')
            fd.write('\n')
        except Exception as e:
            print(e, file=sys.stderr)

    def generate_starseeker_data_file(self, fd, collected, category_id):

        ds_entity_type_id = collected['dataset']['meta']['id']

        try:
            for vector in collected['selection']:
                ds_id = self.seq()
                vector_index = 2
                vector_max = len(vector)
                for attribute in self.attributes:
                    id = attribute['id']
                    id_type = attribute['id_type']
                    if id_type == 'geo:point':
                        if vector_index + 1 < vector_max: 
                            value = f'"{vector[vector_index]}, {vector[vector_index+1]}"'
                            vector_index + vector_index + 2
                        else:
                            value = f'"0, 0"'
                    else:
                        if vector_index < vector_max:
                            value = f'{vector[vector_index]}'
                            vector_index = vector_index + 1
                        elif id_type == 'integer':
                            value = 0
                        elif id_type == 'float':
                            value = 0.0
                        elif id_type == 'datetime':
                            value = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
                        else :
                            value = ''
                    fd.write(f'{ds_id},{ds_entity_type_id},{id},{id_type},{value}\n')
        except Exception as e:
            print(e, file=sys.stderr)

    def selected(self, collection):

        try:
            with open(self.category_file, 'x') as fd_category:
                category_id = self.generate_starseeker_category_file(fd_category, collection)
                with open(self.dataset_file, 'x') as fd_dataset:
                    with open(self.data_file, 'x') as fd_data:
                        for collected in collection:
                            if collected['status'] == 0:
                                self.generate_starseeker_dataset_file(fd_dataset, collected, category_id)
                                self.generate_starseeker_data_file(fd_data, collected, category_id)
        except Exception as e:
            print(e, file=sys.stderr)

        return 0

    def detected(self, collection):

        return 1
