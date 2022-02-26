#!/usr/bin/env python3

import csv
import errno
import io
import re
import requests
import sys
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

def generate_dataset_name(bs_tag, url):
    parent = bs_tag.parent
    if parent is not None:
        name = parent.get_text(strip=True)
        if name is not None and len(name) > 0:
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
    parser.add_argument('url', nargs=1, metavar='URL', help='open data portal url')
    parser.add_argument('-d', '--delimiter', nargs=1, default=',', help='delimiter')

    if len(sys.argv) == 1:
        print(parser.format_usage(), file=sys.stderr)
        exit(1)

    args = parser.parse_args()

    return list_datasets(args.url[0], delimiter=args.delimiter[0])

if __name__ == '__main__':
    exit(main())
