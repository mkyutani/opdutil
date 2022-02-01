#!/usr/bin/env python3

from glob import glob
from os.path import basename
from os.path import splitext

from setuptools import setup
from setuptools import find_packages

setup(
    name='open-data-bunkyo',
    version='0.1.0',
    license='MIT LICENSE',
    description='Open Data CSV utilities',
    author='mkyutani@gmail.com',
    url='http://github.com/mkyutani/open-data-bunkyo',
    packages=find_packages(),
    install_requires=open('requirements.txt').read().splitlines(),
    entry_points={
        'console_scripts': [
            'odutil=odbunkyo.odutil:main',
        ]
    },
    zip_safe=False
)
