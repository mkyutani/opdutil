#!/usr/bin/env python3

from glob import glob
from os.path import basename
from os.path import splitext

from setuptools import setup
from setuptools import find_packages

setup(
    name='opdutil',
    version='0.1.0',
    license='MIT LICENSE',
    description='open data utilities',
    author='mkyutani@gmail.com',
    url='http://github.com/mkyutani/opdutil',
    packages=find_packages(),
    install_requires=open('requirements.txt').read().splitlines(),
    entry_points={
        'console_scripts': [
            'opdselect=opdutil.opdselect:main',
            'opddetect=opdutil.opddetect:main',
            'opdlist=opdutil.opdlist:main'
        ]
    },
    zip_safe=False
)
