#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages
from os import path

# read the contents of your README file
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

with open('history.md') as history_file:
    history = history_file.read()

with open('requirements.txt') as req_file:
    requirements = req_file.read()


setup(
    name='guds',
    version='0.2.2',
    description="GUDS is a geoserver upload/download script for moving data to and from the geoserver for AWSM data products",
    long_description=long_description + '\n\n' + history,
    long_description_content_type='text/markdown',
    author="Micah Johnson",
    author_email='micah.johnson150@gmail.com',
    url='https://github.com/USDA-ARS-NWRC/guds',
    packages=find_packages(include=['guds']),
    entry_points={
        'console_scripts': [
            'guds=guds.upload:main'
        ]
    },
    include_package_data=True,
    install_requires=requirements,
    license="GNU General Public License v3",
    zip_safe=False,
    keywords=['guds', 'geoserver', 'modeling'],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
