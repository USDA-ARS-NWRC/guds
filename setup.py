#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

with open('history.md') as history_file:
    history = history_file.read()

with open('requirements.txt') as req_file:
    requirements = req_file.read()


setup(
    name='guds',
    version='0.1.3',
    description="guds is an upload/ download script for moving data to and from the geoserver for AWSM data products",
    long_description=readme + '\n\n' + history,
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
