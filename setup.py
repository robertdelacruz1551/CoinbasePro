#!/usr/bin/env python

from os.path import exists
from setuptools import setup, find_packages

setup(
    name='CoinbaseProWebsocketClient',
    version=open('VERSION').read().strip(),
    # Your name & email here
    author='Robert De La Cruz',
    author_email='robert.delacruz1551@hotmail.com',
    # If you had CoinbaseProWebsocketClient.tests, you would also include that in this list
    packages=find_packages(),
    # Any executable scripts, typically in 'bin'. E.g 'bin/do-something.py'
    scripts=[],
    # REQUIRED: Your project's URL
    url='https://github.com/robertdelacruz1551/CoinbaseProWebsocketClient',
    # Put your license here. See LICENSE.txt for more information
    license='MIT',
    keywords=['bitcoin', 'ethereum', 'BTC', 'ETH', 'client','exchange', 'crypto', 'currency', 'coinbase','market'],
    # Put a nice one-liner description here
    description='Websocket client to connect to the Coinbase exchange',
    long_description=open('README.md').read() if exists("README.md") else "",
    # Any requirements here, e.g. "Django >= 1.1.1"
    install_requires=[
        
    ],
    # Ensure we include files from the manifest
    include_package_data=True,
)
