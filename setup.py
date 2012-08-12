#!/usr/bin/env python

"""
 Copyright 2012 the original author or authors

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
"""

from setuptools import find_packages, setup
from codecs import open

from toolazydogs.zookeeper import __version__

setup(
    name = 'pookeeper',
    version = __version__,
    url = 'http://github.com/acabrera/pookeeper/',
    license = 'Apache Software License (http://www.apache.org/licenses/LICENSE-2.0)',
    author = 'Alan D. Cabrera',
    author_email = 'adc@toolazydogs.com',
    description = 'Pure Python bindings for Apache Zookeeper.',
    # don't ever depend on refcounting to close files anywhere else
    long_description = open('README.rst', encoding='utf-8').read(),
    packages = find_packages(exclude=['examples', 'tests']),
    zip_safe = False,
    platforms = 'any',
    test_suite = 'nose.collector',
    install_requires = [
        'mockito>=0.5.0',
        'nose>=1.0.0',
    ],
    classifiers = [
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
