# -*- coding: utf-8 -*-

#     Copyright 2019. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

from setuptools import setup
from os import path

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

VERSION = "2.6"

setup(
    version=VERSION,
    name="thingsboard-gateway",
    author="ThingsBoard",
    author_email="info@thingsboard.io",
    license="Apache Software License (Apache Software License 2.0)",
    description="Thingsboard Gateway for IoT devices.",
    url="https://github.com/thingsboard/thingsboard-gateway",
    long_description=long_description,
    long_description_content_type="text/markdown",
    include_package_data=True,
    python_requires=">=3.5",
    packages=['thingsboard_gateway', 'thingsboard_gateway.gateway', 'thingsboard_gateway.storage',
              'thingsboard_gateway.tb_client', 'thingsboard_gateway.connectors', 'thingsboard_gateway.connectors.ble',
              'thingsboard_gateway.connectors.mqtt', 'thingsboard_gateway.connectors.opcua', 'thingsboard_gateway.connectors.request',
              'thingsboard_gateway.connectors.modbus', 'thingsboard_gateway.connectors.can', 'thingsboard_gateway.connectors.bacnet',
              'thingsboard_gateway.connectors.bacnet.bacnet_utilities', 'thingsboard_gateway.connectors.odbc',
              'thingsboard_gateway.connectors.rest', 'thingsboard_gateway.connectors.snmp',
              'thingsboard_gateway.tb_utility', 'thingsboard_gateway.extensions',
              'thingsboard_gateway.extensions.mqtt', 'thingsboard_gateway.extensions.modbus', 'thingsboard_gateway.extensions.opcua',
              'thingsboard_gateway.extensions.ble', 'thingsboard_gateway.extensions.serial', 'thingsboard_gateway.extensions.request',
              'thingsboard_gateway.extensions.can', 'thingsboard_gateway.extensions.bacnet', 'thingsboard_gateway.extensions.odbc',
              'thingsboard_gateway.extensions.rest',  'thingsboard_gateway.extensions.snmp'
              ],
    install_requires=[
        'jsonpath-rw',
        'regex',
        'pip',
        'paho-mqtt',
        'PyYAML',
        'simplejson',
        'requests'
    ],
    download_url='https://github.com/thingsboard/thingsboard-gateway/archive/%s.tar.gz' % VERSION,
    entry_points={
        'console_scripts': [
            'thingsboard-gateway = thingsboard_gateway.tb_gateway:daemon'
        ]})
