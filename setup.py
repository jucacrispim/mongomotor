# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


def get_version_from_file():
    # get version number from __init__ file
    # before module is installed

    fname = 'mongomotor/__init__.py'
    with open(fname) as f:
        fcontent = f.readlines()
    version_line = [l for l in fcontent if 'VERSION' in l][0]
    return version_line.split('=')[1].strip().strip("'").strip('"')


def get_long_description_from_file():
    # content of README will be the long description

    fname = 'README'
    with open(fname) as f:
        fcontent = f.read()
    return fcontent


VERSION = get_version_from_file()

DESCRIPTION = """
MongoMotor: An async document-object mapper for MongoDB
"""
LONG_DESCRIPTION = get_long_description_from_file()

setup(name='mongomotor',
      version=VERSION,
      author='Juca Crispim',
      author_email='juca@poraodojuca.net',
      description=DESCRIPTION,
      long_description=LONG_DESCRIPTION,
      description_content_type='text/x-rst',
      url='http://mongomotor.poraodojuca.net/',
      packages=find_packages(exclude=['tests', 'tests.*']),
      install_requires=['mongoengine>=0.15.0', 'motor>=2.0.0', 'blinker>=1.3',
                        'pymongo<4,>=3.4', 'asyncblink>=0.3.2'],
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: GNU General Public License (GPL)',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Topic :: Software Development :: Libraries :: Python Modules',
          'License :: OSI Approved :: GNU General Public License v3 (GPLv3+)'
      ],
      test_suite='tests',
      provides=['mongomotor'])
