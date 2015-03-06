#-*- coding: utf-8 -*-

from setuptools import setup, find_packages


def get_version_from_file():
    # get version number from __init__ file
    # before module is installed

    fname = 'mongomotor/__init__.py'
    with open(fname) as f:
        fcontent = f.readlines()
    version_line = [l for l in fcontent if 'VERSION' in l][0]
    return version_line.split('=')[1].strip().strip("'").strip('"')


VERSION = get_version_from_file()
DESCRIPTION = """
Mongoengine integration with Motor
"""
LONG_DESCRIPTION = """
Mongomotor lets you use the Motor async driver while keeping all the
convenience from Mongoengine.
"""

setup(name='mongomotor',
      version=VERSION,
      author='Juca Crispim',
      author_email='juca@poraodojuca.net',
      description=DESCRIPTION,
      long_description=LONG_DESCRIPTION,
      url='https://gitorious.org/mongomotor',
      packages=find_packages(exclude=['tests', 'tests.*']),
      install_requires=['mongoengine>=0.8.7', 'motor>=0.3'],
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: GNU General Public License (GPL)',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.4',
          'Topic :: Software Development :: Libraries :: Python Modules',
      ],
      test_suite='tests',
      provides=['mongomotor'])
