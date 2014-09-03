# -*- coding: utf-8 -*-

from mongoengine.connection import connect, disconnect

DB_SETTINGS = {'host': 'localhost',
               'port': 27017,
               'db': 'mongomotordevtest'}


def dbconnect():
    connect(**DB_SETTINGS)


def dbdisconnect():
    disconnect()
