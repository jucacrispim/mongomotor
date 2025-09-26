# -*- coding: utf-8 -*-

import asyncio
import os
from mongomotor.connection import connect

# tks stackoverflow!


def async_test(f):
    def wrapper(*args, **kwargs):
        future = f(*args, **kwargs)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(future)
    return wrapper


def connect2db():

    host = os.environ.get('MONGOMOTOR_TEST_DB_HOST')
    port = os.environ.get('MONGOMOTOR_TEST_DB_PORT')
    username = os.environ.get('MONGOMOTOR_TEST_DB_USERNAME')
    password = os.environ.get('MONGOMOTOR_TEST_DB_PASSWORD')

    conn_kw = {}

    if host:
        conn_kw['host'] = host

    if port:
        conn_kw['port'] = int(port)

    if username:
        conn_kw['username'] = username

    if password:
        conn_kw['password'] = password

    conn_kw['retryWrites'] = False
    db = 'mongomotor-test'

    connect(db, **conn_kw)
