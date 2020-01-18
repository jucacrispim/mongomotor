# -*- coding: utf-8 -*-

import asyncio
import os
from mongomotor import connect

# tks stackoverflow!


def async_test(f):
    def wrapper(*args, **kwargs):
        coro = asyncio.coroutine(f)
        future = coro(*args, **kwargs)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)
    return wrapper


def connect2db(async_framework='asyncio'):

    host = os.environ.get('MONGOMOTOR_TEST_DB_HOST')
    port = os.environ.get('MONGOMOTOR_TEST_DB_PORT')
    username = os.environ.get('MONGOMOTOR_TEST_DB_USERNAME')
    password = os.environ.get('MONGOMOTOR_TEST_DB_PASSWORD')

    conn_kw = {'async_framework': async_framework}

    if host:
        conn_kw['host'] = host

    if port:
        conn_kw['port'] = int(port)

    if username:
        conn_kw['username'] = username

    if password:
        conn_kw['password'] = password

    conn_kw['io_loop'] = asyncio.get_event_loop()
    conn_kw['retryWrites'] = False
    db = 'mongomotor-test'

    connect(db, **conn_kw)
