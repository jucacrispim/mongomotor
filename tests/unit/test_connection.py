# -*- coding: utf-8 -*-

from unittest import TestCase
from mongomotor import connect, disconnect
from mongomotor.connection import MotorClient, AsyncIOMotorClient


class ConnectionTest(TestCase):

    def tearDown(self):
        disconnect()

    def test_connect_with_tornado(self):
        conn = connect()
        self.assertTrue(isinstance(conn, MotorClient))

    def test_connect_with_asyncio(self):
        conn = connect(connection_type='asyncio')
        self.assertTrue(isinstance(conn, AsyncIOMotorClient))
