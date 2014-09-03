# -*- coding: utf-8 -*-

import unittest
import concurrent
# this module raises an error in atexit. Importing it to mock
from tornado import concurrent
from mock import Mock, patch

from mongomotor import connection


# mocking it to get rid of the atexit error
@patch.object(concurrent, 'futures', Mock())
class ConnectionTest(unittest.TestCase):

    @patch.object(connection.connection, '_connection_settings',
                  {'default': {'db': 'a',
                               'slaves': ['naofault']},
                   'naofault': {'db': 'b'}})
    @patch.object(connection, 'motor', Mock())
    @patch.object(connection, 'pymongo', Mock(spec=object))
    @patch.object(connection.connection, '_connections', {})
    @patch.object(connection.connection, '_dbs', {})
    def test_get_connection(self):
        connection.get_connection()
        self.assertTrue(connection.motor.MotorClient.called)

    @patch.object(connection.connection, '_connection_settings',
                  {'default': {'replicaSet': 1,
                               'db': 'test'}})
    @patch.object(connection, 'motor', Mock())
    @patch.object(connection.connection, '_connections', {})
    @patch.object(connection.connection, '_dbs', {})
    def test_connection_with_replicaset(self):
        connection.get_connection()

        self.assertTrue(connection.motor.MotorReplicaSetClient.called)

    @patch.object(connection.connection, '_connection_settings',
                  {'default': {'db': 'a'}})
    @patch.object(connection.connection, 'disconnect', Mock())
    @patch.object(connection, 'motor', Mock())
    @patch.object(connection.connection, '_connections', {})
    @patch.object(connection.connection, '_dbs', {})
    def test_get_connection_reconnect(self):
        connection.get_connection(reconnect=True)

        self.assertTrue(connection.connection.disconnect.called)

    @patch.object(connection.connection, '_connection_settings', {})
    @patch.object(connection.connection, '_connections', {})
    @patch.object(connection.connection, '_dbs', {})
    def test_get_connection_raising_connection_error_caused_by_no_config(self):
        with self.assertRaises(connection.ConnectionError):
            connection.get_connection()

    @patch.object(connection.connection, '_connection_settings',
                 {'default': {'db': 'a'}})
    @patch.object(connection.connection, '_connections', {})
    @patch.object(connection.connection, '_dbs', {})
    @patch.object(connection.motor, 'MotorClient',Mock(side_effect=Exception))
    def test_get_connection_raising_connection_error_caused_conf_error(self):
        with self.assertRaises(connection.ConnectionError):
            connection.get_connection()

    def test_create_conn_uri(self):
        conn_name ='test-coll'
        kw = {'host': 'localhost',
              'port': 27017,
              'username': 'testuser',
              'password': 'bla'}

        expected = 'mongodb://testuser:bla@localhost:27017/test-coll'

        returned = connection._create_conn_uri(conn_name, **kw)

        self.assertEqual(returned, expected)
