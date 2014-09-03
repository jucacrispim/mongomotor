# -*- coding: utf-8 -*-

import pymongo
import motor
from mongoengine import connection, ConnectionError


def get_connection(alias=connection.DEFAULT_CONNECTION_NAME, reconnect=False):

    # Connect to the database if not already connected
    if reconnect:
        connection.disconnect(alias)

    if alias not in connection._connections:
        if alias not in connection._connection_settings:
            msg = 'Connection with alias "%s" has not been defined' % alias
            if alias == connection.DEFAULT_CONNECTION_NAME:
                msg = 'You have not defined a default connection'
            raise ConnectionError(msg)

        conn_settings = connection._connection_settings[alias].copy()
        try:
            conn_name = conn_settings['name']
        except KeyError:
            conn_name = conn_settings['db']

        if hasattr(pymongo, 'version_tuple'):  # Support for 2.1+
            conn_settings.pop('name', None)
            conn_settings.pop('slaves', None)
            conn_settings.pop('is_slave', None)
        else:
            # Get all the slave connections
            if 'slaves' in conn_settings:
                slaves = []
                for slave_alias in conn_settings['slaves']:
                    slaves.append(get_connection(slave_alias))
                conn_settings['slaves'] = slaves
                conn_settings.pop('read_preference', None)

        connection_class = motor.MotorClient
        if 'replicaSet' in conn_settings:
            conn_settings['hosts_or_uri'] = conn_settings.pop('host', None)
            # Discard port since it can't be used on MongoReplicaSetClient
            conn_settings.pop('port', None)
            # Discard replicaSet if not base string
            if not isinstance(conn_settings['replicaSet'], str):
                conn_settings.pop('replicaSet', None)
            connection_class = motor.MotorReplicaSetClient

        try:
            conn_uri = _create_conn_uri(conn_name, **conn_settings)
            connection._connections[alias] = connection_class(conn_uri)
        except Exception as e:
            raise ConnectionError("Cannot connect to database %s :\n%s" % (
                alias, e))
    return connection._connections[alias]


def _create_conn_uri(conn_name, **kwargs):
    uri = 'mongodb://'
    host_port_db = '%s:%s/%s' % (kwargs.get('host'),
                                 kwargs.get('port', '27017'),
                                 conn_name)
    if (kwargs.get('username') and kwargs.get('password')):
        auth = '%s:%s@' % (kwargs.get('username'), kwargs.get('password'))
        uri += auth

    uri += host_port_db

    return uri
