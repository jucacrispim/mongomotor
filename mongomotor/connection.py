# -*- coding: utf-8 -*-

import motor
from mongoengine import connection, ConnectionError
from mongoengine.connection import connect, disconnect


CONNECTION_TYPES = {'tornado': motor.MotorClient,
                    'asyncio': motor.AsyncIOMotorClient}

REPLICASET_TYPES = {'tornado': motor.MotorReplicaSetClient,
                    'asyncio': motor.AsyncIOMotorReplicaSetClient}


def get_connection(alias=connection.DEFAULT_CONNECTION_NAME, reconnect=False,
                   connection_type='tornado'):
    """Returns a connection to mongod.
    :param alias: An alias to a different instance of mongod.
    :param reconnect: Disconnects and reconnects to the database if
      already connected.
    :param connection_type: What type of async framework should be used.
      It can be ``tornado`` or ``asyncio``. Defaults to ``tornado``."""

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

        conn_settings.pop('name', None)
        conn_settings.pop('slaves', None)
        conn_settings.pop('is_slave', None)
        conn_settings.pop('authentication_source', None)

        # Get all the slave connections
        if 'slaves' in conn_settings:
            slaves = []
            for slave_alias in conn_settings['slaves']:
                slaves.append(get_connection(slave_alias))
            conn_settings['slaves'] = slaves
            conn_settings.pop('read_preference', None)

        connection_class = CONNECTION_TYPES[connection_type]
        if 'replicaSet' in conn_settings:
            conn_settings['hosts_or_uri'] = conn_settings.pop('host', None)
            # Discard port since it can't be used on MongoReplicaSetClient
            conn_settings.pop('port', None)
            # Discard replicaSet if not base string
            if not isinstance(conn_settings['replicaSet'], str):
                conn_settings.pop('replicaSet', None)
            connection_class = REPLICASET_TYPES[connection_type]

        try:
            # always create a conn_uri because motor uses an uri
            # to authenticated connections
            conn_uri = _create_conn_uri(conn_name, **conn_settings)
            conn_args = _clean_conn_settings(**conn_settings)
            connection._connections[alias] = connection_class(conn_uri,
                                                              **conn_args)
        except Exception as e:
            raise ConnectionError("Cannot connect to database %s :\n%s" % (
                alias, e))
    return connection._connections[alias]


def connect(db=None, alias=DEFAULT_CONNECTION_NAME, **kwargs):
    """Connect to the database specified by the 'db' argument.

    Connection settings may be provided here as well if the database is not
    running on the default port on localhost. If authentication is needed,
    provide username and password arguments as well.

    Multiple databases are supported by using aliases.  Provide a separate
    `alias` to connect to a different instance of :program:`mongod`.

     Changed in mongomotor to pass a connection_type to get_connection

    """
    global _connections
    if alias not in _connections:
        register_connection(alias, db, **kwargs)

    connection_type = kwargs.get('connection_type', 'tornado')
    return get_connection(alias, connection_type=connection_type)


def _create_conn_uri(conn_name, **kwargs):
    # checking if already is a conn string
    if 'mongodb://' in kwargs.get('host', ''):
        return kwargs.get('host')

    uri = 'mongodb://'
    host_port_db = '%s:%s/%s' % (kwargs.get('host'),
                                 kwargs.get('port', '27017'),
                                 conn_name)
    if (kwargs.get('username') and kwargs.get('password')):
        auth = '%s:%s@' % (kwargs.get('username'), kwargs.get('password'))
        uri += auth

    uri += host_port_db

    return uri


def _clean_conn_settings(**conn_settings):
    """Remove settings already used to create the connection string
    leaving only the kwargs to be passed to connection_class
    """
    new_conn = conn_settings.copy()
    notallowed = ['host', 'port', 'username', 'password']
    for na in notallowed:
        try:
            del new_conn[na]
        except KeyError:
            pass

    return new_conn
