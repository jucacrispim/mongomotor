# -*- coding: utf-8 -*-

from tornado import gen
from blinker import Signal


class AsyncSignal(Signal):
    """
    Signal with async :meth:`send`.
    To use it you must use async methods as receivers
    """

    @gen.coroutine
    def send(self, *sender, **kwargs):
        """
        :meth:`send` that uses tornado.gen to turn it into a
        async method.

        Emit this signal on behalf of *sender*, passing on \*\*kwargs.

        Returns a list of 2-tuples, pairing receivers with their return
        value. The ordering of receiver notification is undefined.

        :param \*sender: Any object or ``None``.  If omitted, synonymous
          with ``None``.  Only accepts one positional argument.

        :param \*\*kwargs: Data to be sent to receivers.

        """

        # Using '*sender' rather than 'sender=None' allows 'sender' to be
        # used as a keyword argument- i.e. it's an invisible name in the
        # function signature.
        if len(sender) == 0:
            sender = None
        elif len(sender) > 1:
            raise TypeError('send() accepts only one positional argument, '
                            '%s given' % len(sender))
        else:
            sender = sender[0]
        if not self.receivers:
            return []
        else:
            return [(receiver, (yield receiver(sender, **kwargs)))
                    for receiver in self.receivers_for(sender)]


class NamedSignal(AsyncSignal):
    """A named generic notification emitter."""

    def __init__(self, name, doc=None):
        Signal.__init__(self, doc)

        #: The name of this signal.
        self.name = name

    def __repr__(self):  # pragma: no cover
        base = Signal.__repr__(self)
        return "%s; %r>" % (base[:-1], self.name)


class Namespace(dict):
    """A mapping of signal names to signals."""

    def signal(self, name, doc=None):
        """Return the :class:`NamedSignal` *name*, creating it if required.

        Repeated calls to this function will return the same signal object.

        """
        try:
            return self[name]
        except KeyError:
            return self.setdefault(name, NamedSignal(name, doc))
