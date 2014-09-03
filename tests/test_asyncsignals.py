# -*- coding: utf-8 -*-

from mock import Mock, patch
from tornado import gen
from tornado.testing import AsyncTestCase, gen_test
from mongomotor import asyncsignals


class AsyncSignalTest(AsyncTestCase):

    @patch.object(asyncsignals.AsyncSignal, 'receivers_for', Mock())
    @gen_test
    def test_send_without_sender(self):
        sig = asyncsignals.AsyncSignal()
        @gen.coroutine
        def rec(*a, **kw):
            return None

        sig.receivers_for.return_value = [rec]
        sig.receivers = [rec]

        ret = yield sig.send()

        self.assertEqual(len(ret), 1)

    @gen_test
    def test_send_with_too_many_senders(self):
        sig = asyncsignals.AsyncSignal()
        with self.assertRaises(TypeError):
            ret = yield sig.send(Mock(), Mock())

    @gen_test
    def test_send_without_receivers(self):
        sig = asyncsignals.AsyncSignal()
        ret = yield sig.send()

        self.assertEqual(ret, [])

    @patch.object(asyncsignals.AsyncSignal, 'receivers_for', Mock())
    @gen_test
    def test_send(self):
        @gen.coroutine
        def rec(*a, **kw):
            return None

        sig = asyncsignals.AsyncSignal()
        sig.receivers = [rec, rec]
        sig.receivers_for.return_value = sig.receivers

        ret = yield sig.send()
        self.assertEqual(len(ret), 2)
