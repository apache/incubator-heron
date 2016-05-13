import unittest2 as unittest
from mock import call, patch, Mock
from heron.common.src.python.network import sock_util
from heron.common.src.python.network.exceptions import ConnectionException

class SockUtilTest(unittest.TestCase):
  def setUp(self):
    pass

  def test_send(self):
    mock_sock = Mock()
    msg = "Hello world"
    def mock_send(msg):
      return len(msg)

    mock_sock.send = mock_send
    self.assertEqual(len(msg), sock_util.send(mock_sock, msg))

  def test_send_multiple_calls_to_socket_send(self):
    mock_sock = Mock()
    msg = "Hello world"
    def mock_send(msg):
      # Ensures that more data needs to be sent to the socket, resulting in multiple
      # calls to socket send.
      return 1

    mock_sock.send = mock_send
    self.assertEqual(len(msg), sock_util.send(mock_sock, msg))

  def test_send_throw_exception_when_socket_closed(self):
    mock_sock = Mock()
    msg = "Hello world"
    def mock_send(msg):
      # Socket closed
      return 0

    mock_sock.send = mock_send
    self.assertRaises(ConnectionException, sock_util.send, mock_sock, msg)

  def test_recv(self):
    mock_sock = Mock()
    msg = "Hello World"
    def mock_recv(size):
      return msg

    mock_sock.recv = mock_recv
    self.assertEqual(msg, sock_util.recv(mock_sock, len(msg)))

  def test_recv_multiple_calls_to_socket_recv(self):
    mock_sock = Mock()
    msg = "Hello World"

    def str_gen():
      for c in msg:
        yield c

    msg_iterator = str_gen()

    def mock_recv(size):
      return msg_iterator.next()

    mock_sock.recv = mock_recv
    self.assertEqual(msg, sock_util.recv(mock_sock, len(msg)))

  def test_recv_exception_when_socket_closed(self):
    mock_sock = Mock()
    msg = "Hello world"
    def mock_recv(msg):
      # Socket closed
      return ''

    mock_sock.recv = mock_recv
    self.assertRaises(ConnectionException, sock_util.recv, mock_sock, len(msg))
