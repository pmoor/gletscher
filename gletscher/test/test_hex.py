import hex
import unittest

class TestHex(unittest.TestCase):

  def test_h2b(self):
    self.assertEqual(b"\xaa\xbb\xcc", hex.h2b("aabbcc"))

  def test_b2h(self):
    self.assertEqual("beef", hex.b2h(b"\xbe\xef"))