import tempfile
import unittest
import crypto
import data
import hex

SMALLEST_CHUNK_OF_DATA = b"!"
SMALL_CHUNK_OF_DATA = b"a small chunk of data!"

class TestDataFile(unittest.TestCase):

  def test_write_and_read(self):
    crypter = crypto.Crypter(b"0" * 32)
    with tempfile.TemporaryDirectory() as dir:
      f = data.DataFile(dir, 200, crypter)
      self.assertTrue(f.fits(SMALL_CHUNK_OF_DATA))
      self.assertEqual((19, 77), f.add(SMALL_CHUNK_OF_DATA))
      self.assertTrue(f.fits(SMALLEST_CHUNK_OF_DATA))
      self.assertEqual((96, 57), f.add(SMALLEST_CHUNK_OF_DATA))
      self.assertFalse(f.fits(SMALL_CHUNK_OF_DATA))
      tree_hash = f.finalize()

      with open(f.file_name(), "rb") as r:
        hasher = crypto.TreeHasher()
        hasher.consume(r)
        self.assertEqual(hasher.get_tree_hash(), tree_hash)

      ro = data.ReadOnlyDataFile(f.file_name(), crypter)
      self.assertEqual(
        SMALL_CHUNK_OF_DATA,
        ro.read(19, 77,
          hex.h2b("11e54425cefb5ba8f55b9c3de9c246da5e99672c60984d792cda8c57c4718ea9")))
      self.assertEqual(
        SMALLEST_CHUNK_OF_DATA,
        ro.read(96, 57,
          hex.h2b("619e9f1f44ea07ab76980b54c31296ee1d390a94da8240f142931d202c18b159")))
      ro.close()

      f.delete()


if __name__ == '__main__':
  unittest.main()