import unittest
from openfactory.kafka import CaseInsensitiveDict


class TestCaseInsensitiveDict(unittest.TestCase):
    """
    Test class for CaseInsensitiveDict
    """

    def setUp(self):
        self.cid = CaseInsensitiveDict({
            'Foo': 1,
            'bAr': 2,
            'BAZ': 3
        })

    def test_getitem_case_insensitive(self):
        self.assertEqual(self.cid['foo'], 1)
        self.assertEqual(self.cid['FOO'], 1)
        self.assertEqual(self.cid['Bar'], 2)
        self.assertEqual(self.cid['baz'], 3)

    def test_getitem_keyerror(self):
        with self.assertRaises(KeyError):
            _ = self.cid['nope']

    def test_get_method(self):
        self.assertEqual(self.cid.get('foo'), 1)
        self.assertEqual(self.cid.get('FOO'), 1)
        self.assertEqual(self.cid.get('nope', 42), 42)
        self.assertIsNone(self.cid.get('nope'))

    def test_contains_case_insensitive(self):
        self.assertTrue('foo' in self.cid)
        self.assertTrue('FOO' in self.cid)
        self.assertTrue('bar' in self.cid)
        self.assertFalse('nope' in self.cid)

    def test_setitem_case_insensitive(self):
        # Update existing key (any case)
        self.cid['FOO'] = 100
        self.assertEqual(self.cid['foo'], 100)
        self.assertEqual(self.cid['FOO'], 100)

        # Add new key
        self.cid['newKey'] = 999
        self.assertEqual(self.cid['newkey'], 999)
        self.assertEqual(self.cid['newKey'], 999)

    def test_delitem_case_insensitive(self):
        # Delete existing key (any case)
        del self.cid['FOO']
        with self.assertRaises(KeyError):
            _ = self.cid['Foo']

        # Deleting non-existing key raises KeyError
        with self.assertRaises(KeyError):
            del self.cid['nope']
