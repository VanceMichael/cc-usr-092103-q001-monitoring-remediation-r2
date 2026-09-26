import unittest

from src.evidence.hashing import canonical, digest, digest_concat, sha256_hex


class HashingTest(unittest.TestCase):
    def test_canonical_is_deterministic_and_order_independent(self):
        a = canonical({"b": 1, "a": [1, 2, {"x": True, "y": None}]})
        b = canonical({"a": [1, 2, {"y": None, "x": True}], "b": 1})
        self.assertEqual(a, b)

    def test_null_and_bool_not_confused_with_strings(self):
        self.assertNotEqual(canonical(None), canonical("__null__"))
        self.assertNotEqual(canonical(True), canonical("__true__"))
        self.assertNotEqual(canonical(True), canonical(1))

    def test_digest_stable_across_calls(self):
        obj = {"phase": "self_check_stage_1", "n": 3, "items": ["a", "b"]}
        self.assertEqual(digest(obj), digest(obj))
        self.assertEqual(len(digest(obj)), 64)

    def test_digest_changes_with_content(self):
        self.assertNotEqual(digest({"a": 1}), digest({"a": 2}))

    def test_concat_is_order_sensitive(self):
        self.assertNotEqual(digest_concat("aa", "bb"), digest_concat("bb", "aa"))
        self.assertEqual(sha256_hex(b""),
                         "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")


if __name__ == "__main__":
    unittest.main()
