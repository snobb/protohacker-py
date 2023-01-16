import task05
import logging
import sys
import unittest

logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)


class Task05Test(unittest.TestCase):

    def setUp(self) -> None:
        self.bogus = task05.BogusCoin(logging.getLogger('test'))

    def tearDown(self) -> None:
        pass

    def test_replace_address_no(self) -> None:
        no7 = "foo7AAAAAAAAAAAAAAAAAAAAAAAAAA"
        res = self.bogus.replace_address(no7)
        self.assertEqual(res, no7)

    def test_replace_address_too_big(self) -> None:
        too_big = "7AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        res = self.bogus.replace_address(too_big)
        self.assertEqual(res, too_big)

    def test_replace_address_start(self) -> None:
        start = "7AAAAAAAAAAAAAAAAAAAAAAAAAA foo"
        res = self.bogus.replace_address(start)
        self.assertEqual(res, f"{task05.BogusCoin.evil_addr} foo")

    def test_replace_address_middle(self) -> None:
        mid = "foo 7AAAAAAAAAAAAAAAAAAAAAAAAAAA bar"
        res = self.bogus.replace_address(mid)
        self.assertEqual(res, f"foo {task05.BogusCoin.evil_addr} bar")

    def test_replace_address_end(self) -> None:
        end = "foo 7AAAAAAAAAAAAAAAAAAAAAAAAAAA"
        res = self.bogus.replace_address(end)
        self.assertEqual(res, f"foo {task05.BogusCoin.evil_addr}")

    def test_replace_address_many(self) -> None:
        end = "foo 7AAAAAAAAAAAAAAAAAAAAAAAAAAA 7AAAAAAAAAAAAAAAAAAAAAAAAAAA"
        res = self.bogus.replace_address(end)
        self.assertEqual(
            res,
            f"foo {task05.BogusCoin.evil_addr} {task05.BogusCoin.evil_addr}")

    def test_replace_address_with_space(self) -> None:
        with_space = "foo 750 Boguscoins bar"
        res = self.bogus.replace_address(with_space)
        self.assertEqual(res, with_space)

    def test_replace_address_solid(self) -> None:
        solid = ("foo 7kL9wo3jDRHc9ZJywsbZH6BSBP-u6BPmVzNxRM2m0EePR"
                 "qnK6H55xpSjtjGZ-1234 bar")
        res = self.bogus.replace_address(solid)
        self.assertEqual(res, solid)
