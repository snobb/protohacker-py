import task07
import logging
import sys
import unittest

logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)


class Task07Test(unittest.TestCase):

    def setUp(self) -> None:
        self.app = task07.App()

    def tearDown(self) -> None:
        pass

    def test_single_line(self) -> None:
        self.app.write(b"hello ")
        self.app.write(b"world\n")
        self.assertEqual(b"dlrow olleh\n", self.app.read())

    def test_single_line_and_skip_incomplete_lines(self) -> None:
        data = [b"hello ", b"world\n", b"foo ", b"bar"]
        for chunk in data:
            self.app.write(chunk)
        self.assertEqual(b"dlrow olleh\n", self.app.read())
        self.app.write(b"\n")
        self.assertEqual(b"rab oof\n", self.app.read())

    def test_three_line_and_skip_incomplete_lines(self) -> None:
        data = [b"hello ", b"world\n", b"bazbaz\n", b"foo ", b"bar"]
        for chunk in data:
            self.app.write(chunk)
        self.assertEqual(b"dlrow olleh\nzabzab\n", self.app.read())
        self.app.write(b"\n")
        self.assertEqual(b"rab oof\n", self.app.read())

    def test_three_line_with_new_line_separately(self) -> None:
        data = [b"hello ", b"world", b"\n", b"foo", b"bar\n"]
        for chunk in data:
            self.app.write(chunk)
        self.assertEqual(b"dlrow olleh\nraboof\n", self.app.read())
