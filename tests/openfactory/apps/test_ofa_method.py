import unittest
from openfactory.apps import ofa_method


class TestOFAMethodDecorator(unittest.TestCase):
    """
    Unit tests for @ofa_method decorator
    """

    def test_metadata_is_attached(self):
        """ Decorator should attach _ofa_method_metadata to function. """

        class Dummy:
            @ofa_method()
            def move(self, x: float, y: float, speed: int = 100):
                pass

        metadata = Dummy.move._ofa_method_metadata

        self.assertEqual(metadata["method_name"], "move")

        self.assertIn("x", metadata["parameters"])
        self.assertIn("y", metadata["parameters"])
        self.assertIn("speed", metadata["parameters"])

        self.assertEqual(metadata["parameters"]["x"]["annotation"], float)
        self.assertEqual(metadata["parameters"]["y"]["annotation"], float)
        self.assertEqual(metadata["parameters"]["speed"]["annotation"], int)

        self.assertTrue(metadata["parameters"]["x"]["required"])
        self.assertTrue(metadata["parameters"]["y"]["required"])
        self.assertFalse(metadata["parameters"]["speed"]["required"])

        self.assertIsNone(metadata["parameters"]["x"]["default"])
        self.assertEqual(metadata["parameters"]["speed"]["default"], 100)

    def test_custom_method_name(self):
        """ Decorator should support overriding method name. """

        class Dummy:
            @ofa_method(name="custom_move")
            def move(self, x: float):
                pass

        metadata = Dummy.move._ofa_method_metadata
        self.assertEqual(metadata["method_name"], "custom_move")

    def test_docstring_is_extracted(self):
        """ Docstring should be stored in metadata. """

        class Dummy:
            @ofa_method()
            def move(self, x: float):
                """Move axis to position x."""
                pass

        metadata = Dummy.move._ofa_method_metadata
        self.assertEqual(metadata["docstring"], "Move axis to position x.")

    def test_requires_self_as_first_parameter(self):
        """ Decorator should reject functions without self. """

        with self.assertRaises(TypeError):

            class Dummy:
                @ofa_method()
                def move(x: float):  # no self
                    pass

    def test_rejects_positional_only_parameters(self):
        """ Decorator should reject positional-only parameters. """

        with self.assertRaises(TypeError):

            class Dummy:
                @ofa_method()
                def move(self, x, /):  # positional-only
                    pass

    def test_rejects_var_positional(self):
        """ Decorator should reject *args. """

        with self.assertRaises(TypeError):

            class Dummy:
                @ofa_method()
                def move(self, *args):
                    pass

    def test_rejects_var_keyword(self):
        """ Decorator should reject **kwargs. """

        with self.assertRaises(TypeError):

            class Dummy:
                @ofa_method()
                def move(self, **kwargs):
                    pass

    def test_allows_keyword_only_parameters(self):
        """ Keyword-only parameters should be allowed. """

        class Dummy:
            @ofa_method()
            def move(self, *, x: float, y: float):
                pass

        metadata = Dummy.move._ofa_method_metadata
        self.assertIn("x", metadata["parameters"])
        self.assertIn("y", metadata["parameters"])

    def test_allows_no_annotations(self):
        """ Parameters without annotations should be accepted (annotation=None). """

        class Dummy:
            @ofa_method()
            def move(self, x, y=5):
                pass

        metadata = Dummy.move._ofa_method_metadata

        self.assertIsNone(metadata["parameters"]["x"]["annotation"])
        self.assertTrue(metadata["parameters"]["x"]["required"])

        self.assertIsNone(metadata["parameters"]["y"]["annotation"])
        self.assertEqual(metadata["parameters"]["y"]["default"], 5)
        self.assertFalse(metadata["parameters"]["y"]["required"])
