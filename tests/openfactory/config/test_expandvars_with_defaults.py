import os
import unittest

from openfactory.config import expandvars_with_defaults


class TestExpandVarsWithDefaults(unittest.TestCase):
    """
    Test cases for environment variable expansion with defaults
    """

    def setUp(self):
        """ Save and clear environment before each test """
        self._original_environ = os.environ.copy()
        os.environ.clear()

    def tearDown(self):
        """ Restore environment after each test """
        os.environ.clear()
        os.environ.update(self._original_environ)

    def test_expand_simple_env_var(self):
        """ ${VAR} is replaced when VAR is set """
        os.environ["ENV_VAR"] = "env_value"

        s = "value is ${ENV_VAR}"
        result = expandvars_with_defaults(s)

        self.assertEqual(result, "value is env_value")

    def test_expand_env_var_not_set_no_default(self):
        """ ${VAR} without default becomes empty string when VAR is unset """
        s = "value is ${ENV_VAR}"
        result = expandvars_with_defaults(s)

        self.assertEqual(result, "value is ")

    def test_expand_env_var_with_default(self):
        """ ${VAR:-default} uses env var when set """
        os.environ["ENV_VAR"] = "env_value"

        s = "value is ${ENV_VAR:-default_value}"
        result = expandvars_with_defaults(s)

        self.assertEqual(result, "value is env_value")

    def test_expand_env_var_with_default_not_set(self):
        """ ${VAR:-default} uses default when VAR is unset """
        s = "value is ${ENV_VAR:-default_value}"
        result = expandvars_with_defaults(s)

        self.assertEqual(result, "value is default_value")

    def test_expand_env_var_with_default_empty_string(self):
        """ ${VAR:-default} uses default when VAR is set but empty """
        os.environ["ENV_VAR"] = ""

        s = "value is ${ENV_VAR:-default_value}"
        result = expandvars_with_defaults(s)

        self.assertEqual(result, "value is default_value")

    def test_multiple_env_vars_in_string(self):
        """ Multiple variables in the same string are expanded """
        os.environ["VAR1"] = "one"
        os.environ["VAR2"] = "two"

        s = "${VAR1}, ${VAR2}, ${VAR3:-three}"
        result = expandvars_with_defaults(s)

        self.assertEqual(result, "one, two, three")

    def test_env_var_with_empty_default(self):
        """ ${VAR:-} results in empty string when VAR is unset """
        s = "value is '${ENV_VAR:-}'"
        result = expandvars_with_defaults(s)

        self.assertEqual(result, "value is ''")

    def test_string_without_env_vars(self):
        """ Strings without variables are unchanged """
        s = "plain string with no variables"
        result = expandvars_with_defaults(s)

        self.assertEqual(result, s)
