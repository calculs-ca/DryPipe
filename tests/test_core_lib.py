import unittest

from dry_pipe.core_lib import expandvars_from_dict


class TestExpandVars(unittest.TestCase):


    def test(self):

        # Replace with os.environ as desired
        envars = {"foo": "bar", "baz": "$Baz"}

        tests = {r"foo": r"foo",
                 r"$foo": r"bar",
                 r"$$": r"$$",  # This could be considered a bug
                 r"$$foo": r"$bar",  # This could be considered a bug
                 r"\n$foo\r": r"nbarr",  # This could be considered a bug
                 r"$bar": r"",
                 r"$baz": r"$Baz",
                 r"bar$foo": r"barbar",
                 r"$foo$foo": r"barbar",
                 r"$foobar": r"",
                 r"$foo bar": r"bar bar",
                 r"$foo-Bar": r"bar-Bar",
                 r"$foo_Bar": r"",
                 r"${foo}bar": r"barbar",
                 r"baz${foo}bar": r"bazbarbar",
                 r"foo\$baz": r"foo$baz",
                 r"foo\\$baz": r"foo\$Baz",
                 r"\$baz": r"$baz",
                 r"\\$foo": r"\bar",
                 r"\\\$foo": r"\$foo",
                 r"\\\\$foo": r"\\bar",
                 r"\\\\\$foo": r"\\$foo"}

        for t, v in tests.items():
            g = expandvars_from_dict(t, envars)
            self.assertEqual(g, v)
