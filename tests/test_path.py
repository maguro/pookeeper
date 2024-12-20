"""
Copyright 2012 the original author or authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import pytest

from pookeeper import zkpath


def test_normpath():
    actual = zkpath.normpath("foo//bar/")
    assert actual == "foo/bar"


def test_normpath_abs():
    actual = zkpath.normpath("/foo//bar/")
    assert actual == "/foo/bar"


@pytest.mark.parametrize("bad", [".", ".."])
def test_normpath_rel(bad):
    try:
        zkpath.normpath("foo/" + bad + "/bar")
    except ValueError:
        pass
    else:
        assert False, "exception not raised"


@pytest.mark.parametrize(
    "parts,should_be",
    [
        (("foo",), "foo"),
        (("foo", "bar"), "foo/bar"),
        (("foo/", "bar"), "foo/bar"),
        (("", "bar"), "bar"),
        (("foo", "/bar"), "/bar"),
    ],
)
def test_join(parts, should_be):
    actual = zkpath.join(*parts)
    assert actual == should_be, actual


@pytest.mark.parametrize("s,is_abs", [("", False), ("foo", False), ("/foo", True)])
def test_isabs(s, is_abs):
    assert zkpath.isabs(s) == is_abs, s


@pytest.mark.parametrize("test_input,expected", [("3+5", 8), ("2+4", 6), ("6*9", 54)])
def test_eval(test_input, expected):
    assert eval(test_input) == expected


@pytest.mark.parametrize("path,basename", [("", ""), ("/", ""), ("foo", "foo"), ("/foo/bar", "bar"), ("/foo/bar/", "")])
def test_basename(path, basename):
    actual = zkpath.basename(path)
    assert actual == basename, f"{actual} != {basename}"
