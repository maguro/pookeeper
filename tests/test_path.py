from toolazydogs.zookeeper import zkpath


def test_normpath():
    actual = zkpath.normpath("foo//bar/")
    assert actual == "foo/bar"


def test_normpath_abs():
    actual = zkpath.normpath("/foo//bar/")
    assert actual == "/foo/bar"


def test_normpath_rel():
    def assert_raises(bad):
        try:
            zkpath.normpath("foo/" + bad + "/bar")
        except ValueError:
            pass
        else:
            assert False, "exception not raised"
    yield assert_raises, "."
    yield assert_raises, ".."


def test_join():
    def assert_equal(parts, should_be):
        actual = zkpath.join(*parts)
        assert actual == should_be, actual
    yield assert_equal, ("foo",), "foo"
    yield assert_equal, ("foo", "bar"), "foo/bar"
    yield assert_equal, ("foo/", "bar"), "foo/bar"
    yield assert_equal, ("", "bar"), "bar"
    yield assert_equal, ("foo", "/bar"), "/bar"


def test_isabs():
    def assert_isabs(s, is_abs):
        assert zkpath.isabs(s) == is_abs, s
    yield assert_isabs, "", False
    yield assert_isabs, "foo", False
    yield assert_isabs, "/foo", True


def test_basename():
    def assert_equal(path, basename):
        actual = zkpath.basename(path) 
        assert actual == basename, "{} != {}".format(actual, basename)
    yield assert_equal, "", ""
    yield assert_equal, "/", ""
    yield assert_equal, "foo", "foo"
    yield assert_equal, "/foo/bar", "bar"
    yield assert_equal, "/foo/bar/", ""
