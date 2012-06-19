"""Path utilities based on :mod:`posixpath`.
"""

def normpath(path):
    """Normalize path, eliminating double slashes, etc. 
    """
    comps = path.split("/")
    new_comps = []
    for comp in comps:
        if comp == "":
            continue
        if comp in (".", ".."):
            raise ValueError("relative paths not allowed")
        new_comps.append(comp)
    slash = u"/" if isinstance(path, unicode) else "/"
    new_path = slash.join(new_comps)
    if path.startswith("/"):
        return slash + new_path
    return new_path


def join(a, *p):
    """Join two or more pathname components, inserting '/' as needed.
    If any component is an absolute path, all previous path components
    will be discarded.
    """
    path = a
    for b in p:
        if b.startswith('/'):
            path = b
        elif path == '' or path.endswith('/'):
            path +=  b
        else:
            path += '/' + b
    return path


def isabs(s):
    """Test whether a path is absolute. """
    return s.startswith("/")


def basename(p):
    """Returns the final component of a pathname"""
    i = p.rfind('/') + 1
    return p[i:]
