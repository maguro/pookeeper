"""Path utilities based on :mod:`posixpath`.

:func:`normpath` has been modified not to allow relative paths.

:mod:`posixpath` is part of the Python standard library, and is licensed under
the `Python Software Foundation License <http://docs.python.org/license.html>`_,
which can be linked with libraries of other licenses and allows changes to be
released under different licenses. See LICENSE.txt for a copy of the PSFL.

The original code can be found `here
<http://hg.python.org/releasing/2.7.3/file/7bb96963d067/Lib/posixpath.py>`_.

The following license text refers to changes to the original code:

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


def normpath(path: str) -> str:
    """Normalize path, eliminating double slashes, etc."""
    comps = path.split("/")
    new_comps = []
    for comp in comps:
        if comp == "":
            continue
        if comp in (".", ".."):
            raise ValueError("relative paths not allowed")
        new_comps.append(comp)
    slash = "/" if isinstance(path, str) else "/"
    new_path = slash.join(new_comps)
    if path.startswith("/"):
        return slash + new_path
    return new_path


def join(root: str, *parts: str) -> str:
    """Join two or more pathname components, inserting '/' as needed.
    If any component is an absolute path, all previous path components
    will be discarded.
    """
    path = root
    for part in parts:
        if part.startswith("/"):
            path = part
        elif path == "" or path.endswith("/"):
            path += part
        else:
            path += "/" + part
    return path


def isabs(s: str) -> bool:
    """Test whether a path is absolute."""
    return s.startswith("/")


def basename(p: str) -> str:
    """Returns the final component of a pathname"""
    i = p.rfind("/") + 1
    return p[i:]
