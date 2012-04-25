"""
Utility functions
"""

import os
import os.path
import shutil
import urllib
import urlparse

def clean_dir(dirr):
    """
    Remove a directory if it already exists.  Then create it and any necessary
    parent directories.
    """
    if os.path.exists(dirr):
        shutil.rmtree(dirr)
    os.makedirs(dirr)

def fetch_file(uri, destdir='.'):
    """
    Fetch a file and deposit it in destdir.

    If uri is a path to a local file it is copied.  Otherwise it is fetched
    using the necessary transport (e.g. HTTP) if possible.
    """
    filename = os.path.basename(urlparse.urlparse(uri)[2])
    destfile = os.path.join(destdir, filename)
    if urlparse.urlparse(uri)[0]:
        urllib.urlretrieve(uri, destfile)
    else:
        shutil.copy2(uri, destfile)
