# coding: utf-8
# Original author: Douglas Creager <dcreager@dcreager.net>
# This file is placed into the public domain.

# Changelog:
# 2011-10-20 inverted VERSION/git-describe precedence. Now VERSION file
#     is considered primary. If it does not exist, run “git describe”.

# Calculates the current version number.  It is content of VERSION file
# if one exists. Otherwise use “git describe” output, modified to conform
# to the versioning scheme that setuptools uses.  On success, version is
# stored into VERSION file.  If “git describe” returns an error
# (most likely because we're in an unpacked copy of a release tarball,
# rather than in a git working copy), that's an error.
#
# To use this script, simply import it your setup.py file, and use the
# results of get_git_version() as your package version:
#
# from get_git_version import get_git_version
#
# setup(
#     version=get_git_version(),
#     .
#     .
#     .
# )
#
# This will automatically update the VERSION file, if
# necessary.  Note that the VERSION file should *not* be
# checked into git; please add it to your top-level .gitignore file.
#
# You'll probably want to distribute the VERSION file in your
# sdist tarballs; to do this, just create a MANIFEST.in file that
# contains the following line:
#
#   include VERSION

__all__ = ("get_git_version",)

from subprocess import Popen, PIPE


VERSION_FILENAME = "heroshi/VERSION"


def call_git_describe(abbrev):
    try:
        p = Popen(["git", "describe", "--tags", "--always", "--abbrev={0}".format(abbrev)],
                  stdout=PIPE, stderr=PIPE)
        p.stderr.close()
        line = p.stdout.readlines()[0]
        return line.replace("heads/", "").strip()
    except Exception:
        return None


def read_version_file():
    try:
        with open(VERSION_FILENAME, 'r') as f:
            version = f.readlines()[0]
            return version.strip()
    except Exception:
        return None


def write_version_file(version):
    with open(VERSION_FILENAME, 'w') as f:
        f.write(version + "\n")


def get_git_version(length):
    # Read in the version that's currently in VERSION file.
    version = read_version_file()

    # On any errors fall back to “git describe”.
    if version is None:
        version = call_git_describe(length)

        # If we still don't have anything, that's an error.
        if version is None:
            raise ValueError("Cannot find the version number!")

        write_version_file(version)

    # Finally, return the current version.
    return version
