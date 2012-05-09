# coding: utf-8
from distribute_setup import use_setuptools
use_setuptools()
import os
from setuptools import find_packages, setup

from get_git_version import get_git_version


install_requires = [
    'eventlet >= 0.9.9',
    'httplib2',
    'psycopg2 >= 2.2.1',
    'webob',
]

try:
    import json
except ImportError:
    install_requires.append("simplejson")

setup(
    name='Heroshi',
    version=get_git_version(length=6),

    description=u"Web crawler.",
    long_description=open(
        os.path.join(
            os.path.dirname(__file__),
            'README'
        )
    ).read(),
    author=u"Sergey Shepelev",

    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Go",
        "Programming Language :: Python",
        "Topic :: Internet :: WWW/HTTP :: Indexing/Search",
    ],

    packages=find_packages(),
    include_package_data=True,
    install_requires=install_requires,
    zip_safe=False,

    entry_points = {
        'console_scripts': [
            'heroshid = heroshi.manager.cli_manager:main',
            'heroshi-append = heroshi.worker.cli_append:main',
            'heroshi-crawl = heroshi.worker.cli_crawl:main',
            'heroshi-get-jobs = heroshi.worker.cli_get_jobs:main',
            'heroshi-report = heroshi.worker.cli_report:main',
        ],
    },
)
