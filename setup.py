# coding: utf-8
from distribute_setup import use_setuptools
use_setuptools()
from os import path
from setuptools import find_packages, setup

from heroshi import __version__

install_requires = [
    'BeautifulSoup',
    'eventlet == 0.9.9',
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
    version=__version__,

    description=u"Web crawler.",
    long_description=open(
        path.join(
            path.dirname(__file__),
            'README'
        )
    ).read(),
    author='Sergey Shepelev',

    classifiers=[
        "Programming Language :: Python",
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
        ],
    },
)
