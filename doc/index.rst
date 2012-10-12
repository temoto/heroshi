Heroshi documentation
=====================

Heroshi is a `web crawler <http://en.wikipedia.org/wiki/Web_crawler>`_.

The goal of the project is to build very fast, distributed web spider.

Current status:

* low level HTTP client – alpha, usable for test runs
* URL queue – not started

Project is under heavy development, so expect big changes.


Download
--------

`Heroshi source code <https://github.com/temoto/heroshi>`_ is hosted on `Github <http://github.com/>`_, so you may use either

* go get/install::

    go install github.com/temoto/heroshi/heroshi-worker

* clone repository to hack around::

    git clone git://github.com/temoto/heroshi.git

* or download `latest Heroshi master tarball <http://github.com/temoto/heroshi/tarball/master>`_.


Identity
--------

Heroshi identifies itself with::

    User-Agent: HeroshiBot/version (+http://temoto.github.com/heroshi/; temotor@gmail.com)


Load problems
-------------

Heroshi worker doesn't open more than 1 concurrent connection to each domain:port. This is a very low load
to properly configured websites but the world is not perfect, and it may hurt legacy installations.

Heroshi was not meant to be a harm tool, it will not abuse your servers again and again continuously.
Instead, it will wait for some time before visiting same pages again.

So far i believe i'm the only one who runs Heroshi, so if it loads your website too much,
there is no need to ban User-agent/IP or something, just contact me, and i'll set up as low limit
for your website/domain/IP, as acceptable.


Robots.txt support
------------------

Heroshi obeys `standard robots.txt rules <http://www.robotstxt.org/robotstxt.html>`_. As implemented by `Go robots.txt library <https://github.com/temoto/robotstxt.go>`_.

To completely disallow Heroshi crawl your site, place the following lines into file,
accessible as /robots.txt on your site::

    User-agent: HeroshiBot
    Disallow: /


Contact information
-------------------

Use this email (XMPP/Jabber too) for questions/demands/reports about Heroshi: temotor@gmail.com


License
-------

Heroshi is made available under the terms of the open source `MIT license <http://www.opensource.org/licenses/mit-license.php>`_.


Contents
========

.. toctree::
   :maxdepth: 2

   worker
   storage


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

