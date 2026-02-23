===============
Getting started
===============

Tutorial videos
===============
Videos for helping users to get started with :code:`colabfit-tools` have been
created and uploaded to Vimeo:

1. Installation: `link <https://vimeo.com/684477958>`_
2. Overview of database structure: `part 1 link <https://vimeo.com/684478158>`_ and
   `part 2 link <https://vimeo.com/684478223>`_
3. Example using the Si PRX dataset: `link <https://vimeo.com/684478369>`_
4. Dataset exploration example: `link <https://vimeo.com/684478619>`_

Installing colabfit-tools
=========================

Using pip
^^^^^^^^^

Install directly from the GitHub repository using :code:`pip`.

.. code-block:: console

    $ pip install git+https://github.com/colabfit/colabfit-tools.git@master

Setting up PostgreSQL
=====================

ColabFit uses PostgreSQL for its relational database backend. Ensure you have
a PostgreSQL server available and install the package with PostgreSQL support::

    pip install colabfit-tools[pg]

Next steps
==========

* Take a look at the :ref:`Overview` to see how the Database is structured.
* Review the :ref:`Basics of Configurations` to better understand how data is
  stored when it is first loaded in.
* Follow the :ref:`Basic example`
* Continue with the :ref:`QM9 example` and/or the :ref:`Si PRX GAP example`
