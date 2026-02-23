.. module:: colabfit.tools.pg.property

========
Property
========

Basics of a Property
====================

A Property stores all of the information about the output of a calculation. A
Property is usually extracted from a
:class:`~colabfit.tools.pg.configuration.AtomicConfiguration` object using the
:meth:`~colabfit.tools.pg.property.Property.from_definition` class method,
which takes a list of property definitions, a configuration, and a
``property_map`` dictionary.

For more details about how data is stored on a Configuration, see
:ref:`Configuration info and arrays fields`. For more details on how to specify
the mapping for loading data off of a Configuration, see
:ref:`Parsing data`.

Metadata such as the software package, DFT functional, or other
calculation-specific information is attached via the ``_metadata`` key in the
``property_map``. See :ref:`PropertySettings` for details.

.. autoclass:: colabfit.tools.pg.property.Property
   :members:
   :special-members:
