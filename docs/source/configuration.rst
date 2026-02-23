.. module:: colabfit.tools.pg.configuration

=============
Configuration
=============

Configuration info and arrays fields
=====================================================

Configurations inherit directly from `ase.Atoms
<https://wiki.fysik.dtu.dk/ase/ase/atoms.html?highlight=self%20arrays#the-atoms-object>`_ objects.
Because of this, an AtomicConfiguration can use its :attr:`info` and :attr:`arrays`
dictionaries to store a large variety of information about the inputs/outputs
and metadata of a calculation.

* The :attr:`AtomicConfiguration.info` dictionary uses strings as keys, and stores any
  arbitrary information about the Configuration as a whole (e.g., its name,
  computed energy, metadata labels, ...)
* The :attr:`AtomicConfiguration.arrays` dictionary uses strings as keys, and stores
  arrays of per-atom data (e.g., atomic numbers, positions, computed forces, ...)

Basics of Configurations
========================

A :class:`AtomicConfiguration` stores all of the information about an atomic
structure, i.e., the input to a calculation. The :class:`AtomicConfiguration` class
inherits from the :class:`ase.Atoms` class, and populates required fields
in its :attr:`Atoms.info` dictionary.

Required fields:

* :attr:`~colabfit.ATOMS_NAME_FIELD` = :code:`"_name"`:
    A string or list of strings identifying the configuration.
* :attr:`~colabfit.ATOMS_LABELS_FIELD` = :code:`"_labels"`:
    A list of short strings used for providing queryable metadata about the
    configuration.

Building Configurations
=======================

The most common way to create an :class:`AtomicConfiguration` is from an existing
:code:`ase.Atoms` object:

.. code-block:: python

   from colabfit.tools.pg.configuration import AtomicConfiguration

   config = AtomicConfiguration.from_ase(atoms, co_md_map=co_md_map)

.. autoclass:: colabfit.tools.pg.configuration.AtomicConfiguration
    :members:
    :special-members:
