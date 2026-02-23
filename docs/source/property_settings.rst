================
PropertySettings
================

.. note::

   The ``PropertySettings`` class is not part of the PostgreSQL implementation.
   In the current implementation, per-calculation metadata (such as software
   package, method/functional, k-point settings, etc.) is stored as a JSON
   string in the ``metadata`` column of the ``property_objects`` table and the
   ``configurations`` table.

   To attach metadata to a property, include a ``_metadata`` key in the
   ``property_map`` when calling
   :meth:`~colabfit.tools.pg.database.DataManager.insert_data`. For example:

   .. code-block:: python

       property_map = {
           "energy-forces-stress": {
               "energy": {"field": "energy", "units": "eV"},
               "forces": {"field": "forces", "units": "eV/Ang"},
           },
           "_metadata": {
               "software": {"value": "VASP"},
               "method": {"value": "PBE"},
               "property_keys": {"value": {"cutoff_energy": 520}},
           },
       }

   The ``method`` and ``software`` fields are lifted out of ``_metadata`` and
   stored in their own columns (``method VARCHAR(256)`` and
   ``software VARCHAR(256)``) on the ``property_objects`` table. All remaining
   metadata is serialised as JSON into the ``metadata`` column.
