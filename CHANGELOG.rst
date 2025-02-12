=========
Changelog
=========

0.4.0
=====

Added
-----
- WOM-626: Added 'System.DisplayedDiagnosis' to ``nodes`` and ``attributes`` dictionaries.

Changed
-------
- WOM-359: Optimised OPCUA server tree scanning method ``generate_node_dicts_from_server()``, as well as ``get_command_arguments()``.
- WOM-625: Refactored command function creation to work for any server methods, not just 'PLC_PRG' commands. Also changed ``get_command_arguments()`` to take ``Node`` object as argument, to allow DiSQ to get inputs for any server method.

0.3.0
=====

Added
-----
- WOM-593: Added ``get_command_arguments()`` method that, for a given command, returns a list of tuples with each input argument's name and its OPCUA data type.

0.2.0
=====

Added
-----
- WOM-511: Added attribute data type caching.
- KAR-1302: Added an option to reuse a ``SubscriptionHandler`` instance, rather than creating a new one for every subscription.

0.1.2
=====

Fixed
-----
- WOM-424: Added missing 'packaging' dependency.
- WOM-454: Prevent ``subscribe()`` from failing if none of the input attributes are in the node dict and lower logging call level in method.

0.1.1
=====

Fixed
-----
- WOM-520: Fix track load method with the track load node parent's call_method.

0.1.0
=====

Added
-----
- WOM-386, WOM-445: Added new ``StaticPointingModel`` class for the import/export of a global static pointing model from/to a JSON file.
- WOM-446, WOM-464: Added missing commands to the ``Command`` enum.
- WOM-464: Added ``ServerStatus.CurrentTime`` key to ``nodes`` and ``attributes`` properties that reads the server's local time.

Changed
-------
- WOM-484: Updated ``subscribe()`` to subscribe to all input nodes in one OPCUA call.

Removed
-------
- WOM-479: Removed redundant ``get_enum_strings()`` method. Use ``get_attribute_data_type()`` instead.

Fixed
-----
- WOM-479: Updated ``get_attribute_data_type()`` to fix exceptions in DiSQ.
- WOM-506: Updated the command authority checks to not block sending a 'TakeAuth' or 'ReleaseAuth' command under any circumstance.
- WOM-492: Catch ``ConnectionError`` exception when trying to unsubscribe after connection has been closed.
- WOM-509: Manually create enumeration data types from nodes for CETC simulator v4.4 compatibility.

Documentation
-------------
- KAR-1198: Updated 'How to use SCU'.

Older history
=============

The source files of this project were migrated from the `ska-mid-disq 
<https://gitlab.com/ska-telescope/ska-mid-disq>`_ project on 10 Sept 2024, 
maintaining the commit history of `sculib.py` (WOM-471).
