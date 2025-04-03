=========
Changelog
=========

0.7.1
=====

Changed
-------
- KAR-1423: Revert ``CreateSubscriptionParameters`` change:

  - Revert the usage of ``CreateSubscriptionParameters`` in ``subscribe()`` method

0.7.0
=====

Changed
-------
- WOM-704: Changes to node tree scanning (``generate_node_dicts_from_server()`` method):

  - 'Physical' namespace is skipped in addition to 'Trace' and 'Parameter' when scanning the 'Server' tree during setup.
  - Any OPC-UA and asyncio timeout exceptions are caught, logged and handled gracefully to allow scanning to continue without crashing.

- WOM-706: Add optional ``sampling_interval`` arg to ``subscribe()`` method:
  
  - The client can now request a sampling interval and the server will use the closest rate that it supports.
  - The default value is ``None``, which means that the server will use its fastest practical rate.

0.6.0
=====

Changed
-------
- WOM-693: Changes to ``subscribe()`` method:

  - The ``period`` argument has been renamed to ``publishing_interval`` to match OPC-UA nomenclature, and no longer has a default value.
  - The client now requests that samples are buffered on the server (at its default rate). All samples are then received concurrently at the set publishing interval. 
  - The 'MinSupportedSampleRate' value of the server is read to set an appropriate queue size for sample buffering. 
  - The optional ``buffer_samples`` argument can be explicitly set to ``False`` to revert to the original behaviour of only receiving the latest sample at the publishing interval. 
  
- WOM-694: Updated ``subscribe()`` method with an optional argument ``trigger_on_change``. If it is set to ``False``, the subscription will trigger on timestamps - i.e. notifications will always be received at the ``publishing_interval``. This is useful for attributes that are not expected to change frequently, but need to be read at a high rate.

Fixed
-----
- WOM-695: Check for connected client before trying to release authority, unsubscribe and disconnect.

0.5.1
=====

Fixed
-----
- WOM-696: Add specific nodes by child names instead of ID for the Karoo simulator.

0.5.0
=====

Changed
-------
- WOM-628: 

  - The 'Server' tree is scanned by default for the ``nodes``, ``attributes`` and ``commands`` dictionaries, which includes the 'PLC_PRG' tree, but the 'Trace' tree is skipped. 
  - Renamed ``plc_prg_nodes_timestamp`` property to just ``nodes_timestamp``.
  - The 'Parameter' tree is no longer scanned by default: Renamed the ``gui_app`` argument of ``SteeringControlUnit`` class (that was used by the DiSQ GUI) to ``scan_parameter_node`` and inverted its logic. It defaults to ``False`` and must be set to ``True`` to scan the 'Parameter' tree.

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
