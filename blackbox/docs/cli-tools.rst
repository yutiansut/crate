.. highlight:: sh
.. _cli:
.. _cluster_management:

=========
CLI tools
=========

.. rubric:: Table of contents

.. contents::
   :local:

Crate
=====

Running CrateDB
---------------

This document covers the basics of running CrateDB from the command line.

.. SEEALSO::

   For help installing CrateDB for the first time, check out `Getting Started With CrateDB`_.

   If you're deploying CrateDB, check out the `CrateDB Guide`_.

.. _Getting Started With CrateDB: https://crate.io/docs/crate/getting-started/en/latest/install/index.html
.. _CrateDB Guide: https://crate.io/docs/crate/guide/en/latest/deployment/index.html

Introduction
------------

CrateDB ships with a ``crate`` command in the ``bin`` directory.

The simplest way to start a CrateDB instance is to invoke ``crate`` without
parameters. This will start the process in the foreground.

::

  sh$ ./bin/crate

You can also start CrateDB in the background using the ``-d`` option. When
starting CrateDB in the background it is helpful to write the process ID into a
pid file so you can find out the process id easlily::

  sh$ ./bin/crate -d -p ./crate.pid

To stop the process that is running in the background send the ``TERM`` or
``INT`` signal to it.

::

  sh$ kill -TERM `cat ./crate.pid`

The ``crate`` executable supports the following command line options:

Command-line options
--------------------

+------------------+----------------------------------------------------------+
| Option           | Description                                              |
+==================+==========================================================+
| ``-d``           | Start the daemon in the background                       |
+------------------+----------------------------------------------------------+
| ``-h``           | Print usage information                                  |
+------------------+----------------------------------------------------------+
| ``-p <pidfile>`` | Log the pid to a file                                    |
+------------------+----------------------------------------------------------+
| ``-v``           | Print version information                                |
+------------------+----------------------------------------------------------+
| ``-C``           | Set a CrateDB :ref:`configuration <config>` value        |
|                  | (overrides configuration file)                           |
+------------------+----------------------------------------------------------+
| ``-D``           | Set a Java system property value                         |
+------------------+----------------------------------------------------------+
| ``-X``           | Set a nonstandard java option                            |
+------------------+----------------------------------------------------------+

Example::

  sh$ ./bin/crate -d -p ./crate.pid

.. _cli_signals:

Signal handling
---------------

The CrateDB process can handle the following signals.

+-----------+---------------------------------------------+
| Signal    | Description                                 |
+===========+=============================================+
| ``TERM``  | Stops a running CrateDB process             |
|           |                                             |
|           | ``kill -TERM `cat /path/to/pidfile.pid```   |
|           |                                             |
+-----------+---------------------------------------------+
| ``INT``   | Stops a running CrateDB process             |
|           |                                             |
|           | Same behaviour as ``TERM``.                 |
+-----------+---------------------------------------------+

.. TIP::

    The ``TERM`` signal stops CrateDB immediately. As a result, pending
    requests may fail. To ensure that any pending requests are completed before
    the node is stopped, you can perform a `graceful stop`_ with the
    :ref:`DECOMMISSION <alter_cluster_decommission>` statement instead.

Crate-node
==========

Managing clusters
-----------------

This section shows you how to manage CrateDB clusters from the command line.

.. SEEALSO::

   For step-by-step how-tos and examples on how to troubleshoot CrateDB clusters and
   nodes from the command line, please refer to `Troubleshooting with crate-node CLI`_.

Introduction
------------

CrateDB provides a ``crate-node`` command, in the ``bin`` directory, that lets
you form new clusters, repair broken clusters, and repair broken nodes.

With this command you can:

 * Unsafely bootstrap a cluster
 * Detach a node from its cluster
 * Change the role of a node

.. NOTE::

  Cluster management operations on a node are only possible while the node is
  shut down.

A good way to start is to invoke ``crate-node`` with the ``-h`` option. This
will give you an overview of the available options.

.. code-block:: console

    sh$ ./bin/crate-node --help

Synopsis
--------

.. code-block:: console

   bin/crate-node repurpose|unsafe-bootstrap|detach-cluster
   [--ordinal <Integer>] [-E <KeyValuePair>]
   [-h, --help] ([-s, --silent] | [-v, --verbose])

Command-line options
--------------------

The ``crate-node`` executable supports the following command line options:


+------------------------+-------------------------------------------------------+
| Option                 | Description                                           |
+========================+=======================================================+
| ``--ordinal <Integer>``| Specify which node to target, if there is more than   |
|                        | one node sharing a data path                          |
+------------------------+-------------------------------------------------------+
| ``-E <KeyValuePair>``  | Print usage information                               |
+------------------------+-------------------------------------------------------+
| ``-h, --help``         | Return all of the command parameters                  |
+------------------------+-------------------------------------------------------+
| ``-s, --silent``       | Show minimal output                                   |
+------------------------+-------------------------------------------------------+
| ``-v, --verbose``      | Shows verbose output                                  |
+------------------------+-------------------------------------------------------+

Commands
--------

The ``crate-node`` command provides for the three operations ``repurpose``,
``unsafe-bootstrap``, and ``detach-cluster``. You can use these options to
repurpose nodes, unsafely bootstrap clusters, and detach nodes from clusters.

Repurpose
^^^^^^^^^
The ``repurpose`` operation lets you delete data from a node that used to be a
data node, or a master-eligible node, but has been repurposed to have none of
these roles.

  .. code-block:: console

      sh$ crate-node repurpose

Unsafe-bootstrap
^^^^^^^^^^^^^^^^
The ``unsafe-bootstrap`` operation lets you force one of the nodes to form a
new cluster on its own, using its local copy of the cluster metadata. To
perform unsafe cluster bootstrapping, run:

  .. code-block:: console

      sh$ crate-node unsafe-bootstrap

Detach-cluster
^^^^^^^^^^^^^^
The ``detach-cluster`` operation lets you move nodes from one cluster to
another. You can also move nodes into a cluster that you have created using
``unsafe-bootstrap`` operation. If ``unsafe-bootstrap`` was not possible, it
also lets you move nodes into a brand-new cluster.

  .. code-block:: console

      sh$ crate-node detach-cluster

.. _Troubleshooting with crate-node CLI: https://crate.io/docs/crate/guide/en/latest/best-practices/crate-node.html
.. _Rolling Upgrade: http://crate.io/docs/crate/guide/best_practices/rolling_upgrade.html
.. _graceful stop: https://crate.io/docs/crate/guide/en/latest/admin/rolling-upgrade.html#step-2-graceful-stop
