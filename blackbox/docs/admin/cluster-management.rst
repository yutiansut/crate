.. _cluster_management:

==================
Cluster management
==================

This document shows you how to manage your CrateDB clusters.

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
will give you an overview of available options.

.. code-block:: console

    sh$ ./bin/crate-node --help

Synopsis
~~~~~~~~

.. code-block:: console

   bin/crate-node repurpose|unsafe-bootstrap|detach-cluster
   [--ordinal <Integer>] [-E <KeyValuePair>]
   [-h, --help] ([-s, --silent] | [-v, --verbose])

Command-line options
~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~

The ``crate-node`` command has three modes – ``repurpose``,
``unsafe-bootstrap``, and ``detach-cluster``. You can use these commands to
repurpose nodes, unsafely bootstrap clusters, and detach nodes from clusters.

Repurpose
^^^^^^^^^
* Use ``crate-node repurpose`` to delete data from a node if it used to be a
  data node or a master-eligible node but has been repurposed to have none of
  these roles.

Unsafe-bootstrap
^^^^^^^^^^^^^^^^
* Use ``crate-node unsafe-bootstrap`` to perform unsafe cluster bootstrapping.
  It forces one of the nodes to form a new cluster on its own, using its local
  copy of the cluster metadata.

Detach-cluster
^^^^^^^^^^^^^^
* Use ``crate-node detach-cluster`` to move nodes from one cluster to another.
  You can also use it to move nodes into a new cluster that you have created
  with ``crate-node unsafe-bootstrap``. If ``crate-node unsafe-bootstrap`` was
  not possible, you can use this mode to move nodes into a brand-new cluster.

.. NOTE::

  The message `Node was successfully detached from the cluster` means that the
  ``detach-cluster`` tool successfully completed its job. It does not say that there
  has not been a data loss.

Parameters
~~~~~~~~~~

* ``repurpose``
    Delete excess data when a node’s role is changed.

* ``unsafe-bootstrap``
    Unsafely bootstrap this node as a new one-node cluster.

* ``detach-cluster``
    Specify to unsafely detach this node from its cluster so it can join
    another cluster.

* ``--ordinal <Integer>``
    Specify which node to target if there is more than one node sharing a data
    path. Defaults to 0, meaning to use the first node in the data path.

* ``-C <KeyValuePair>``
    Configure a setting.

* ``-h, --help``
    Return all of the command parameters.

* ``-s, --silent``
    Show minimal output.

* ``-v, --verbose``
    Show verbose output.

Examples
--------

For how-tos and usage examples regarding the ``crate-node`` command, please
refer to `Troubleshooting with the crate-node command`_.

.. _Troubleshooting with the crate-node command: https://crate.io/docs/crate/guide/en/latest/best-practices/crate-node.html
