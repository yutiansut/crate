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
will give you an overview of the available options.

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
~~~~~~~~

The ``crate-node`` command provides for the three operations ``repurpose``,
``unsafe-bootstrap``, and ``detach-cluster``. You can use these options to
repurpose nodes, unsafely bootstrap clusters, and detach nodes from clusters.

Repurpose
^^^^^^^^^
* The ``repurpose`` operation lets you delete data from a node that used to be a
  data node, or a master-eligible node, but has been repurposed to have none of
  these roles.

  .. code-block:: console

      sh$ crate-node repurpose

Unsafe-bootstrap
^^^^^^^^^^^^^^^^
* The ``unsafe-bootstrap`` operation lets you force one of the nodes to form a
  new cluster on its own, using its local copy of the cluster metadata. To
  perform unsafe cluster bootstrapping, run:

  .. code-block:: console

      sh$ crate-node unsafe-bootstrap

Detach-cluster
^^^^^^^^^^^^^^
*  The ``detach-cluster`` operation lets you move nodes from one cluster to
   another. You can also move nodes into a cluster that you have created using
   ``unsafe-bootstrap`` operation. If ``unsafe-bootstrap`` was not possible, it
   also lets you move nodes into a brand-new cluster.

  .. code-block:: console

      sh$ crate-node detach-cluster

.. SEEALSO::

   For step-by-step how-tos and usage examples, please refer to `Troubleshooting with crate-node CLI`_.

.. _Troubleshooting with crate-node CLI: https://crate.io/docs/crate/guide/en/latest/best-practices/crate-node.html
