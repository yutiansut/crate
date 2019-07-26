.. _cluster_management:

==================
Cluster management
==================

.. rubric:: Table of contents

.. contents::
   :local:

With the ``crate-node`` command and the crate-node CLI tool, you can manage your
CrateDB clusters in the sense of forming new clusters, repairing broken
clusters, and broken nodes.

This section introduces you to the ``crate-node`` command that comprises the
three modes ``repurpose``, ``unsafe-bootstrap``, and ``detach cluster`` at hand,
to execute all of the above CrateDB cluster management operations.

.. rubric:: Table of contents

.. contents::
   :local:

The ``crate-node`` command
--------------------------

The ``crate-node`` command enables you to:

 * Unsafely bootstrap a cluster
 * Detach a node from its cluster
 * Change the role of a node
 * Eventually recover some data after a disaster

Synopsis
~~~~~~~~

.. code-block:: console

   bin/crate-node repurpose|detach-cluster|unsafe-bootstrap
   [--ordinal <Integer>] [-C <KeyValuePair>]
   [-h, --help] ([-s, --silent] | [-v, --verbose])

Modes
~~~~~

The following list shows the commands to modify and recover nodes and when to use
which mode in a troubleshooting situation:

* Use ``crate-node repurpose`` to delete data from a node if it used to be a
  data node or a master-eligible node but has been repurposed to have none of
  these roles.

* Use ``crate-node detach-cluster`` to move nodes from one cluster to another,
  move nodes into a new cluster created with ``crate-node unsafe-bootstrap``,
  and move nodes into a brand-new cluster if ``crate-node unsafe-bootstrap`` was
  not possible.

  * Use ``crate-node unsafe-bootstrap`` to perform unsafe cluster bootstrapping.
    It forces one of the nodes to form a new cluster on its own, using its local
    copy of the cluster metadata.

Troubleshooting with the crate-node CLI tool
--------------------------------------------

For how-tos and use-case examples respecting the ``crate-node`` command please
refer to :ref:`crate-node-cli`.
