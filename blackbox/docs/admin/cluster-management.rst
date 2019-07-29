.. _cluster_management:

==================
Cluster management
==================

With the ``crate-node`` command and the crate-node CLI tool, you can manage your
CrateDB clusters by forming new clusters, repairing broken clusters, and broken
nodes.

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

This section introduces you to the ``crate-node`` command that is comprised of
the three modes ``repurpose``, ``unsafe-bootstrap``, and ``detach cluster``.
These modes allow you to execute all of the above CrateDB cluster management
operations.

* Use ``crate-node repurpose`` to delete data from a node if it used to be a
  data node or a master-eligible node but has been repurposed to have none of
  these roles.

* Use ``crate-node detach-cluster`` to move nodes from one cluster to another.
  You can also use it to move nodes into a new cluster that you have created
  with ``crate-node unsafe-bootstrap``. If ``crate-node unsafe-bootstrap`` was
  not possible, you can use this mode to move nodes into a brand-new cluster.

* Use ``crate-node unsafe-bootstrap`` to perform unsafe cluster bootstrapping.
  It forces one of the nodes to form a new cluster on its own, using its local
  copy of the cluster metadata.

.. Examples for the use of the crate-node CLI tool
.. -----------------------------------------------

.. Find further, more detailed use-case examples and how-tos on the
.. ``crate-node`` command at :doc:`crate-node.rst`.
