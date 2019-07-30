.. _cluster_management:

==================
Cluster management
==================

With the ``crate-node`` CLI tool you can manage your CrateDB clusters by forming
new clusters, repairing broken clusters, and by repairing broken nodes.

The ``crate-node`` CLI tool
---------------------------

The ``crate-node`` CLI tool lets you:

 * Unsafely bootstrap a cluster
 * Detach a node from its cluster
 * Change the role of a node (and may be able to recover some data after a
   disaster)

.. NOTE::

  Cluster management operations on a node are only possible while the node is shut down.

Synopsis
~~~~~~~~

.. code-block:: console

   bin/crate-node repurpose|unsafe-bootstrap|detach-cluster
   [--ordinal <Integer>] [-C <KeyValuePair>]
   [-h, --help] ([-s, --silent] | [-v, --verbose])

Modes
~~~~~

The ``crate-node`` CLI tool is comprised of the three modes ``repurpose``,
``unsafe-bootstrap``, and ``detach-cluster``. You can use them to manage your
clusters and repurpose nodes, bootstrap clusters, and detach nodes from
clusters.

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

For how-tos and usage examples regarding the ``crate-node`` CLI tool, please
refer to `Troubleshooting with the crate-node CLI tool`_.

.. _Troubleshooting with the crate-node CLI tool: https://crate.io/docs/crate/guide/en/latest/best-practices/crate-node.html
