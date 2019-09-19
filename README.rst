==========
cinderella
==========

Cleans up after your ugly sisters

Description
===========

Cleans stale metrics from prometheus, you can use the environmental variables:

- ``PROMETHEUS_URL``, to set the URL that prometheus listens on, defaults to: ``http://localhost:9090``.
- ``PROMETHEUS_HEAD``, to set the headers

Examples
========

Top ten metrics by number of time series:

.. code-block::
    cinderella top 10

Only keep one week of `node_systemd_unit_state`:

.. code-block::
    cinderella delete 'node_systemd_unit_state' 1w

List all metrics:

.. code-block::
    cinderella list
