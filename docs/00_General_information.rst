General information
===================

:Authors: `Sebastian D. Lumpp`_, `Michel Zadé`_, `Markus Doepfert`_
:Organization: `Chair of Energy Economy and Application Technology`_, Technical University of Munich
:Version: 1.0
:Date: 01.06.2021
:Copyright: The model code is licensed under the `GNU General Public License 3.0`_.
            This documentation is licensed under a `Creative Commons Attribution 4.0 International`_ license.

Description
--------
lemlab is an open-source tool for the agent-based development and testing of local energy market applications offering:

* a fully open-source, agent-based local energy market modelling toolbox
* a modular and extendable design for easy adaptation to your own research questions
* real-time capabilities for the development and testing of hard- and software
* a database-agnostic approach that enables the integration of multiple database technologies
* integrated time-series data for several plant types (household loads, pv, wind, heat pumps, electric vehicles etc...)
* template functionality for load and generation forecasting, trading strategies, cutting-edge market clearing
  algorithms designed specifically for LEMs [paper under review] and much more...

Changes
-------
01.06.2021 - first release

Dependencies
------------
* `Python`_ please see lemlab-env.yaml for virtual environment configuration
* Any solver supported by pyomo. We suggest `gurobi`_ or `cplex`_
* PostgreSQL


.. _Sebastian D. Lumpp: sebastian.lumpp@tum.de
.. _Michel Zadé: michel.zade@tum.de
.. _Markus Doepfert: markus.doepfert@tum.de
.. _Chair of Energy Economy and Application Technology: https://www.ei.tum.de/en/ewk/
.. _GNU General Public License 3.0: https://www.gnu.org/licenses/gpl-3.0
.. _Creative Commons Attribution 4.0 International: https://creativecommons.org/licenses/by/4.0/
.. _Python: https://www.python.org/
.. _gurobi: https://www.gurobi.com/
.. _cplex: https://www.ibm.com/analytics/cplex-optimizer