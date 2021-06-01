Configuring and executing scenarios
===================================
The following sections explain how to configure new scenarios using the provided scenario generator. As of version 1.0
it is possible to create scenarios with single-family homes only. Future versions will also include multi-dwelling
units, commercial and service buildings as well as industry.

The config file
---------------
The config file specifies all parameters that are needed for the setup up. Two config files can be found in the example
folder, which can be used as starter for simulation (SIM) or real-time (RTS) modes. Furthermore, the
example folder contains standard files to create, run and analyze new scenarios.

Each config file is divided into the following categories:

* simulation
* lem
* supplier
* prosumer
* producer
* aggregator
* db_connections

simulation
^^^^^^^^^^
All general parameters that the simulation requires to run are gathered in "simulation". These differ depending on, if
the simulation is real-time or not.

lem
^^^
The section contains all information regarding the setup of the local energy market (LEM). The parameters include, for
example, how the market is cleared, what transaction types are to be modeled and what what types of qualities are traded
on the lem. Please note, that the market can be cleared using different methods at once, however, the market's behavior
is based on the very first provided method.

supplier
^^^^^^^^
The supplier represents the entity that ensures the balance of the market by selling additional energy and buying
surplus energy. The price at which the supplier sells/buys energy sets the boundaries between which the participants
will trade as they would never be willing to pay more than what is guaranteed by the supplier. Likewise they would not
sell their energy at a lower price than the supplier's minimum price.

The simplest possible supplier simply creates a market price floor and ceiling by entering unlimited buy and sell offers
at fixed prices. Limited coupling capacity can be represented by limiting these buy and sell offers.

prosumer
^^^^^^^^
This is the most extensive section as it contains all information for the individual prosumers. The prosumer class
also includes simple consumers. In this case, they are modeled as prosumers that do not own any generation or
flexibility capabilities. It is mainly divided into *general settings*, *plant configuration* and *market agent
configuration*.

The setup starts with **general settings**, which contain, for example, the number of prosumers
that are part of the LEM.

The **household settings** represent the fixed household consumption and their method of forecasting the demand. The
demand can be modeled using either a uniform value for all households or a distribution to simulate differently sized
households.

The **pv settings** determine the share of households with PV as well as the according sizing method and forecast
model.

The **battery settings** can be set either dependently or independently of the PV settings allowing users to own a
battery without having to own a PV-system as well. The settings mainly contain the battery characteristics as well as
the charging method.

The **electric vehicle settings** encompass the share as well as the basic parameters of an electric vehicle and the
corresponding forecasting method to predict the availability and SoC.

The **fixed gen generator settings** offer the possibility to implement a constant generation, which can serve as
base-load generation, which could model run-of-river or CHP generation.

The **model predictive control settings** specify how the mpc will forecast the local electricity price and what its
trading horizon should be.

The **market agent settings** define the prosumer's trading strategy in the market. Additionally, it can be
specified, if the prosumer is willing to pay a premium for electricity of a higher quality, e.g. if he is willing to
pay 20 % more for local energy.

The **metering settings** are mainly relevant for real-time simulations as lemlab allows to operate Hardware-in-the-Loop
(HiL) LEMs. They allow to simulate that meter readings arrive either late or never.

producer
^^^^^^^^
The producer is a simple prosumer with only one generator. This allows the inclusion of a large producer, for example a
community wind or PV farm.

aggregator
^^^^^^^^^^
The aggregator can be used to simulate an agent that trades on the market for several prosumers. The settings allow to
specify which types of prosumers should be aggregated. Furthermore, the forecast method and trading horizon of the
aggregator can be set. Similar to the individual prosumers, the aggregator can be configured with a price premium that
it is willing to pay for energy of higher quality.

db_connections
^^^^^^^^^^^^^^
The database connections contain the setup of the admin, i.e. the manager of the LEM, as well as the one of the market
participants. Depending on the setup, specified by the user, various database platforms can be used.

Adding input data
-----------------
The modeling of market participants requires various input files that give each prosumer concrete values for their
configuration. Some of the files have a specific naming method that must be abode to and is divided into the following
subfolders found under *input_data*:

* balancing_prices
* ev
* ex_post_pricing
* fixedgen
* households
* levy_prices
* pv

.. _balancing_prices:

balancing_prices
^^^^^^^^^^^^^^^^
lemlab allows to model balancing prices either as constant or varying values. When the prices are supposed to vary a
time-series file needs to be provided, which contains the positive and negative balancing prices for every time step.
The file needs to be csv and there is no specific naming scheme to follow. The name of the file is to be provided
in the config file of the simulation in the *lem* section. The form of the data is shown in the following table.

Format: csv (table)

Naming scheme: None

+--------------+--------------------+---------------------------------+---------------------------------+
| Column names |    timestamp       | price_balancing_energy_positive | price_balancing_energy_negative |
+==============+====================+=================================+=================================+
| Unit         |     unix timestamp |                          €/kWh* |                          €/kWh* |
+--------------+--------------------+---------------------------------+---------------------------------+
| Data type    |            integer |                           float |                           float |
+--------------+--------------------+---------------------------------+---------------------------------+
| Description  | current timestamp  | balancing price for procured    | balancing price for fed-in      |
|              |                    | positive energy                 | negative energy                 |
+--------------+--------------------+---------------------------------+---------------------------------+

\*€ can be substituted with any other currency

ev
^^
The folder contains all driving profiles for the EVs. Every EV in the simulation is randomly assigned a driving profile
for the simulation. This occurs in the scenario creation of scenario_manager.py. The file is a csv file and has no
naming scheme as they are randomly chosen.

Format: csv (table)

Naming scheme: None

+--------------+-------------------+---------------------+----------------------+
| Column names |     timestamp     |     availability    |    distance_driven   |
+==============+===================+=====================+======================+
| Unit         |    unix timestamp |                None |                   km |
+--------------+-------------------+---------------------+----------------------+
| Data type    |           integer |             boolean |              integer |
+--------------+-------------------+---------------------+----------------------+
| Description  | current timestamp | 1: EV available     | driven distance      |
|              |                   | 0: EV not available | since last departure |
+--------------+-------------------+---------------------+----------------------+

ex_post_pricing
^^^^^^^^^^^^^^^
The files within the folder describe the clearing for ex-post methods. In the ex-post methods the price for each kWh
is based on the supply-demand-ratio within the LEM for each time step. The file contains a dictionary with two keys,
which specify the price for the various supply-demand-ratios. When the ratio lies between two explicitly specified
ratios the price is interpolated using the two closest values. The name of the file needs to be identical with the
name of the method specified in the config file under *type_pricing_ex_post*.

Format: json (dictionary)

Naming scheme: "[name of pricing type].json"

+-------------+------------------+---------------------------------+
| Dict keys   |       price      |       supply_demand_ratio       |
+=============+==================+=================================+
| Unit        |           €/kWh* |                            None |
+-------------+------------------+---------------------------------+
| Data type   |            float |                           float |
+-------------+------------------+---------------------------------+
| Description | price per kWh    | ratio between supply and demand |
|             |                  | within the LEM                  |
+-------------+------------------+---------------------------------+

.. _fixedgen:

fixedgen
^^^^^^^^
The files contain the power output of the fixed generation. The file is a csv file and has no naming scheme as they are
randomly chosen. The power output is specified as p.u. between 0 and 1 to allow differently sized fixed generation.

Format: csv (table)

Naming scheme: None

+--------------+-------------------+-------------------------+
| Column names |     timestamp     |            power        |
+==============+===================+=========================+
| Unit         |    unix timestamp |                    p.u. |
+--------------+-------------------+-------------------------+
| Data type    |           integer |                   float |
+--------------+-------------------+-------------------------+
| Description  | current timestamp | power output specified  |
|              |                   | per unit between [0,1]  |
+--------------+-------------------+-------------------------+

.. _households:

households
^^^^^^^^^^
The folder contains the household profiles that contain the discrete energy use over the specified time period. Each
time stamp has a specific energy consumption in Wh. This demand is seen as inflexible and needs to be served at all
times. The file is a csv and has a specific naming conventions, which needs to be followed for the automatic scenario
creator to identify the file.

Format: csv

Naming scheme: "hh_[total demand in kWh]_[nth profile with the same demand].csv"

+--------------+-------------------+---------------------+
| Column names |     timestamp     |          power      |
+==============+===================+=====================+
| Unit         |    unix timestamp |                  Wh |
+--------------+-------------------+---------------------+
| Data type    |           integer |             integer |
+--------------+-------------------+---------------------+
| Description  | current timestamp | energy consumption  |
+--------------+-------------------+---------------------+

levy_prices
^^^^^^^^^^^
Similar to `balancing_prices`_ the folder contains files that specify levies for each time step. The file needs to be
csv but there is no naming scheme that needs to be adhered since the file to use for the simulation needs to be written
in the config file. The file is only used when file-based levies are specified as it is also possible to specify
fixed levies in the config file.

Format: csv
Naming scheme: None

+--------------+--------------------+---------------------------------+---------------------------------+
| Column names |    timestamp       | price_energy_levies_positive    | price_energy_levies_negative    |
+==============+====================+=================================+=================================+
| Unit         |     unix timestamp |                          €/kWh* |                          €/kWh* |
+--------------+--------------------+---------------------------------+---------------------------------+
| Data type    |            integer |                           float |                           float |
+--------------+--------------------+---------------------------------+---------------------------------+
| Description  | current timestamp  | levies for energy fed into      | levies for energy taken from    |
|              |                    | the grid                        | the grid                        |
+--------------+--------------------+---------------------------------+---------------------------------+

.. _pv:

pv (incomplete)
^^^^^^^^^^^^^^^
The PV files contain the normalized power output of different PV systems. Similar to `fixedgen`_ the PV profile is
randomly chosen when the prosumer is created within scenario_manager.py. Therefore, there is no specific naming scheme
to follow for now. However, this will change in upcoming releases once the weather data will be implemented. Therefore,
this subsection is still incomplete.

Format: csv

Naming scheme: None

+--------------+-------------------+-------------------------+
| Column names |     timestamp     |            power        |
+==============+===================+=========================+
| Unit         |    unix timestamp |                    p.u. |
+--------------+-------------------+-------------------------+
| Data type    |           integer |                   float |
+--------------+-------------------+-------------------------+
| Description  | current timestamp | power output specified  |
|              |                   | per unit between [0,1]  |
+--------------+-------------------+-------------------------+

.. _weather:

weather (incomplete)
^^^^^^^^^^^^^^^^^^^^
The weather files are linked to `households`_ and `pv`_. Future releases will also link the weather to the heat supply
(e.g. heat pump and CHP). As the files currently do not exist, this section merely serves as information for the reader
that further information will be added in future releases.

Format: json

Naming scheme: tba

Creating a new scenario
-----------------------
This section explains how to create a new
scenario using *scenario_manager.py* with the aid of the example file *sim_1_create_scenario.py* in the subfolder
*code_examples*.

**sim_1_create_scenario.py**::

    import lemlab

    if __name__ == "__main__":
        sim_name = "test_sim"

        scenario = lemlab.Scenario()
        scenario.new_scenario(path_specification="sim_0_config.yaml",
                              scenario_name=f"{sim_name}")


New scenario are created by first calling an instance of the scenario manager. Afterwards, the function *new_scenario*
requires the relative path of the config file that is to be used for the simulation as well as a name of the scenario.
A short text appears in the terminal when the creation of the scenario is completed. The scenario will be saved in the
subfolder *scenarios* under the given scenario name.

Editing an existing scenario
----------------------------
In principal there are two methods to edit an existing scenario. The first is manually by editing the config file within
a scenario. The second is automatically by opening a scenario config file using code, which allows to serialize
scenario editing on the basis of an existing scenario. Both methods will be explained with the aid of the example file
*sim_2_edit_scenario.py* in the subfolder *code_examples*, which contains the latter method. Please note that not all
settings can be changed as some are fundamental for a scenario. In these cases it is best to create a new scenario.

**sim_2_edit_scenario.py**::

    import lemlab
    from ruamel.yaml import YAML

    if __name__ == "__main__":

        sim_name = "test_sim"

        # create new config file from which to edit scenario
        with open(f"../scenarios/{sim_name}/config.yaml") as config_file:
            config = YAML().load(config_file)
        config["aggregator"]["active"] = True
        with open(f"../scenarios/{sim_name}/config_edited.yaml", 'w+') as file:
            YAML().dump(config, file)

        # generate new scenario from edited config
        scenario = lemlab.Scenario()
        scenario.edit_scenario(path_new_config=f"../scenarios/{sim_name}/config_edited.yaml",
                               name_new_scenario="test_sim_with_agg")

Manual editing
^^^^^^^^^^^^^^
Manual editing is most suitable when only one new scenario is to be generated based on an existing scenario as it is a
fast method. Simply navigate to the scenario that you wish to edit and create a copy of the config file within the
folder. Open the copy and edit all settings that you wish to change. Since the new config file already exists, you can
skip the middle part of the code shown above. All you need to do is to create an instance of the scenario and use the
function *edit_scenario*. The function requires you to specify the path of the edited config file as well as the name
of the new scenario. The scenario manager will then create a new scenario based on the existing one.

Automatic editing
^^^^^^^^^^^^^^^^^
Automatic editing is most suitable when several scenarios need to be generated from an existing one as it allows the
use of for-loops. The method differs only slightly from the manual one and is shown in the above code. Since an edited
config file was not created it needs to be done within the code. To do so, the config file of the existing scenario
needs to be imported. Afterwards, the settings can be changed. For example, in the example file the aggregator was set
to True to activate it and trade for the specified prosumers on the market. Naturally, more than one parameter can be
changed at once. The rest of the code is identical with the manual method.

Executing a scenario
--------------------
The execution of a scenario is independent of whether it is real-time or not. However, real-time simulations have a few
more features which will be explained separately.

Non-real-time scenarios
^^^^^^^^^^^^^^^^^^^^^^^
As both methods require the same code to be run, they will be explained with the aid of the example file
*sim_3_run.py* in the subfolder *code_examples*. The file shows the execution code for a non-real-time simulation. The
almost identical code for real-time simulations can be found in *rts_3_start.py*

**sim_3_run_py.**::

    import lemlab

    if __name__ == "__main__":
        sim_name = "test_sim"

        simulation = lemlab.ScenarioExecutor(path_scenario=f"../scenarios/{sim_name}",
                                             path_results=f"../simulation_results/{sim_name}")
        simulation.run()

To run a simulation you first need to create an instance of the scenario executor. The instance requires the relative
path of the scenario as well as the path where to store the simulation results. By using *run* the simulation is
started.

Real-time scenarios
^^^^^^^^^^^^^^^^^^^
Due to their nature, real-time simulations offer a few more features. After a simulation is started, it is possible to
pause the simulation to adjust some settings. An example on how the simulation is paused is shown in *rts_4_pause.py*.
Afterwards, the simulation can be continued as shown in *rts_5_restart.py*. Please note that the edits should occur in
between clearing points as it can otherwise cause issues when trying to restart the simulation. Furthermore, it is
possible to obtain visual information using *rts_6_plot_live.py*. This will export the current results of the simulation
to be plotted using the scenario analyzer, which is explained in :ref:`Analyzing results`. To stop the simulation use the
code provided in *rts_7_stop.py*.