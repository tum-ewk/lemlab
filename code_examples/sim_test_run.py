import lemlab

if __name__ == "__main__":
    # initialise new Scenario based on ....yml and named demo_scenario
    # this will create all required scenario describing input files in the
    # scenario folder
    sim_name = "test_run_no_rts"
    scenario = lemlab.Scenario()
    scenario.new_scenario(path_specification="./sim_test_config.yaml",
                          scenario_name=f"{sim_name}")

    # execute the demo scenario as defined in the files in the scenario folder
    simulation = lemlab.ScenarioExecutor(path_scenario=f"../scenarios/{sim_name}",
                                         path_results=f"../simulation_results/{sim_name}")
    simulation.run()

    # run basic analysis
    # analysis = lemlab.ScenarioAnalyzer(path_results="./simulation_results/test_sdl")
    # analysis.run_analysis()
