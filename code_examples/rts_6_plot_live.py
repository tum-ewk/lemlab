import lemlab

# how to create plots from a real-time simulation that is still running

if __name__ == "__main__":
    sim_name = "test_rts"

    # first export the current database state to local CSV files
    simulation = lemlab.ScenarioExecutor(path_scenario=f"../scenarios/{sim_name}",
                                         path_results=f"../simulation_results/{sim_name}")
    simulation.export_database_snapshot()

    # run a normal analyzer
    analysis = lemlab.ScenarioAnalyzer(path_results=f"../simulation_results/{sim_name}",
                                       save_figures=True)
    analysis.run_analysis()
