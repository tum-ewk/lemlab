import lemlab

# this example demonstrates the use of the scenario analyzer library
# by showing some simple plots of the results of the demo simulation

if __name__ == "__main__":
    sim_name = "test_sim"

    analysis = lemlab.ScenarioAnalyzer(path_results=f"../simulation_results/{sim_name}",
                                       show_figures=True,
                                       save_figures=True)
    analysis.run_analysis()
