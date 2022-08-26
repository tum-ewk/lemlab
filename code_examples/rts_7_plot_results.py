import lemlab

# how to plot for a real-time simulation that has been stopped using the end_execution function

if __name__ == "__main__":
    sim_name = "test_rts"

    analysis = lemlab.ScenarioAnalyzer(path_results=f"../simulation_results/{sim_name}",
                                       save_figures=True)
    analysis.run_analysis()
