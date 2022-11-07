from lemlab import ScenarioExecutor

# how to start a real-time simulation

if __name__ == "__main__":
    sim_name = "test_rts"

    simulation = ScenarioExecutor(path_scenario=f"../scenarios/{sim_name}",
                                  path_results=f"../simulation_results/{sim_name}")
    simulation.run()
