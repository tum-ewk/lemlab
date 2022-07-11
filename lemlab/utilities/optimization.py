__author__ = "sdlumpp"
__credits__ = []
__license__ = ""
__maintainer__ = "sdlumpp"
__email__ = "sebastian.lumpp@tum.de"


class OptimizationManager:
    """
    The OptimizationManager is used by agents to create, edit, and solve optimization problems. Module design allows
    the prosumers and central optimizer to reuse code to construct their respective problems from the same methods
    accessing the same plant configurations.

    Use cases:
        - prosumer real time controllers
        - prosumer model predictive controllers
        - central optimizers for optimality gap determination

        Public methods:

        __init__ :       Create an instance of the Prosumer class from a configuration folder created using the
                         Simulation class

        controller_real_time:
    """

    def __init__(self, type=None, t_override=None):
        """Create an OptimizationManager instance.

        :param path: path to prosumer configuration directory
        :param t_override: pandas Timestamp, if supplied, this parameter forces the MP to use the supplied
                            timestamp, otherwise the current time is used.
        """
        self.opt_model = pyo.ConcreteModel()
        self.type = type

        # variables for real time prosumer controllers

        # variables for prosumer mpc controllers

        # variables for global optimizer

