"""L1-Trend Filter Model in PyTorch with Hyperparameter Optimization"""
import torch as th
from typing import Union


class L1TrendFilter:
    """L1-Trend Filter Model in PyTorch"""

    def __init__(
        self,
        x,
        y,
        params: dict[str : Union[float, int, bool]] = {
            "k": 2,
            "seed": 1234,
        },
    ):
        self.x = x
        self.y = y
        self.params = params

        self.device = th.device("cuda") if th.cuda.is_available() else th.device("cpu")
        self.dtype = th.float64

    def fit(self):
        """Trains the L1-Trend Filter Model using the given data and parameters"""

        training_results = None

        return training_results

    def predict(self, t):
        """Predict at new points in time through trend filtering basis functions"""

        return None
