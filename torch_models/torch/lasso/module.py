import torch
import torch.nn as nn


class LassoRegressionModel(nn.Module):
    """
    Module implementing a Lasso Regression model in the paper:
    https://www.nber.org/system/files/working_papers/w30366/w30366.pdf

    Parameters
    ----------
    n_features
        Number of features used to predict.
    l1_strength
        The L1 regularization term or lambda.
    """

    def __init__(
        self, 
        n_features: int, 
        l1_strength: float
    ) -> None:
        super().__init__()
        
        assert n_features >= 1

        self.linear = nn.Linear(n_features, 1, bias=True)
        self.l1_strength = l1_strength
    

    def forward(self, x):
        return self.linear(x)

    
    def lasso_loss(
        self, 
        predictions: torch.tensor, 
        targets: torch.tensor
    ) -> torch.tensor:
        l1_regularization = self.l1_strength * torch.sum(torch.abs(self.linear.weight))
        mse_loss = nn.functional.mse_loss(predictions, targets)
        return mse_loss + l1_regularization