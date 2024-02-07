import pytorch_lightning as pl
import torch

from .module import LassoRegressionModel



class LassoRegressionLightningModel(pl.LightningModule):
    """
    A ``pl.LightningModule`` class that can be used to train the
    ``LassoRegressionModel`` with PyTorch Lightning.

    Parameters
    ----------
    model_kwargs
        Keyword arguments to construct the ``DLinearModel`` to be trained.
    lr
        Learning rate.
    weight_decay
        Weight decay regularization parameter.
    """

    def __init__(
            self, 
            model_kwargs: dict, 
            lr: float = 0.001,
            weight_decay: float = 1e-8
        ):
        super().__init__()
        self.model = LassoRegressionModel(**model_kwargs)
        self.learning_rate = lr
        self.weight_decay = weight_decay
    

    def forward(self, x):
        return self.model.forward(x)
    

    def training_step(self, batch, batch_idx:int):
        inputs, targets = batch
        predictions = self.model(inputs)
        loss = self.model.lasso_loss(predictions, targets)
        self.log('train_loss', loss, on_epoch=True, on_step=False, prog_bar=True)
        return loss
    
    def predict_step(self, batch):
        inputs, target = batch
        return self.model(inputs)
    
    def configure_optimizers(self):
        return torch.optim.Adam(
            self.model.parameters(), 
            lr=self.learning_rate,
            weight_decay=self.weight_decay
        )
