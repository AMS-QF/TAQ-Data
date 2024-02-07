import pytorch_lightning as pl
import torch
import torch.nn as nn

from torch_models.metadata import Dataset
from torch_models.metadata.pandas import PandasDataset
from torch_models.evaluations.metrics import out_of_sample_R2
from .torch_utils import create_pandas_dataloader

class PyTorchPredictor:


    def __init__(
        self,
        trainer: pl.Trainer,
        prediction_net: nn.Module,
    ) -> None:  
        self.trainer = trainer
        self.prediction_net = prediction_net

    
    def predict(
        self,
        dataset: Dataset
    ):
        if isinstance(dataset, PandasDataset):
            test_loader = create_pandas_dataloader(
                data=dataset,
                batch_size=1000,
                num_workers=7
            )
            return self.trainer.predict(self.prediction_net, test_loader)
        else:
            raise NotImplementedError
    

    def backtest(
        self,
        dataset: Dataset
    ):
        y_predict = self.predict(dataset)[0]
        y_true = dataset.y

        y_predict = y_predict.cpu().detach()
        y_predict = y_predict.numpy()
        print("Out of sample R2:", out_of_sample_R2(y_true, y_predict))
        
