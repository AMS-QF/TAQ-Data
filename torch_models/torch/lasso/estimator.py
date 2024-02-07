from typing import Iterable, Optional, Dict, Any
import pytorch_lightning as pl
import torch
from torch.utils.data import DataLoader, TensorDataset

from torch_models.metadata import Dataset
from torch_models.metadata.pandas import PandasDataset
from torch_models.torch.predictor import PyTorchPredictor
from torch_models.torch.estimator import PyTorchEstimator
from torch_models.torch.torch_utils import create_pandas_dataloader

from .lightning_module import LassoRegressionLightningModel

class LassoEstimator(PyTorchEstimator):

    def __init__(
        self,
        n_features: int,
        l1_strength: float,
        lr: float = 1e-3,
        weight_decay: float = 1e-8,
        batch_size: int = 1000,
        num_workers: int = 7,
        trainer_kwargs: Optional[Dict[str, Any]] = None
    ) -> None:
    
        default_trainer_kwargs = {
            "max_epochs": 100,
            "gradient_clip_val": None,
        }
        if trainer_kwargs is not None:
            default_trainer_kwargs.update(trainer_kwargs)

        super().__init__(trainer_kwargs=default_trainer_kwargs)

        self.lr = lr
        self.weight_decay = weight_decay
        self.batch_size = batch_size
        self.num_workers = num_workers

        self.model_kwargs = {
            "n_features": n_features,
            "l1_strength": l1_strength
        } 
        
    
    def create_lightning_module(self) -> pl.LightningModule:
        return LassoRegressionLightningModel(
            model_kwargs = self.model_kwargs,
            lr = self.lr,
            weight_decay = self.weight_decay
        )
    

    def create_training_data_loader(
        self, 
        data: Dataset, 
        **kwargs
    ) -> Iterable:
        if isinstance(data, PandasDataset):
            return create_pandas_dataloader(
                    data=data,
                    batch_size=self.batch_size,
                    num_workers=self.num_workers
            )
        else:
            raise NotImplementedError
    

    def create_validation_data_loader(
        self, 
        data: Dataset, 
        **kwargs
    ) -> Iterable:
        if isinstance(data, PandasDataset):
            return create_pandas_dataloader(
                    data=data,
                    batch_size=self.batch_size,
                    num_workers=self.num_workers
            )
        else:
            raise NotImplementedError
    

    def create_predictor(
        self, 
        trainer: pl.Trainer,
        module: pl.LightningModule
    ) -> PyTorchPredictor:
        return PyTorchPredictor(trainer, module)

        