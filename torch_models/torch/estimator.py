import logging
import pytorch_lightning as pl
import torch.nn as nn
from typing import Optional, Dict, Any, Iterable

from torch_models.metadata import Dataset
from .predictor import PyTorchPredictor

logger = logging.getLogger(__name__)


class Estimator:
    """
    An abstract class representing a trainable model.

    The underlying model is trained by calling the `train` method with a
    training `Dataset`, producing a `Predictor` object.
    """

    def __init__(self, **kwargs) -> None:
        pass

    def train(
        self,
        training_data: Dataset,
        validation_data: Optional[Dataset] = None
    ):
        """
        Train the estimator on the given dataset.

        Parameters
        ----------
        training_data
            Dataset to train the model on.
        validation_data
            Optional dataset to validate the model on during training.

        Returns
        -------
        Predictor
            The predictor containing the trained model.
        """
        raise NotImplementedError
    


class PyTorchEstimator(Estimator):
    """
    An `Estimator` type abstract class with utilities for creating PyTorch-Lightning-based
    models.
    """

    def __init__(
        self,
        trainer_kwargs: Dict[str, Any]
    ) -> None:
        self.trainer_kwargs = trainer_kwargs
    

    def create_lightning_module(self) -> pl.LightningModule:
        """
        Create and return the network used for training (i.e., computing the
        loss).

        Returns
        -------
        pl.LightningModule
            The network that computes the loss given input data.
        """
        raise NotImplementedError
    

    def create_predictor(
        self,
        trainer: pl.Trainer,
        module: pl.LightningModule,
    ) -> PyTorchPredictor:
        """
        Create and return a predictor object.

        Parameters
        ----------
        module
            A trained `pl.LightningModule` object.

        Returns
        -------
        Predictor
            A predictor wrapping a `nn.Module` used for inference.
        """
        raise NotImplementedError

    def create_training_data_loader(
        self, 
        data: Dataset,
        **kwargs
    ) -> Iterable:
        """
        Create a training data loader

        Parameters
        ----------
        data
            Dataset from which to create the data loader.

        Returns
        -------
        Iterable
            The data loader, i.e. and iterable over batches of data.
        """
        raise NotImplementedError
    
    def create_validation_data_loader(
        self, 
        data: Dataset, 
        **kwargs
    ) -> Iterable:
        """
        Create a validation data loader.

        Parameters
        ----------
        data
            Dataset from which to create the data loader.

        Returns
        -------
        Iterable
            The data loader, i.e. and iterable over batches of data.
        """
        raise NotImplementedError

    def train_model(
        self,
        train_data: Dataset,
        validation_data: Optional[Dataset] = None,
        cache_data: bool = False,
        ckpt_path: Optional[str] = None,
        **kwargs,
    ) -> PyTorchPredictor:
        
        train_network = self.create_lightning_module()
        train_dataloader = self.create_training_data_loader(train_data)
        val_dataloader = None
        if validation_data:
            val_dataloader = self.create_validation_data_loader(validation_data)
        
        monitor = "train_loss" if validation_data is None else "val_loss"
        checkpoint = pl.callbacks.ModelCheckpoint(
            monitor=monitor, mode="min", verbose=True
        )

        # custom_callbacks = self.trainer_kwargs.pop("callbacks", [])
        trainer = pl.Trainer(
            accelerator="auto",
            callbacks=checkpoint,
            **self.trainer_kwargs,
        )

        trainer.fit(
            model=train_network,
            train_dataloaders=train_dataloader,
            val_dataloaders=val_dataloader,
            ckpt_path=ckpt_path,
        )
        
        # Add loading best model
        
        best_model = train_network
    
        return self.create_predictor(trainer, best_model)
      

    def train(
        self,
        train_data: Dataset,
        validation_data: Optional[Dataset] = None,
        cache_data: bool = False,
        ckpt_path: Optional[str] = None,
        **kwargs,
    ) -> PyTorchPredictor:
        return self.train_model(
            train_data,
            validation_data,
            cache_data,
            ckpt_path
        )