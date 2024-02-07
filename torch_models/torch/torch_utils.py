import torch
from torch.utils.data import DataLoader, TensorDataset

from torch_models.metadata.pandas import PandasDataset

def create_pandas_dataloader(
        data:PandasDataset,
        batch_size:int,
        num_workers: int,
        persistent_workers: bool = True
    ):
        X_tensor = torch.tensor(data.X, dtype=data.dtype)
        y_tensor = torch.tensor(data.y, dtype=data.dtype)
        if y_tensor.ndim == 1:
            y_tensor = y_tensor.view(-1, 1)
        dataset = TensorDataset(X_tensor, y_tensor)
        return DataLoader(dataset, 
                        batch_size=batch_size, 
                        num_workers=num_workers,
                        persistent_workers=persistent_workers)