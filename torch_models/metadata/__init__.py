from typing import runtime_checkable, Protocol


@runtime_checkable
class Dataset(Protocol):
    """
    Abstract class for dataset that is used for model training
    """

    def __len__(self) -> int:
        raise NotImplementedError