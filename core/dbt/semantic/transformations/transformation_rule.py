
from abc import ABC, abstractmethod

from dbt.semantic.model import UserConfiguredSemanticModel


class ModelTransformRule(ABC):
    """Encapsulates logic for transforming a model. e.g. add metrics based on measures."""

    @staticmethod
    @abstractmethod
    def transform_model(model: UserConfiguredSemanticModel) -> UserConfiguredSemanticModel:
        """Copy and transform the given model into a new model."""
        pass