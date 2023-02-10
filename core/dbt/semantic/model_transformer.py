import copy
import logging

from typing import Sequence, Tuple
from dbt.semantic.model import UserConfiguredSemanticModel

## TODO: Add model transformation logic

class ModelTransformer:
    """Helps to make transformations to a model for convenience.
    Generally used to make it more convenient for the user to develop their model.
    """