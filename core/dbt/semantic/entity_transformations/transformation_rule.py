
from abc import ABC, abstractmethod

from dbt.semantic.model import UserConfiguredSemanticModel


##TODO: GET RID OF THIS FILE
# We'll be doing this transformation at compile time
# To make sure it is represented in the manifest. UNlike MF
# we want these changes reflected in the long lived manfiest.
