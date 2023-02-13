"""Classes required for defining metric definition object instances (see MdoInstance)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, TypeVar, Generic, Tuple

from dbt.dbt_semantic.aggregation_types import AggregationState
from dbt.dataclass_schema import dbtClassMixin
from dbt.dbt_semantic.references import ElementReference

from metricflow.column_assoc import ColumnAssociation
from metricflow.dataclass_serialization import SerializableDataclass
from metricflow.references import ElementReference
from metricflow.specs import (
    MetadataSpec,
    MeasureSpec,
    DimensionSpec,
    IdentifierSpec,
    MetricSpec,
    InstanceSpec,
    TimeDimensionSpec,
    InstanceSpecSet,
)