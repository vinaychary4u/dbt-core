from abc import ABC 
from collections import defaultdict
from typing import Dict, List
from dataclasses import field
from dbt.contracts.graph.nodes import Entity
from dbt.dbt_semantic.objects.dimensions import Dimension, DimensionType
from dbt.dbt_semantic.validations.unique_valid_names import UniqueAndValidNames
from dbt.dbt_semantic.time import TimeGranularity
from dbt.dbt_semantic.references import DimensionReference, MeasureReference
from dbt.dbt_semantic.validations.validation_helpers import DimensionInvariants
from dbt.dbt_semantic.validations.unique_valid_names import UniqueAndValidNames
from dbt.exceptions import DbtSemanticValidationError


class EntityValidator:
    """This class exists to contain the functions we use to validate entities,
    identifiers, measures, and dimensions. It is called in schemas.py after 
    entity parsing to validate them."""

    @staticmethod
    def validate_entity(manifest_entities): # noqa: D
        
        ## Entity Validation
        UniqueAndValidNames._validate_entities(manifest_entities)
        MeasureValidator._validate_measure_unique(manifest_entities)

        for entity in manifest_entities:
            ## Dimension Validation
            dimension_to_invariant: Dict[DimensionReference, DimensionInvariants] = {}
            time_dims_to_granularity: Dict[DimensionReference, TimeGranularity] = {}
            DimensionValidator._validate_entity(
                        entity=entity,
                        dimension_to_invariant=dimension_to_invariant,
                        update_invariant_dict=True
                    )
            for dimension in entity.dimensions:
                # breakpoint()
                DimensionValidator._validate_time_dimension(
                        dimension=dimension,
                        time_dims_to_granularity=time_dims_to_granularity,
                        entity=entity,
                    )

class MeasureValidator:
    """This class exists to validate measures"""

    @staticmethod
    def _validate_measure_unique(manifest_entities):
        """Asserts all measure names are unique across the entire model."""
        validation_errors=[]
        measure_references_to_entities: Dict[MeasureReference, List] = defaultdict(list)
        
        entities_with_measures = (entity for entity in manifest_entities if entity.measures)
 
        for entity in entities_with_measures:
            for measure in entity.measures:
                if measure.reference in measure_references_to_entities:
                    validation_errors.append(
                        f"Measure with name {measure.name} found in multiple entities "
                        f"with names ({measure_references_to_entities[measure.reference]})"
                    )

                measure_references_to_entities[measure.reference].append(entity.name)
       
        if validation_errors:
            raise DbtSemanticValidationError(
                f"Errors: {', '.join(e for e in validation_errors)}"
            )


class DimensionValidator:
    """This class exists to validate dimensions
    Replaces dimension_const.py"""

    @staticmethod
    def _validate_time_dimension(
        dimension:Dimension,
        time_dims_to_granularity: Dict[DimensionReference, TimeGranularity],
        entity: Entity
    ):
        """Checks that time dimensions of the same name that aren't primary have the same time granularity specifications

        ##TODO: FIX
        Args:
            dimension: the dimension to check
            time_dims_to_granularity: a dict from the dimension to the time granularity it should have
            entity: the associated entity. Used for generated issue messages
        """
        if dimension.type == DimensionType.TIME:
            if dimension.reference not in time_dims_to_granularity and dimension.type_params:
                time_dims_to_granularity[dimension.reference] = dimension.type_params.time_granularity
            
            else: 
                # The primary time dimension can be of different time granularities, so don't check for it.
                if (
                    dimension.type_params is not None
                    and not dimension.type_params.is_primary
                    and dimension.type_params.time_granularity != time_dims_to_granularity[dimension.reference]
                ):
                    expected_granularity = time_dims_to_granularity[dimension.reference]
                    raise DbtSemanticValidationError(
                        f"Time granularity must be the same for time dimensions with the same name. "
                        f"Problematic dimension: {dimension.name} in entity with name: "
                        f"`{entity.name}`. Expected granularity is {expected_granularity.name} but "
                        f"received granularity {dimension.type_params.time_granularity}."
                    )

    @staticmethod
    def _validate_entity(
        entity: Entity,
        dimension_to_invariant: Dict[DimensionReference, DimensionInvariants],
        update_invariant_dict: bool,
    ):
        """Checks that the given entity has dimensions consistent with the given invariants.
        Args:
            entity: the entity to check
            dimension_to_invariant: a dict from the dimension name to the properties it should have
            update_invariant_dict: whether to insert an entry into the dict if the given dimension name doesn't exist.
        """

        validation_errors=[]
        for dimension in entity.dimensions:
            dimension_invariant = dimension_to_invariant.get(dimension.reference)

            if dimension_invariant is None:
                if update_invariant_dict:
                    dimension_invariant = DimensionInvariants(dimension.type, dimension.is_partition or False)
                    dimension_to_invariant[dimension.reference] = dimension_invariant
                    continue
                # TODO: Can't check for unknown dimensions easily as the name follows <id>__<name> format.
                # e.g. user__created_at
                continue
            # is_partition might not be specified in the configs, so default to False.
            is_partition = dimension.is_partition or False
            
            if dimension_invariant.type != dimension.type:
                        validation_errors.append(
                            f"has type conflict for dimension `{dimension.name}` "
                            f"- already in entity as type `{dimension_invariant.type}` but got `{dimension.type}`"
                        )

            if dimension_invariant.is_partition != is_partition:
                validation_errors.append(
                        f"conflicting is_partition attribute for dimension "
                        f"`{dimension.reference}` - already in entity"
                        f" with is_partition as `{dimension_invariant.is_partition}` but got "
                        f"`{is_partition}``",
                    )

        if validation_errors:
            raise DbtSemanticValidationError(
                f"The entity name '{entity.name}' is invalid.  It {', '.join(e for e in validation_errors)}"
            )


class IdentifierValidator:
    """This class exists to validate identifiers"""