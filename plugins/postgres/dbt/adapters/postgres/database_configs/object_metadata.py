from typing import Dict, Union

import agate


"""
This is what object metadata from the database looks like. It's a dictionary because there will be
multiple grains of data for a single object. For example, a materialized view has base level information,
like name. But it also can have multiple indexes, which needs to be a separate query.
"""
ObjectMetadata = Dict[str, Union[agate.Row, agate.Table]]
