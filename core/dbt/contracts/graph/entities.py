class EntityReference(object):
    def __init__(self, entity_name, package_name=None):
        self.entity_name = entity_name
        self.package_name = package_name

    def __str__(self):
        return f"{self.entity_name}"


class ResolvedEntityReference(EntityReference):
    """
    Simple proxy over an Entity which delegates property
    lookups to the underlying node. Also adds helper functions
    for working with metrics (ie. __str__ and templating functions)
    """

    def __init__(self, node, manifest, Relation):
        super().__init__(node.name, node.package_name)
        self.node = node
        self.manifest = manifest
        self.Relation = Relation

    def __getattr__(self, key):
        return getattr(self.node, key)

    def __str__(self):
        return f"{self.node.name}"
