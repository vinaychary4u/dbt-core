from typing import List, Dict, Optional

from dbt import semver
from dbt.flags import get_flags
from dbt.version import get_installed_version
from dbt.clients import registry
from dbt.contracts.project import (
    RegistryPackageMetadata,
    RegistryPackage,
)
from dbt.deps.base import PinnedPackage, UnpinnedPackage
from dbt.exceptions import (
    DependencyError,
    PackageNotFoundError,
    PackageVersionNotFoundError,
    VersionsNotCompatibleError,
)


class RegistryPackageMixin:
    def __init__(self, package: str) -> None:
        super().__init__()
        self.package = package

    @property
    def name(self):
        return self.package

    def source_type(self) -> str:
        return "hub"


class RegistryPinnedPackage(RegistryPackageMixin, PinnedPackage):
    def __init__(self, package: str, version: str, version_latest: str) -> None:
        super().__init__(package)
        self.version = version
        self.version_latest = version_latest

    @property
    def name(self):
        return self.package

    def source_type(self):
        return "hub"

    def get_version(self):
        return self.version

    def get_version_latest(self):
        return self.version_latest

    def nice_version_name(self):
        return "version {}".format(self.version)

    def _fetch_metadata(self, project, renderer) -> RegistryPackageMetadata:
        dct = registry.package_version(self.package, self.version)
        return RegistryPackageMetadata.from_dict(dct)

    def install(self, project, renderer):
        self._install(project, renderer)

    def __repr__(self):
        return f"{self.package}@{self.version}"


class RegistryUnpinnedPackage(RegistryPackageMixin, UnpinnedPackage[RegistryPinnedPackage]):
    def __init__(
        self,
        package: str,
        versions: List[semver.VersionSpecifier],
        install_prerelease: bool,
        by_whom: str = "user",
        who_wants_which: Optional[Dict[str, List[semver.VersionSpecifier]]] = None,
    ) -> None:
        super().__init__(package)
        self.versions = versions
        self.install_prerelease = install_prerelease
        self.by_whom = by_whom
        self.who_wants_which = who_wants_which

        if not self.who_wants_which:
            self.who_wants_which = {self.by_whom: self.versions}

    def __repr__(self) -> str:
        version_list = []
        for version in self.versions:
            version_list.append(version.to_version_string())
        return f"{self.name} : {version_list}"

    def _check_in_index(self):
        index = registry.index_cached()
        if self.package not in index:
            raise PackageNotFoundError(self.package)

    def package_requests(self):
        message = ""
        for who, which in self.who_wants_which.items():
            version_list = []
            for version in which:
                version_list.append(version.to_version_string())
            message = message + f"\n    Required by {who}: {version_list}"
        return message

    @classmethod
    def from_contract(cls, contract: RegistryPackage, by_whom="user") -> "RegistryUnpinnedPackage":
        raw_version = contract.get_versions()

        versions = [semver.VersionSpecifier.from_version_string(v) for v in raw_version]
        return cls(
            package=contract.package,
            versions=versions,
            install_prerelease=bool(contract.install_prerelease),
            by_whom=by_whom,
        )

    def incorporate(self, other: "RegistryUnpinnedPackage") -> "RegistryUnpinnedPackage":
        who_wants_which = (self.who_wants_which or {}) | (other.who_wants_which or {})
        return RegistryUnpinnedPackage(
            package=self.package,
            install_prerelease=self.install_prerelease,
            versions=self.versions + other.versions,
            who_wants_which=who_wants_which,
        )

    def resolved(self) -> RegistryPinnedPackage:
        self._check_in_index()
        try:
            range_ = semver.reduce_versions(*self.versions)
        except VersionsNotCompatibleError as e:
            new_msg = f"Version error for package {self.name}: {self.package_requests()}"
            print(f"--- in resolved. new_msg: {new_msg}")
            raise DependencyError(new_msg) from e
        flags = get_flags()
        should_version_check = bool(flags.VERSION_CHECK)
        dbt_version = get_installed_version()
        compatible_versions = registry.get_compatible_versions(
            self.package, dbt_version, should_version_check
        )
        prerelease_version_specified = any(bool(version.prerelease) for version in self.versions)
        installable = semver.filter_installable(
            compatible_versions, self.install_prerelease or prerelease_version_specified
        )
        if installable:
            # for now, pick a version and then recurse. later on,
            # we'll probably want to traverse multiple options
            # so we can match packages. not going to make a difference
            # right now.
            target = semver.resolve_to_specific_version(range_, installable)
        else:
            target = None
        if not target:
            # raise an exception if no installable target version is found
            raise PackageVersionNotFoundError(
                self.package, range_, installable, should_version_check
            )
        latest_compatible = installable[-1]
        return RegistryPinnedPackage(
            package=self.package, version=target, version_latest=latest_compatible
        )
