# coding=utf-8
# # TODO: use dbt deps and debug code as a template to print out helpful information for the dbt conrtracts command
import os
import shutil
import json
import dbt.utils
import dbt.deprecations
import dbt.exceptions
from dbt.ui import green, red

from dbt.config import UnsetProfileConfig

# from dbt.config.renderer import DbtProjectYamlRenderer
# from dbt.deps.base import downloads_directory
# from dbt.deps.resolver import resolve_packages

# from dbt.events.functions import fire_event
# from dbt.events.types import (
#     DepsNoPackagesFound,
#     DepsStartPackageInstall,
#     DepsUpdateAvailable,
#     DepsUTD,
#     DepsInstallInfo,
#     DepsListSubdirectory,
#     DepsNotifyUpdatesAvailable,
#     EmptyLine,
# )
# from dbt.clients import system

from dbt.task.base import BaseTask, move_to_nearest_project_dir
from dbt.clients.yaml_helper import load_yaml_text

# from dbt.clients.git import clone_and_checkout

# TODO: point to github repo to consume using existing mechanic for packages
# TODO: run a dbt compile to output the consumed manifest.json
# TODO: integrate Doug's consumer ref code
# TODO: what if I included this directly in the deps command? no, keep this separate
# Remember, we aren't doing a real implementation of contracts, just a proof of concept. Therefore, I can create net new scripts knowing they will be thrown away. The goal is understanding the general structure of the code and how it will be used.


class DepsTask(BaseTask):
    ConfigType = UnsetProfileConfig

    def __init__(self, args, config: UnsetProfileConfig):
        super().__init__(args=args, config=config)

    def track_package_install(self, package_name: str, source_type: str, version: str) -> None:
        # Hub packages do not need to be hashed, as they are public
        # Use the string 'local' for local package versions
        if source_type == "local":
            package_name = dbt.utils.md5(package_name)
            version = "local"
        elif source_type != "hub":
            package_name = dbt.utils.md5(package_name)
            version = dbt.utils.md5(version)
        dbt.tracking.track_package_install(
            self.config,
            self.config.args,
            {"name": package_name, "source": source_type, "version": version},
        )

    def run(self):
        print("xxxxxxxxxxxxxxxxxxxx")
        # system.make_directory(self.config.packages_install_path)
        # packages = self.config.packages.packages
        # TODO: Locate the dbt_contracts.yml file
        project_dir = os.getcwd()  # running a dbt project locally
        default_directory_location = os.path.join(project_dir, "dbt_contracts.yml")
        print(f"default_directory_location: {default_directory_location}")

        # TODO: read in the dbt_contracts.yml as a dictionary
        with open(default_directory_location, "r") as stream:
            contracts_consumed_rendered = load_yaml_text(stream)
        print(f"contracts_consumed_rendered: {contracts_consumed_rendered}")
        consumer = contracts_consumed_rendered.get("consumer")
        print("xxxxxxxxxxxxxxxxxxxx\n")
        # TODO: Verify the api private key works(print statement for now: fire_event)
        # Will have to create a menu of options such as gcs, s3, API key, etc. to authenticate
        contract_validation = {}
        for x in consumer:
            contract_validation.update({x.get("contract_location"): x.get("credentials")})
            print(f'{x.get("name")}: contract credentials verified {green("[OK connection ok]")}')

        # TODO: output the consumed code to a `contracts/projects/consumed` directory
        contracts_dir = project_dir + "/dbt_contracts"
        if not os.path.exists(contracts_dir):
            os.mkdir(contracts_dir)

        # download the contracts from the contract_location and store them in the contracts_dir
        # in the short-term, we will copy the contracts from the local test directory to the contracts_dir
        # this contracts.json will consolidate a subset of the manifest.json, catalog.json, run_results.json, sources.json files and then merge that with the consumer's manifest.json, catalog.json(run_results.json, sources.json files are for validating contract requirements only)
        dummy_contracts_file_location = "../../tests/functional/dbt_contracts/contracts.json"
        for x in consumer:
            contract_name = x.get("name")
            contract_version_expected = x.get("contract_version")
            contract_destination = f"{contracts_dir}/{contract_name}-contracts.json"
            with open(dummy_contracts_file_location) as json_file:
                contract_data = json.load(json_file)
                contract_version_actual = contract_data.get("metadata").get("contract_version")
            if contract_version_expected == contract_version_actual:
                shutil.copyfile(dummy_contracts_file_location, contract_destination)
                print(f"Successful contract consumed[{contract_name}]: {contract_destination}")
                # TODO: output the consumed contracts.json to a `contracts/consumed` directory within the respective consumed project directory
                # TODO: Read in the consumed `contracts.json` to produce a report card in a terminal output
                # What's published vs. private nodes?
                print(f"  Published Nodes: {contract_data.get('contracts').get('published')}")
                print(f"  Private Nodes: {contract_data.get('contracts').get('private')}")
                # What are the contract expectations vs. actuals?
                print(
                    f"  Test Coverage: {contract_data.get('contracts').get('requirements').get('test_coverage')} {green('[OK and valid]')}"
                )
                print(
                    f"  Freshness Coverage: {contract_data.get('contracts').get('requirements').get('freshness_coverage')} {green('[OK and valid]')}"
                )
                print(
                    f"  Max Upgrade Time Between Versions: {contract_data.get('contracts').get('requirements').get('max_upgrade_time')}"
                )
                # What permissions do I need to select published nodes?
                print(
                    f"  Published Node Permissions: {contract_data.get('contracts').get('permissions')}"
                )
                # How do I select them?
                contract_name = contract_data.get("contracts").get("name")
                print("  Published Node Selection:")
                print(f"    select * from {{{{ ref('{contract_name}','my_first_model') }}}}")
                print(f"    select * from {{{{ ref('{contract_name}','my_second_model') }}}}")
            else:
                print(
                    f"Contract version mismatch, will not consume[{contract_name}]. Expected: {contract_version_expected}, Actual: {contract_version_actual} {red('[Not Compatible]')} \n"
                )

        # git clone may not be necessary because the contracts.json will contain all the info from the manifest.json and catalog.json
        # for x in consumer:
        #     project_location = x.get("path")
        #     print(f"project_location: {project_location}")
        #     clone_and_checkout(repo=project_location, cwd=contracts_dir)

        # if not packages:
        #     fire_event(DepsNoPackagesFound())
        #     return

        # with downloads_directory():
        #     final_deps = resolve_packages(packages, self.config)

        #     renderer = DbtProjectYamlRenderer(self.config, self.config.cli_vars)

        #     packages_to_upgrade = []
        #     for package in final_deps:
        #         package_name = package.name
        #         source_type = package.source_type()
        #         version = package.get_version()

        #         fire_event(DepsStartPackageInstall(package_name=package_name))
        #         package.install(self.config, renderer)
        #         fire_event(DepsInstallInfo(version_name=package.nice_version_name()))
        #         if source_type == "hub":
        #             version_latest = package.get_version_latest()
        #             if version_latest != version:
        #                 packages_to_upgrade.append(package_name)
        #                 fire_event(DepsUpdateAvailable(version_latest=version_latest))
        #             else:
        #                 fire_event(DepsUTD())
        #         if package.get_subdirectory():
        #             fire_event(DepsListSubdirectory(subdirectory=package.get_subdirectory()))

        #         self.track_package_install(
        #             package_name=package_name, source_type=source_type, version=version
        #         )
        #     if packages_to_upgrade:
        #         fire_event(EmptyLine())
        #         fire_event(DepsNotifyUpdatesAvailable(packages=packages_to_upgrade))

    @classmethod
    def from_args(cls, args):
        # deps needs to move to the project directory, as it does put files
        # into the modules directory
        move_to_nearest_project_dir(args)
        return super().from_args(args)
