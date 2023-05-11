#!/usr/bin/env python
import os
import sys

if sys.version_info < (3, 7, 2):
    print("Error: dbt does not support this version of Python.")
    print("Please upgrade to Python 3.7.2 or higher.")
    sys.exit(1)


from setuptools import setup

try:
    from setuptools import find_namespace_packages
except ImportError:
    # the user has a downlevel version of setuptools.
    print("Error: dbt requires setuptools v40.1.0 or higher.")
    print('Please upgrade setuptools with "pip install --upgrade setuptools" ' "and try again")
    sys.exit(1)


this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md")) as f:
    long_description = f.read()


package_name = "dbt-core"
package_version = "1.6.0a1"
description = """With dbt, data analysts and engineers can build analytics \
the way engineers build applications."""


setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="dbt Labs",
    author_email="info@dbtlabs.com",
    url="https://github.com/dbt-labs/dbt-core",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    test_suite="test",
    entry_points={
        "console_scripts": ["dbt = dbt.cli.main:cli"],
        # TODO, can we namespace this like modules?
        # feels like a very useful place for a good naming convention
        # Or maybe better yet, auto-populate these from a location on disk
        # since we should only ever have offically sanctioned plugins listed here
        #
        # The more I think about I like that second option.  We can easily scan a
        # given namespace (dbt.plugable I.E.) and generate ep groups from that.
        # This reduces the amount of boilerplate we have to write.
        # TODO, I don't love the "default" naming convention because
        # it's not actually descriptive. Can we "tag" these in some fashion
        #  for better selection logic interally?
        # More thoughts on this-- we can actually skip the "default" plugin and make
        # that code what's found in the main codebase! This saves some work by making
        # our plugin system more of a select-a-thing-to-replace-the-existing-fucntion
        # as opposed to a select-a-thing-from-a-pile-of-things-which-becomes-the-function
        #
        # The downside of this approach is that if the plugin system fails to properly
        # select our plugin, things keep working with the default behavior.
        # TODO. Consider a generic function name so we have a consistant plugin design
        # Switching to "execute" for now although I don't love that
        # (we have a _lot_ of functions with that name in the codebase already)
        "run_results_writer": [
            "sqlite3 = dbt.plugable.run_results_writer.sqlite3:execute",
            "ians_email = dbt.plugable.run_results_writer.ians_email:execute",
        ],
    },
    install_requires=[
        "Jinja2==3.1.2",
        "agate>=1.6,<1.7.1",
        "click>=7.0,<9",
        "colorama>=0.3.9,<0.4.7",
        "hologram>=0.0.14,<=0.0.16",
        "isodate>=0.6,<0.7",
        "logbook>=1.5,<1.6",
        "mashumaro[msgpack]==3.6",
        "minimal-snowplow-tracker==0.0.2",
        "networkx>=2.3,<2.8.1;python_version<'3.8'",
        "networkx>=2.3,<3;python_version>='3.8'",
        "packaging>20.9",
        "sqlparse>=0.2.3,<0.4.4",
        "dbt-extractor~=0.4.1",
        "typing-extensions>=3.7.4",
        "werkzeug>=1,<3",
        "pathspec>=0.9,<0.12",
        "protobuf>=4.0.0",
        "pytz>=2015.7",
        # the following are all to match snowflake-connector-python
        "requests<3.0.0",
        "idna>=2.5,<4",
        "cffi>=1.9,<2.0.0",
        "pyyaml>=6.0",
    ],
    zip_safe=False,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.7.2",
)
