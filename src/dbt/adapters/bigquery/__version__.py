# Two versions on purpose.
#
# `version` is what dbt-core imports from dbt.adapters.bigquery.__version__
# and parses with its own semver (dbt_common.semver). That parser accepts
# MAJOR.MINOR.PATCH plus optional -prerelease / +build, and rejects PEP 440
# post-releases. Putting a ".postN" suffix here aborts `dbt debug` with
# "not a valid semantic version".
#
# `pypi_version` is the distribution version uploaded to PyPI (hatch reads
# only this assignment). Scheme: <upstream>.post<N>. On a rebase, set
# `version` to the new upstream release and bump `pypi_version` to
# "<upstream>.post1". Do not put ".postN" into `version`.
version = "1.12.1"
pypi_version = "1.12.1.post6"
