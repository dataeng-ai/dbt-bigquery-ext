# Tag the PyPI release (pypi_version), not the dbt semver string.
tag:
	@v=$$(python3 -c "import pathlib,re; t=pathlib.Path('src/dbt/adapters/bigquery/__version__.py').read_text(); print(re.search(r'pypi_version\s*=\s*\"([^\"]+)\"', t).group(1))"); \
	git tag "v$$v"; \
	git push origin "v$$v"

build:
	python3 -m pip install -U build twine
	rm -rf dist build *.egg-info
	python3 -m build

# Requires PyPI token for the dataeng-ai publisher account:
#   export TWINE_USERNAME=__token__
#   export TWINE_PASSWORD=pypi-...
publish: build
	python3 -m twine upload dist/*
