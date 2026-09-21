include version from src/dbt/adapters/bigquery/__version__.py

# First release tag should match __version__.py, e.g. v1.12.1.post1
tag:
	@v=$$(python3 -c "import pathlib,re; t=pathlib.Path('src/dbt/adapters/bigquery/__version__.py').read_text(); print(re.search(r'version\s*=\s*\"([^\"]+)\"', t).group(1))"); \
	git tag "v$$v"; \
	git push origin "v$$v"

build:
	python3 -m pip install -U build twine
	rm -rf dist build *.egg-info
	python3 -m build

# Requires PyPI token for the dataeng-ai publisher account:
#   TWINE_USERNAME=__token__ TWINE_PASSWORD=pypi-...
publish: build
	python3 -m twine upload dist/*
