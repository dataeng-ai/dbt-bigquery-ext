PYPI_VERSION := $(shell python3 -c "import pathlib,re; t=pathlib.Path('src/dbt/adapters/bigquery/__version__.py').read_text(); print(re.search(r'pypi_version\s*=\s*\"([^\"]+)\"', t).group(1))")
GHCR_IMAGE ?= ghcr.io/dataeng-ai/change-metadata-pooler

# Tag the PyPI release (pypi_version), not the dbt semver string.
tag:
	git tag "v$(PYPI_VERSION)"; \
	git push origin "v$(PYPI_VERSION)"

build:
	python3 -m pip install -U build twine
	rm -rf dist build *.egg-info
	python3 -m build

# Requires PyPI token for the dataeng-ai publisher account:
#   export TWINE_USERNAME=__token__
#   export TWINE_PASSWORD=pypi-...
publish-pypi: build
	python3 -m twine upload dist/*

# GHCR (public). Login once:
#   echo "$$GITHUB_TOKEN" | docker login ghcr.io -u USERNAME --password-stdin
# Token needs write:packages (and SSO authorized for the org if required).
publish-docker:
	docker build \
		-f services/change_metadata_pooler/Dockerfile \
		--build-arg PYPI_VERSION=$(PYPI_VERSION) \
		-t $(GHCR_IMAGE):$(PYPI_VERSION) \
		-t $(GHCR_IMAGE):latest \
		.
	docker push $(GHCR_IMAGE):$(PYPI_VERSION)
	docker push $(GHCR_IMAGE):latest

# PyPI wheel + GHCR image at the same pypi_version, in parallel.
publish: build
	$(MAKE) -j2 _publish-pypi-only publish-docker

_publish-pypi-only:
	python3 -m twine upload dist/*
