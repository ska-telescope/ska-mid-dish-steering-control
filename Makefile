SHELL=/bin/bash
.SHELLFLAGS=-o pipefail -c

NAME=ska-mid-dish-steering-control
VERSION=$(shell grep -e "^version = s*" pyproject.toml | cut -d = -f 2 | xargs)

-include .make/base.mk

########################################################################
# DOCS
########################################################################
-include .make/docs.mk


########################################################################
# PYTHON
########################################################################
PYTHON_LINE_LENGTH = 99
PYTHON_SWITCHES_FOR_BLACK = --line-length 99
PYTHON_SWITCHES_FOR_ISORT = -w 99
PYTHON_SWITCHES_FOR_FLAKE8 = --max-line-length=99
PYTHON_VARS_BEFORE_PYTEST ?= PYTHONPATH=.:./src
PYTHON_VARS_AFTER_PYTEST ?= -m '$(MARK)' --forked --json-report --json-report-file=build/report.json --junitxml=build/report.xml

# Set the specific environment variables required for pytest
python-test: MARK = unit

-include .make/python.mk

########################################################################
# OCI, HELM, K8S
########################################################################
OCI_TAG = $(VERSION)-dev.c$(CI_COMMIT_SHORT_SHA)

CI_REGISTRY ?= registry.gitlab.com

-include .make/oci.mk

########################################################################
# PRIVATE OVERRIDES
########################################################################

-include PrivateRules.mak
