#
# Project makefile for SKA-Mid Dish Structure Steering Control Unit project. 
#
# Distributed under the terms of the BSD 3-clause new license.
# See LICENSE for more info.

include .make/base.mk

########################################################################
# DOCS
########################################################################
include .make/docs.mk

########################################################################
# PYTHON
########################################################################
include .make/python.mk

PYTHON_LINE_LENGTH = 88

python-post-lint:
	$(PYTHON_RUNNER) mypy src/ tests/


ifndef CI_JOB_TOKEN
SIM_IMAGE=simulator:4.4.0
SIM_PORT=4840
CONTAINER_NAME=cetc54-simulator-for-scu-tests

python-pre-test:
	@if ! docker image inspect $(SIM_IMAGE) > /dev/null 2>&1; then \
		echo "Error: Docker image '$(SIM_IMAGE)' not found. The image is needed to run the tests - please pull or build it first!"; \
		exit 1; \
	fi
	@if curl -s --http0.9 http://localhost:$(SIM_PORT) > /dev/null 2>&1; then \
		echo "CETC54 simulator is already running on port $(SIM_PORT)."; \
	elif [ $$? -eq 23 ]; then \
		echo "CETC54 simulator is running, but curl returned code 23 (write error)."; \
	else \
		echo "CETC54 simulator is not running. Starting Docker container..."; \
		docker run --rm -d -p 8090:8090 -p $(SIM_PORT):$(SIM_PORT) -p 5005:5005 --name $(CONTAINER_NAME)  $(SIM_IMAGE); \
		sleep 6; \
	fi

python-post-test:
	@if docker ps -q --filter "name=$(CONTAINER_NAME)" | grep -q .; then \
		echo "Stopping the CETC54 simulator container..."; \
		docker stop $(CONTAINER_NAME); \
	else \
		echo "CETC54 simulator container is not running."; \
	fi
endif


########################################################################
# OCI, HELM, K8S
########################################################################
include .make/oci.mk

########################################################################
# PRIVATE OVERRIDES
########################################################################
-include PrivateRules.mak
