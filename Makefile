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


########################################################################
# OCI, HELM, K8S
########################################################################
include .make/oci.mk

########################################################################
# PRIVATE OVERRIDES
########################################################################
-include PrivateRules.mak
