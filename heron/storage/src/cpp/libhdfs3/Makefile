# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# -*-makefile-*-
#------------------------------------------------------------------------------
# A makefile that integrate building this module with hawq
#------------------------------------------------------------------------------
subdir = depends/libhdfs3
top_builddir = ../../
include Makefile.global

PRE_CFG_ARG =
# get argument for running ../boostrap
ifeq ($(enable_debug), yes)
	PRE_CFG_ARG += --enable-debug
endif # enable_debug

ifeq ($(enable_coverage), yes)
	PRE_CFG_ARG += --enable-coverage
endif # enable_coverage

##########################################################################
#
.PHONY: build all install distclean maintainer-clean clean pre-config

ifeq ($(with_libhdfs3), yes)

# We will need to install it temporarily under build/install for hawq building.
all: build
	cd $(top_builddir)/$(subdir)/build; mkdir -p install; \
	$(MAKE) DESTDIR=$(abs_top_builddir)/$(subdir)/build/install install

install: build
	cd $(top_builddir)/$(subdir)/build && $(MAKE) install

distclean:
	rm -rf $(top_builddir)/$(subdir)/build

maintainer-clean: distclean

clean:
	if [ -d $(top_builddir)/$(subdir)/build ]; then \
		cd $(top_builddir)/$(subdir)/build && $(MAKE) clean && rm -f libhdfs3_build_timestamp; \
	fi

build: pre-config
	cd $(top_builddir)/$(subdir)/build && $(MAKE)

# trigger bootstrap only once.
pre-config:
	cd $(top_builddir)/$(subdir)/; \
	mkdir -p build; \
	cd build; \
	if [ ! -f libhdfs3_build_timestamp ]; then \
		$(abs_top_srcdir)/$(subdir)/bootstrap --prefix=$(prefix) $(PRE_CFG_ARG) && touch libhdfs3_build_timestamp; \
	fi

else

all install distclean maintainer-clean clean pre-config:

endif
