SHELL := /bin/bash

setup:
	@brew list hugo || brew install hugo
	@npm install gulp
	@npm install
	@gulp build
	@make assets

serve:
	@hugo server --watch

assets:
	@gulp build

pages:
	@hugo

linkchecker:
	#linkchecker removes previous runs, checks error number.
	#if pass -> error list is removed, if fail -> error list remains for review.
	@pip install linkchecker
	-rm linkchecker-errors.csv && -rm linkchecker-out.csv
	-linkchecker public/index.html --no-warnings -F csv
	@cut -sd ';' -f 1,2 linkchecker-out.csv | tr ';' ' ' | awk '{ print $$2 " " $$1}' | sort -u >> linkchecker-errors.csv
	@rm linkchecker-out.csv
	# error check if linkchecker has more than 1 entry
	@wc -l linkchecker-errors.csv | tr ' ' ' ' | if [[ $$(awk '{ print $$1}') > 1 ]]; then echo 'Site contains broken links! check linkchecker-errors.csv'; else echo 'Linkchecker Passes!'; rm linkchecker-errors.csv; fi

site:
	@make assets
	@make pages
	# TODO make javadocs in public/api
	#   - bazel build --config=darwin heron:javadoc
	#   - extract bazel-bin/heron/javadoc.zip to website/public/api excluding top-level javadoc dir
	@make linkchecker

