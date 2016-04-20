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
	#linkchecker removes previous runs.
	@pip install linkchecker
	@rm -f linkchecker-errors.csv && rm -f linkchecker-out.csv
	-linkchecker public/index.html --no-warnings -F csv 
	@cut -sd ';' -f 1,2 linkchecker-out.csv | tr ';' ' ' | awk '{ print $$2 " " $$1}' | sort -u >> linkchecker-errors.csv
	@rm -f linkchecker-out.csv
	#following fails if linkchecker fails. if pass -> error list is removed, if fail -> error list remains for review. 
	@linkchecker public/index.html --no-warnings 
	@echo "linkchecker passes!"
	@rm -f linkchecker-errors.csv

site:
	@make assets
	@make pages
	# TODO make javadocs in public/api
	#   - bazel build --config=darwin heron:javadoc
	#   - extract bazel-bin/heron/javadoc.zip to website/public/api excluding top-level javadoc dir
	@make linkchecker

