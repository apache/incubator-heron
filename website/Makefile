GULP = node_modules/.bin/gulp

setup:
	scripts/setup.sh

clean:
	rm -rf public/* static/*

serve:
	@hugo serve

develop-static-assets:
	@${GULP} dev

build-static-assets:
	@${GULP} clean
	@${GULP} build

pages:
	@hugo

linkchecker:
	@scripts/linkchecker.sh

javadocs:
	@scripts/javadocs.sh

python-docs:
	@scripts/python-doc-gen.sh

site:
	@make clean
	@make build-static-assets
	@make pages
	@make javadocs
	@make python-docs

travis-site:
	@make clean
	@make build-static-assets
	@make pages
	@scripts/javadocs.sh --travis
	@make python-docs
