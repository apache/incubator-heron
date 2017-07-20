GULP = node_modules/.bin/gulp

setup:
	scripts/setup.sh

clean:
	rm -rf public/* static/*

serve:
	@hugo serve

develop-static-assets:
	@${GULP} develop

build-static-assets:
	@${GULP} clean
	@${GULP} build

pages:
	@hugo

linkchecker:
	@scripts/linkchecker.sh

javadocs:
	@scripts/javadocs.sh

javadocs-travis:
	@scripts/javadocs.sh --travis

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
	@make javadocs-travis
	@make python-docs
