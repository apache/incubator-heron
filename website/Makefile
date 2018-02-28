HERONPY_VERSION := $(shell python -c "import yaml; print yaml.load(open('config.yaml', 'r'))['params']['versions']['heronpy']")
GULP            = node_modules/.bin/gulp

setup:
	scripts/setup.sh

clean:
	rm -rf public/* static/api static/style.css static/style*.css

serve:
	@hugo serve --buildDrafts

develop-static-assets:
	@$(GULP) dev

build-static-assets:
	@$(GULP) build

pages:
	@hugo

linkchecker:
	@scripts/linkchecker.sh

javadocs:
	@scripts/javadocs.sh

python-docs:
	@scripts/python-doc-gen.sh $(HERONPY_VERSION)

site:
	@make clean
	@make build-static-assets
	@make javadocs
	@make python-docs
	@make pages

publish:
	@scripts/publish.sh

travis-site:
	@make clean
	@make build-static-assets
	@scripts/javadocs.sh --travis
	@make python-docs
	@make pages
