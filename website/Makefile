HERONPY_VERSION := $(shell python -c "import yaml; print yaml.load(open('config.yaml', 'r'))['params']['versions']['heronpy']")
NODE_BIN        = node_modules/.bin
GULP            = $(NODE_BIN)/gulp
CONCURRENTLY    = $(NODE_BIN)/concurrently

setup:
	scripts/setup.sh

clean:
	rm -rf public/* static/api static/style.css static/style*.css

develop-content: build-static-assets
	@ENV=development hugo serve --buildDrafts --disableFastRender

serve:
	$(CONCURRENTLY) "make develop-content" "make develop-static-assets"

develop-static-assets:
	@$(GULP) dev

build-static-assets:
	@$(GULP) build

pages:
	@ENV=production hugo

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
	@rm static/css/style-*.css static/js/app-*.js

publish:
	@scripts/publish.sh

travis-site:
	@make clean
	@make build-static-assets
	@scripts/javadocs.sh --travis
	@make python-docs
	@make pages
