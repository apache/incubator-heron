setup:
	scripts/setup.sh

clean:
	rm -rf public/* static/*

serve:
	@hugo server --watch --ignoreCache

develop-static-assets:
	@npm run develop

build-static-assets:
	@npm run clean
	@npm run build

pages:
	@hugo

linkchecker:
	@scripts/linkchecker.sh

javadocs:
	@scripts/javadocs.sh

site:
	@make clean
	@make build-static-assets
	@make pages
	@make javadocs
	@make linkchecker

travis-site:
	@make clean
	@make build-static-assets
	@make pages
	@scripts/javadocs.sh --travis
	@make linkchecker
