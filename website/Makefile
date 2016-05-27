setup:
	@brew list hugo || brew install hugo
	@npm install
	@pip uninstall requests
	@pip install pyyaml linkchecker pygments requests==2.9.0

clean:
	rm -rf public/*

serve:
	@hugo server --watch

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

update-heron-version:
	@scripts/heron-version.sh

site:
	@make clean
	@make build-static-assets
	@make pages
	@make javadocs
	@make linkchecker
