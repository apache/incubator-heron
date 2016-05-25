setup:
	@brew list hugo || brew install hugo
	@npm install
	@pip uninstall requests
	@pip install linkchecker pygments requests==2.9.0
	@(cd assets/solarized-pygment && ./setup.py install)

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

site:
	@make build-static-assets
	@make pages
	@make javadocs
	@make linkchecker
