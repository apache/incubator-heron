setup:
	@brew list hugo || brew install hugo
	@npm install gulp
	@npm install

serve:
	@hugo server --watch

assets:
	@node_modules/.bin/gulp build

pages:
	@hugo

linkchecker:
	@bash linkchecker.sh

javadocs:
	@./javadocs.sh

site:
	@make assets
	@make pages
	@make javadocs
	@make linkchecker

