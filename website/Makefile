setup:
	@brew list hugo || brew install hugo
	@npm install gulp
	@npm install
	@make assets

serve:
	@hugo server --watch

assets:
	@gulp build

pages:
	@hugo

javadocs:
	@./javadocs.sh

linkchecker:
	@echo "TODO: linkchecking"

site:
	@make assets
	@make pages
	@make javadocs
	@make linkchecker
