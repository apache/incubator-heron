setup:
	@brew list hugo || brew install hugo
	@npm install
	@which gulp || npm install gulp
	@pip install linkchecker

serve:
	@hugo server --watch

assets:
	@npm run build

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

