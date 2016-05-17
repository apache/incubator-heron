setup:
	@brew list hugo || brew install hugo
	@npm install

serve:
	@hugo server --watch

assets:
	@npm run build

pages:
	@hugo --serve

linkchecker:
	@bash linkchecker.sh

javadocs:
	@./javadocs.sh

site:
	@make assets
	@make pages
	@make javadocs
	@make linkchecker

