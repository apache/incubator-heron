setup:
	@brew list hugo || brew install hugo
	@npm install
	@which gulp || npm install gulp
	@pip install linkchecker requests==2.9.0

serve:
	@hugo server --watch

assets:
	@npm run build

pages:
	@hugo

linkchecker:
	@./scripts/linkchecker.sh

javadocs:
	@./scripts/javadocs.sh

site:
	@make assets
	@make pages
	@make javadocs
	@make linkchecker
