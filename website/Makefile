setup:
	@brew list hugo || brew install hugo
	@npm install
	@which gulp || npm install gulp
	@pip install linkchecker

serve:
	@hugo server --watch

assets:
	@npm run build

dev-assets:
	@npm run dev-assets

pages:
	@hugo

linkchecker:
	@scripts/linkchecker.sh

javadocs:
	@scripts/javadocs.sh

site:
	@make assets
	@make pages
	@make javadocs
	@make linkchecker
