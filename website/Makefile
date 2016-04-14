setup:
	@brew list hugo || brew install hugo
	@npm install gulp
	@npm install
	@gulp build
	@make assets

serve:
	@hugo server --watch

assets:
	@gulp build

pages:
	@hugo

site:
	@make assets
	@make pages
	# TODO make javadocs in public/api
	# TODO run link checker
