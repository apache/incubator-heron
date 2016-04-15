setup:
	@brew install hugo
	@npm install gulp
	@npm install
	@gulp build
	@make assets

serve:
	@hugo server --watch

assets:
	@gulp build

publish:
	@hugo
	@pip install linkchecker
	@linkchecker public/index.html 
