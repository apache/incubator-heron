setup:
	@brew list hugo || brew install hugo
	@npm install gulp
	@npm install
	@gulp build
	@make assets
	@pip install linkchecker

serve:
	@hugo server --watch

assets:
	@gulp build

pages:
	@hugo

linkchecker:
	linkchecker public/index.html 
site:
	@make assets
	@make pages
	# TODO make javadocs in public/api
	#   - bazel build --config=darwin heron:javadoc
	#   - extract bazel-bin/heron/javadoc.zip to website/public/api excluding top-level javadoc dir
	@make linkchecker

