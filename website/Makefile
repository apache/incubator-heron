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
	#   - bazel build --config=darwin heron:javadoc
	#   - extract bazel-bin/heron/javadoc.zip to website/public/api excluding top-level javadoc dir
	# TODO run link checker
