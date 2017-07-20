GULP = node_modules/.bin/gulp
HUGO_VERSION = 0.25.1
BUNDLER_VERSION = 1.15.1

setup:
	scripts/setup.sh

clean:
	rm -rf public/* static/*

serve:
	@hugo serve

develop-static-assets:
	@${GULP} develop

build-static-assets:
	@${GULP} clean
	@${GULP} build

pages:
	@hugo

linkchecker:
	@scripts/linkchecker.sh

javadocs:
	@scripts/javadocs.sh

javadocs-travis:
	@scripts/javadocs.sh --travis

python-docs:
	@scripts/python-doc-gen.sh

site:
	@make clean
	@make build-static-assets
	@make pages
	@make javadocs
	@make python-docs
	@make linkchecker

travis-ruby-setup:
	NOKOGIRI_USE_SYSTEM_LIBRARIES=true bundle install \
		--path vendor/bundle \
		--jobs=3

travis-python-setup:
	sudo -H pip install pygments==2.1.3 pdoc==0.3.2

travis-install-hugo:
	wget -q https://github.com/gohugoio/hugo/releases/download/v${HUGO_VERSION}/hugo_${HUGO_VERSION}_Linux-64bit.deb
	sudo dpkg -i hugo_${HUGO_VERSION}_Linux-64bit.deb
	hugo version

travis-website-setup: travis-ruby-setup travis-python-setup travis-install-hugo

travis-website-check:
	@make clean
	@make pages
	@make javadocs-travis
	@make python-docs
	@make linkchecker
