.PHONY : start javadocs python-docs buildsite publish clean

start:
	yarn start

javadocs:
	@scripts/javadocs.sh

python-docs:
	@scripts/python-doc-gen.sh 0.0.0

buildsite: javadocs python-docs
	@scripts/build-site.sh

publish:
	@scripts/publish_site.sh

clean:
	rm -rf static/api/*
	rm -rf build/*