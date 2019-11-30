start:
	yarn start

javadocs:
	@scripts/javadocs.sh

python-docs:
	@scripts/python-doc-gen.sh 0.0.0

buildsite:
	@scripts/javadocs.sh
	@scripts/python-doc-gen.sh 0.0.0
	@scripts/build-site.sh

publish:
	@scripts/publish_site.sh
