PROJECT=memdb
PROJECT_DESCRIPTION=simple memory database with MVCC semantic
PROJECT_VERSION=0.1

DOC_DEPS = edown
EDOC_OPTS = {doclet, edown_doclet}

doc: edoc
	cp doc/README.md .

distclean:: distclean-edown

distclean-edown:
	rm -rf doc/*.md


include erlang.mk
