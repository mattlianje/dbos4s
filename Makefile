.PHONY: compile test test-it fmt fmt-check repl run publish-local clean

compile:
	./mill '__.compile'

test:
	./mill '__.test'

# Self-contained integration tests
test-it:
	./mill '__.test'

fmt:
	./mill mill.scalalib.scalafmt/

fmt-check:
	./mill mill.scalalib.scalafmt/ --check

repl:
	./mill -i repl.console

run:
	./mill examples.runMain example.Starter

publish-local:
	./mill '__.publishLocal'

clean:
	./mill clean
