.DEFAULT: build

.PHONY: clean

build:
	mvn clean package -Dmaven.test.skip=true

fmt:
	./dev/javafmt

test:
	mvn clean test

clean:
	mvn clean
