run:
	bazelisk run :tikv_java_client
uber_jar:
	bazelisk build :tikv_java_client_deploy.jar
test:
	bazelisk test //src/test/java/org/tikv:tikv_client_java_test --test_output=errors  --test_timeout=3600
