package(default_visibility = ["//visibility:public"])

load(":rule.bzl", "junit_suite_test")

junit_suite_test(
    name = "tikv_client_java_test",
    srcs = glob(
        ["**/*.java"],
    ),
    deps = [
        "//src/main/java/org/tikv:tikv_java_client_lib",
        "//:tikv_proto_java_lib",
        "@com_fasterxml_jackson_core_jackson_annotations//jar",
        "@com_fasterxml_jackson_core_jackson_core//jar",
        "@com_fasterxml_jackson_core_jackson_databind//jar",
        "@org_apache_commons_commons_lang3//jar",
        "@org_zhexuany_rule_proto_java//java:grpc_compiletime_deps",
        "@org_zhexuany_rule_proto_java//java:netty_runtime_deps",
        "@net_sf_trove4j_trove4j//jar",
        "@junit_junit//jar",
        "@joda_time//jar",
        "@log4j_log4j//jar",
    ],
)

