workspace(name = "tispark")

maven_jar(
      name = "org_apache_commons_commons_lang3",
      artifact = "org.apache.commons:commons-lang3:3.5",
      sha1 = "6c6c702c89bfff3cd9e80b04d668c5e190d588c6",
)

maven_jar(
    name = "com_fasterxml_jackson_core_jackson_annotations",
    artifact = "com.fasterxml.jackson.core:jackson-annotations:2.6.6",
)

maven_jar(
    name = "com_fasterxml_jackson_core_jackson_databind",
    artifact = "com.fasterxml.jackson.core:jackson-databind:2.6.6",
)

maven_jar(
    name = "com_fasterxml_jackson_core_jackson_core",
    artifact = "com.fasterxml.jackson.core:jackson-core:2.6.6",
)

maven_jar(
    name = "org_slf4j_slf4j_api",
    artifact = "org.slf4j:slf4j-api:1.7.16",
)

maven_jar(
    name = "org_slf4j_jcl_over_slf4j",
    artifact = "org.slf4j:jcl-over-slf4j:1.7.16",
)

maven_jar(
    name = "org_slf4j_jul_to_slf4j",
    artifact = "org.slf4j:jul-to-slf4j:1.7.16",
)

maven_jar(
    name = "log4j_log4j",
    artifact = "log4j:log4j:1.2.17",
)

maven_jar(
    name = "joda_time",
    artifact = "joda-time:joda-time:2.9.9",
)

maven_jar(
    name = "junit_junit",
    artifact = "junit:junit:4.12",
)

maven_jar(
    name = "org_hamcrest_hamcrest_core",
    artifact = "org.hamcrest:hamcrest-core:1.3",
)

maven_jar(
    name = "org_javassist_javassist",
    artifact = "org.javassist:javassist:3.21.0-GA",
)

maven_jar(
    name = "org_powermock_powermock_reflect",
    artifact = "org.powermock:powermock-reflect:1.6.6",
)

maven_jar(
    name = "org_powermock_powermock_api_mockito",
    artifact = "org.powermock:powermock-api-mockito:1.6.6",
)

maven_jar(
    name = "org_mockito_mockito_core",
    artifact = "org.mockito:mockito-core:1.10.19",
)

maven_jar(
    name = "org_objenesis_objenesis",
    artifact = "org.objenesis:objenesis:2.1",
)

maven_jar(
    name = "org_powermock_powermock_api_mockito_common",
    artifact = "org.powermock:powermock-api-mockito-common:1.6.6",
)

maven_jar(
    name = "com_sangupta_murmur",
    artifact = "com.sangupta:murmur:1.0.0"
)

maven_jar(
    name = "org_powermock_powermock_api_support",
    artifact = "org.powermock:powermock-api-support:1.6.6",
)

maven_jar(
   name = "net_sf_trove4j_trove4j",
   artifact = "net.sf.trove4j:trove4j:3.0.1",
)
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
git_repository(
  name = "org_zhexuany_rule_proto_java",
  remote = "https://github.com/zhexuany/rule_proto_java",
  commit = "093a9a0338411b9d64db99a3ab1c7ed7aa6b2637",
)

load("@org_zhexuany_rule_proto_java//java:rules.bzl", "java_proto_repositories")
java_proto_repositories()

bazel_shade_version = "master"
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
http_archive(
             name = "com_github_zhexuany_bazel_shade",
             url = "https://github.com/zhexuany/bazel_shade_plugin/archive/%s.zip"%bazel_shade_version,
             type = "zip",
             strip_prefix= "bazel_shade_plugin-%s"%bazel_shade_version
)
load(
    "@com_github_zhexuany_bazel_shade//:java_shade.bzl",
    "java_shade_repositories",
    "java_shade"
)
java_shade_repositories()

