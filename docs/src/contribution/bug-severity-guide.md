## Bug Severity Guidelines

This guide is about determining defects severity on TiKV Java Client according
to the impact on the online service. The higher effect the defect has on the
overall functionality or performance, the higher the severity is. There are 4
severity levels:

1. Critical
2. Major
3. Moderate
4. Minor

Each severity is described with examples in the remaining contents.

### Critical Defects

A defect that affects critical data or functionality and leaves users
with no workaround is classified as a critical defect.

Guideline 1. A defect that breaks the API definition is regarded as critical.
For example:

* [client-java/issues/412](https://github.com/tikv/client-java/issues/412)
in this defect, gRPC timeout is not set for certain requests, which causes the
requests can not be terminated as expected when the processing time is too long.

### Major Defects

### Moderate Defects

### Minor Defects