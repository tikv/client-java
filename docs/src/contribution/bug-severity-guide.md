## Bug Severity Guidelines

This is a **working-in-progress** guide about determining defects severity on
TiKV Java Client according to the impact on the online service. The higher
effect the defect has on the overall functionality or performance, the higher
the severity is. There are 4 severity levels:

1. Critical
2. Major
3. Moderate
4. Minor

Each severity is described with examples in the remaining contents.

### Critical Defects

A defect that affects critical data or functionality and leaves users
with no workaround is classified as a critical defect. These defects are
labeled with `type/bug` and `severity/critical`, can be found
[here](https://github.com/tikv/client-java/issues?q=is%3Aissue+is%3Aopen+sort%3Aupdated-desc+label%3Aseverity%2Fcritical)

Guideline 1. A defect that breaks the API definition is regarded as critical.
For example:

* [client-java/issues/412](https://github.com/tikv/client-java/issues/412)
in this defect, gRPC timeout is not set for certain requests, which causes the
requests can not be terminated as expected when the processing time is too long.

### Major Defects

A defect that affects critical data or functionality and forces users to employ
a workaround is classified as a major defect. These defects are labeled with
`type/bug` and `severity/major`, can be found
[here](https://github.com/tikv/client-java/issues?q=is%3Aissue+is%3Aopen+sort%3Aupdated-desc+label%3Aseverity%2Fmajor)

### Moderate Defects

A defect that affects non-critical data or functionality and forces users to
employ a workaround is classified as moderate defect. These defects are labeled
with `type/bug` and `severity/moderate`, can be found
[here](https://github.com/tikv/client-java/issues?q=is%3Aissue+is%3Aopen+sort%3Aupdated-desc+label%3Aseverity%2Fmoderate)

### Minor Defects

A defect that does not affect data or functionality. It does not even need a
workaround. It does not impact productivity or efficiency. It is merely an
inconvenience. These defects are labeled with `type/bug` and `severity/minor`,
can be found
[here](https://github.com/tikv/client-java/issues?q=is%3Aissue+is%3Aopen+sort%3Aupdated-desc+label%3Aseverity%2Fminor)
