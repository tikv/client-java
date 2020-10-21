# TiSpark Dev Tools Guide

## Formatting

### Java Format

TiKV Java Client formats its code using [Google-Java-Format Maven Plugin](https://github.com/coveooss/fmt-maven-plugin) which follows Google's code styleguide. It is also checked on CI before build.

1. In Intellij IDEA

    1. you should download the [Google-Java-format Plugin](https://plugins.jetbrains.com/plugin/8527-google-java-format) via marketplace. Restart IDE, and enable google-java-format by checking the box in `Other Settings`.

    2. you may also use [Java-Google-style xml file](./intellij-java-google-style.xml) and export the schema to Intellij:

        `Preferences`->`Editor`->`Code Style`->`Import Scheme`->`Intellij IDEA Code Style XML`.

2. You may also run [Java format script](./javafmt) before you commit & push to corresponding dev branch.

    ```shell script
   ./dev/javafmt
    ```