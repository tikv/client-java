# Contribution Guide

## Build the package

```
mvn clean package -Dmaven.test.skip=true
```

## Install the package to local maven repository

```
mvn clean install -Dmaven.test.skip=true
```

## Run tests

```
export RAWKV_PD_ADDRESSES=127.0.0.1:2379
export TXNKV_PD_ADDRESSES=127.0.0.1:2379
mvn clean test
```