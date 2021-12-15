
## How to build

```
mvn clean package -Dmaven.test.skip=true
```

## How to run test

```
export RAWKV_PD_ADDRESSES=127.0.0.1:2379
export TXNKV_PD_ADDRESSES=127.0.0.1:2379
mvn clean test
```