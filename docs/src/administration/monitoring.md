## Java Client Metrics

Client Java supports exporting metrics to Prometheus using poll mode and viewing on Grafana. The following steps shows how to enable this function.

### Step 1: Enable metrics exporting

- set the config `tikv.metrics.enable` to `true`
- call TiConfiguration.setMetricsEnable(true)

### Step 2: Set the metrics port

- set the config `tikv.metrics.port`
- call TiConfiguration.setMetricsPort

Default port is 3140.

### Step 3: Config Prometheus

Add the following config to `conf/prometheus.yml` and restart Prometheus.

```yaml
- job_name: "tikv-client"
    honor_labels: true
    static_configs:
    - targets:
        - '127.0.0.1:3140'
        - '127.0.0.2:3140'
        - '127.0.0.3:3140'
```

### Step 4: Config Grafana

Import the [Client-Java-Summary dashboard config](/metrics/grafana/client_java_summary.json) to Grafana.