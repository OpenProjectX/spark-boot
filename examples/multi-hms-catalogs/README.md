# Multi-HMS Catalog Example Notes

This example demonstrates multiple Hive Metastore-backed catalogs in one Spark application:

- `analytics_iceberg` uses Iceberg's Hive catalog client against the analytics HMS.
- `archive_iceberg` uses Iceberg's Hive catalog client against the archive HMS.
- `analytics_hive` uses Kyuubi's Hive catalog client against the analytics HMS.
- `archive_hive` uses Kyuubi's Hive catalog client against the archive HMS.

The `bigdata-test` Gradle plugin starts two independent MySQL-backed Hive Metastore instances and injects their Thrift URIs into the test and application JVMs.

## What Was Learned

### Kyuubi Hive Catalog Requires Hive Support

Kyuubi's `HiveTableCatalog` checks Spark SQL's static catalog implementation setting during catalog initialization. Without Hive support, Spark fails with:

```text
Require setting spark.sql.catalogImplementation to `hive` to enable hive support.
```

Spark Boot handles this automatically when any `kyuubi-hive` catalog is configured:

- `SparkSession.Builder.enableHiveSupport()`
- `spark.sql.catalogImplementation = hive`

This is a Kyuubi/Spark integration requirement, not a `bigdata-test` issue.

### Multiple HMS Catalogs Are Feasible

Spark can host multiple named SQL catalogs in one `SparkSession`. Each catalog gets its own metastore URI:

```hocon
spark.boot.catalogs {
  analytics_iceberg {
    type = "iceberg-hive"
    uri = "thrift://analytics-hms:9083"
  }

  archive_hive {
    type = "kyuubi-hive"
    uri = "thrift://archive-hms:9083"
  }
}
```

This works because the catalog name scopes the HMS client configuration:

- `spark.sql.catalog.analytics_iceberg.uri`
- `spark.sql.catalog.archive_hive.hive.metastore.uris`

### Configuration Isolation Matrix

Not every Hive or Hadoop client setting can be isolated per catalog. The practical boundary is:

| Configuration source | Example keys | Scope in one Spark JVM | Notes |
| --- | --- | --- | --- |
| Spark SQL catalog options | `spark.sql.catalog.<catalog>.uri`, `spark.sql.catalog.<catalog>.hive.metastore.uris`, `spark.sql.catalog.<catalog>.warehouse` | Per catalog | This is the supported path for different HMS URIs and catalog warehouses. |
| Spark Boot catalog config | `spark.boot.catalogs.<name>.uri`, `spark.boot.catalogs.<name>.warehouse`, `spark.boot.catalogs.<name>.properties.*` | Per catalog before Spark session creation | Spark Boot maps these into catalog-scoped Spark SQL properties. |
| `hive-site.xml` on classpath | `hive.metastore.uris`, `hive.metastore.client.socket.timeout`, `hive.metastore.sasl.enabled` | Shared | Spark/Hive load this as process/classpath configuration. Do not rely on separate `hive-site.xml` files per catalog in one JVM. Prefer catalog-scoped options where supported. |
| `core-site.xml` on classpath | `fs.defaultFS`, `hadoop.security.authentication`, `fs.s3a.*` | Shared | Hadoop `Configuration` is shared by the Spark session and Hadoop clients. |
| `hdfs-site.xml` on classpath | `dfs.nameservices`, `dfs.ha.namenodes.*`, `dfs.client.failover.proxy.provider.*` | Shared | Multiple HDFS nameservices can coexist only when described in the same shared Hadoop configuration. Separate conflicting files are not isolated per catalog. |
| `spark.hadoop.*` | `spark.hadoop.fs.s3a.endpoint`, `spark.hadoop.fs.s3a.access.key`, `spark.hadoop.hive.metastore.use.SSL` | Shared | Spark copies these into the session Hadoop configuration. |
| Hive metastore client jars/version | `spark.sql.hive.metastore.version`, `spark.sql.hive.metastore.jars`, `spark.sql.hive.metastore.jars.path` | Shared/static | These affect Hive client class loading for the Spark application, not a single catalog. |
| Kerberos login context | keytab, ticket cache, `krb5.conf`, KDC/realm | Shared JVM/security context | Multiple HMS catalogs should use one compatible Kerberos context, such as same realm or cross-realm trust. Independent KDCs are not a reliable target in one JVM. |

Rule of thumb:

- Put different HMS addresses in catalog-scoped properties.
- Put common Hadoop, HDFS, S3, SSL, and Kerberos settings in the shared Spark/Hadoop configuration.
- If two catalogs require incompatible shared settings, run separate Spark applications or put a federation layer in front.

### S3 Is the Main Boundary

Spark has one active Hadoop configuration per `SparkSession`. Settings such as these are shared:

```text
spark.hadoop.fs.s3a.endpoint
spark.hadoop.fs.s3a.access.key
spark.hadoop.fs.s3a.secret.key
spark.hadoop.fs.s3a.aws.credentials.provider
```

That means multiple HMS catalogs are fine, but multiple incompatible S3 endpoints or credentials in the same Spark session are not a clean Spark-level abstraction.

Hive Metastore also validates database and table locations server-side. If an Iceberg or Hive table uses an `s3a://...` location, the HMS container/server may need matching S3 configuration and credentials too. Supplying S3 config only to the Spark driver is not always enough.

For that reason this example uses local file warehouses. It keeps the example focused on multi-HMS catalog behavior rather than cross-service object-store credential plumbing.

## `bigdata-test` Role

The multi-instance support in `bigdata-test` worked for this use case:

- It starts two independent HMS services.
- Each HMS gets its own backing MySQL database.
- It injects per-instance Thrift URIs as system properties.

The final example reads those injected properties directly and maps them into Spark Boot catalog properties before creating the Spark session.

## Practical Guidance

Use one Spark application when:

- You need to read/write tables across multiple HMS catalogs.
- The catalogs can share the same Hadoop filesystem configuration.
- Warehouses are on local paths, HDFS, or a shared object-store endpoint.

Use separate Spark applications or isolated sessions when:

- Catalogs need different S3 endpoints.
- Catalogs need incompatible S3 credentials.
- HMS services require different server-side filesystem configuration.

Kyuubi and Iceberg here are client libraries only. This example does not require a separate Kyuubi server or Gravitino server.
