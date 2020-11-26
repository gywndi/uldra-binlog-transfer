# uldra-binlog-trasfer

1. MySQL replicator that supports various target databases.
2. Parallel processing on a specific column basis
3. Data change processing according to binlog event.

## build & run
(For reference, Oracle ojdbc is not included in maven repository.)

    mvn install
    java -jar target/uldra-binlog-transfer-0.0.1.jar --config-file="uldra-config.yml"

## uldra-config.yml
```yaml
workerCount: 16
wokerQueueSize: 500
exporterPort: 42000

binlogServer: 127.0.0.1:3306
binlogServerID: 5
binlogServerUsername: repl
binlogServerPassword: repl
binlogInfoFile: "binlog.info"

binlogPolicies:
- name: origin.s_user
  partitionKey: userid
  rowHandlerClassName: net.gywn.binlog.api.RowHandlerImpl
  rowHandlerParams:
    param1: "a"
    param2: "b"
  replicatPolicies:
  - name: "t_user"
    softDelete: true
    upsertMode: true
    colums: ["userid", "name", "phone:cell", "reg_dttm", "mod_tmsp"]

targetDataSource: !!org.apache.commons.dbcp2.BasicDataSource
  driverClassName: com.mysql.jdbc.Driver
  url: jdbc:mysql://127.0.0.1:3306/target?autoReconnect=true&cacheServerConfiguration=true&useLocalSessionState=true&elideSetAutoCommits=true&connectTimeout=3000&socketTimeout=60000&useSSL=false&useAffectedRows=true&cacheCallableStmts=true&noAccessToProcedureBodies=true&characterEncoding=utf8&characterSetResults=utf8&connectionCollation=utf8_bin&sessionVariables=SQL_MODE='NO_AUTO_VALUE_ON_ZERO'
  username: target
  password: target
  maxTotal: 30
  maxWaitMillis: 100
  validationQuery: SELECT 1
  testOnBorrow: false
  testOnReturn: false
  testWhileIdle: true
  timeBetweenEvictionRunsMillis: 60000
  minEvictableIdleTimeMillis : 1200000
  numTestsPerEvictionRun : 10

targetHandlerClassName: "net.gywn.binlog.dbms.UldraMysqlApplier"
# targetHandlerParamFile: "uldra-mysql-applier.yml"
```



### 1. binlogPolicies

- name : source table name ex) {{database}}.{{table_name}}
- partitionKey: partition column for parallel processing, included in pk is recommended.
- rowHandlerClassName: custom handler to modify row from binlog, must implement net.gywn.binlog.api.RowHandler
- rowHandlerParams: parameters  for row handler
- replicatPolicies: replicate policies for target table
    - name : target table name
    - softDelete: update null except pk if delete event income
    - upsertMode: convert insert to upsert query 
    - colums:  target columns to replicate, default is origin columns

### 2. targetHandlerClassName
Currently, only a single mysql target is implemented.
If you want to pass the configuration file to applier, you can define additionally in targetHandlerParamFile.

**target handler interface** 

```java
public interface TargetHandler {

	public void init(final UldraConfig uldraConfig) throws Exception;

	public void begin(final Connection connection) throws SQLException;

	public void commit(final Connection connection) throws SQLException;

	public void rollback(final Connection connection) throws SQLException;

	public void insert(final Connection connection, final TargetOperation operation) throws SQLException;

	public void upsert(final Connection connection, final TargetOperation operation) throws SQLException;

	public void update(final Connection connection, final TargetOperation operation) throws SQLException;

	public void delete(final Connection connection, final TargetOperation operation) throws SQLException;

	public void softdel(final Connection connection, final TargetOperation operation) throws SQLException;

}
```
Please refer to net.gywn.binlog.dbms.UldraMysqlApplier.

Still in development. Monitoring and logging implementation is required.
