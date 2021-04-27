# Database Command Line Tool

## Command

### Usage

```shell
Usage: dbcli [-hV] -j=<jdbcUrl> [-u=<username>] [-p=<password>] [-f]
             [-t=<time>] [-i=<interval>] [-c] [-r] inputs...
database command line tool
*     inputs...          SQL(s) or File(s) to execute.
  -h, --help             Show this help message and exit.
  -V, --version          Print version information and exit.
* -j, --jdbc=<jdbcUrl>   Database JDBC url to connect.
  -u, --username=<username>
                         Database username to connect.
  -p, --password=<password>
                         Database password to connect.
  -f, --file             Execute SQL statements in files.
                           Default: false
  -t, --times=<time>     Execute times.
                           Default: 1
  -i, --interval=<interval>
                         Interval time between SQL executions in milliseconds.
                           Default: 0
  -c, --connection       Create new JDBC connections for every request.
                           Default: false
  -r, --results          Show execution results.
                           Default: true
```

### Execute SQL

```shell
./dbcli -j [JDBC_URL] -u [USER] -p [PASSWORD] SQL_1 SQL_2
```

### Execute SQL File

```shell
./dbcli -j [JDBC_URL] -u [USER] -p [PASSWORD] -f SQL_FILE_1 SQL_FILE_2
```

### Execute multiple times

```shell
./dbcli -j [JDBC_URL] -u [USER] -p [PASSWORD] -t [TIMES] SQL_1
```

### help

```shell
./dbcli -h
./dbcli --help
```

### version

```shell
./dbcli -V
./dbcli --version
```

## JDBC URL:

* MySQL: jdbc:mysql://[IP]:[PORT]/[SCHEMA]?autoReconnect=true&characterEncoding=utf8&useSSL=false

* Oracle SID: jdbc:oracle:thin:@[IP]:[PORT]:[SID]

* Oracle Service Name: jdbc:oracle:thin:@//[IP]:[PORT]:[SID]/[SERVICENAME]

* PostgreSQL: jdbc:postgresql://[IP]:[PORT]/[DATABASE]
