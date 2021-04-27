/*
 * Copyright 2013-2021 https://github.com/seraphain.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.seraphain.dbcli;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Visibility;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Slf4j
@Command(name = "dbcli",
         version = "1.0.0",
         mixinStandardHelpOptions = true,
         sortOptions = false,
         requiredOptionMarker = '*',
         description = "Database Command Line Tool")
public class DatabaseCommandLine implements Callable<Integer> {

    private static final String LINE_SEPARATOR = System.lineSeparator();

    private static final String SQL_SEPARATOR = ";";

    @Option(names = { "-j", "--jdbc" },
            description = "Database JDBC url to connect.",
            required = true,
            order = 1)
    private String jdbcUrl;

    @Option(names = { "-u", "--username" },
            description = "Database username to connect.",
            required = false,
            defaultValue = "",
            order = 2)
    private String username = StringUtils.EMPTY;

    @Option(names = { "-p", "--password" },
            description = "Database password to connect.",
            required = false,
            defaultValue = "",
            order = 3)
    private String password = StringUtils.EMPTY;

    @Option(names = { "-f", "--file" },
            description = "Execute SQL statements in files.",
            required = false,
            defaultValue = "false",
            showDefaultValue = Visibility.ALWAYS,
            order = 4)
    private boolean isFile = false;

    @Option(names = { "-t", "--times" },
            description = "Execute times.",
            required = false,
            defaultValue = "1",
            showDefaultValue = Visibility.ALWAYS,
            order = 5)
    private long time = 1;

    @Option(names = { "-i", "--interval" },
            description = "Interval time between SQL executions in milliseconds.",
            required = false,
            defaultValue = "0",
            showDefaultValue = Visibility.ALWAYS,
            order = 6)
    private long interval = 0;

    @Option(names = { "-c", "--connection" },
            description = "Create new JDBC connections for every request.",
            required = false,
            defaultValue = "false",
            showDefaultValue = Visibility.ALWAYS,
            order = 7)
    private boolean createNewConnections = false;

    @Option(names = { "-r", "--results" },
            description = "Show execution results.",
            required = false,
            defaultValue = "true",
            showDefaultValue = Visibility.ALWAYS,
            order = 8)
    private boolean showResults = true;

    @Parameters(arity = "1..*",
                description = "SQL(s) or File(s) to execute.",
                paramLabel = "inputs")
    private String[] inputs;

    public Integer call() throws Exception {
        List<String> sqls = getSqls();
        if (log.isInfoEnabled()) {
            log.info("Following SQL(s) will be executed:\n{}\n", StringUtils.join(sqls, LINE_SEPARATOR));
        }
        try {
            execute(sqls);
        } catch (IllegalArgumentException | SQLException e) {
            log.error("Execute SQL(s) error. SQL(s):\n{}", sqls, e);
            return 1;
        }
        return 0;
    }

    private List<String> getSqls() {
        if (inputs == null || inputs.length == 0) {
            return Collections.emptyList();
        }
        List<String> sqls;
        if (isFile) {
            sqls = Arrays.stream(inputs).filter(StringUtils::isNotBlank).map(File::new).filter(File::exists)
                    .filter(File::isFile).filter(File::canRead).map(this::readString).map(this::separateSQL)
                    .flatMap(List::stream).collect(Collectors.toList());
        } else {
            sqls = Arrays.asList(inputs);
        }
        return sqls.stream().map(s -> StringUtils.split(s, LINE_SEPARATOR)).flatMap(Arrays::stream)
                .map(s -> StringUtils.split(s, SQL_SEPARATOR)).flatMap(Arrays::stream).filter(StringUtils::isNotBlank)
                .map(StringUtils::trim).collect(Collectors.toList());
    }

    private String readString(File file) {
        try {
            return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.error("Read file error. File: {}", file, e);
            return null;
        }
    }

    private List<String> separateSQL(String sql) {
        return Arrays.stream(StringUtils.split(sql, LINE_SEPARATOR)).filter(StringUtils::isNotBlank)
                .map(StringUtils::trim).filter(s -> !StringUtils.startsWith(s, "-") && !StringUtils.startsWith(s, "#"))
                .collect(Collector.of(ArrayList::new, (list, s) -> {
                    if (list.isEmpty() || list.get(list.size() - 1).endsWith(SQL_SEPARATOR)) {
                        list.add(s);
                    } else {
                        list.set(list.size() - 1, list.get(list.size() - 1) + s);
                    }
                }, (left, right) -> {
                    if (left.isEmpty()) {
                        return right;
                    }
                    if (right.isEmpty()) {
                        return left;
                    }
                    int i = 0;
                    while (!StringUtils.endsWith(left.get(left.size() - 1), SQL_SEPARATOR)) {
                        left.set(left.size() - 1, left.get(left.size() - 1) + right.get(i));
                        i++;
                    }
                    for (int j = i; j < right.size(); j++) {
                        left.add(right.get(j));
                    }
                    return left;
                }));
    }

    private void execute(List<String> sqls) throws SQLException {
        if (createNewConnections) {
            for (int i = 0; i < time; i++) {
                executeSQLsInNewConnection(sqls);
            }
        } else {
            try (Connection connection = createConnection()) {
                for (int i = 0; i < time; i++) {
                    executeSQLsInSameConnection(connection, sqls);
                }
            }
        }
    }

    private void executeSQLsInNewConnection(List<String> sqls) throws SQLException {
        for (String sql : sqls) {
            sleep(interval);
            log.info("Executing SQL: {}", sql);
            long start = System.currentTimeMillis();
            try (Connection connection = createConnection()) {
                execute(connection, sql.endsWith(SQL_SEPARATOR) ? sql.substring(0, sql.length() - 1) : sql);
            }
            long end = System.currentTimeMillis();
            log.info("Executed SQL: {}\tTime cost: {} ms.", sql, end - start);
        }
    }

    private void executeSQLsInSameConnection(Connection connection, List<String> sqls) throws SQLException {
        for (String sql : sqls) {
            sleep(interval);
            log.info("Executing SQL: {}", sql);
            long start = System.currentTimeMillis();
            execute(connection, sql.endsWith(SQL_SEPARATOR) ? sql.substring(0, sql.length() - 1) : sql);
            long end = System.currentTimeMillis();
            log.info("Executed SQL: {}\tTime cost: {} ms.", sql, end - start);
        }
    }

    private Connection createConnection() throws SQLException {
        if (jdbcUrl.startsWith("jdbc:mysql:")) {
            DriverManager.registerDriver(new com.mysql.cj.jdbc.Driver());
        } else if (jdbcUrl.startsWith("jdbc:oracle:")) {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
        } else if (jdbcUrl.startsWith("jdbc:postgresql:")) {
            DriverManager.registerDriver(new org.postgresql.Driver());
        } else {
            throw new IllegalArgumentException("Unsupported JDBC type.");
        }
        long start = System.currentTimeMillis();
        Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
        long end = System.currentTimeMillis();
        log.info("JDBC connection created. Time cost: {}", end - start);
        return connection;
    }

    private void execute(Connection connection, String sql) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            boolean isResultSet = stmt.execute(sql);
            if (showResults) {
                if (isResultSet) {
                    try (ResultSet rs = stmt.getResultSet()) {
                        printResultSet(rs);
                    }
                } else {
                    log.info("Update count: {}", stmt.getUpdateCount());
                }
            }
        }
    }

    private void printResultSet(ResultSet rs) throws SQLException {
        if (log.isInfoEnabled()) {
            StringBuilder builder = new StringBuilder();
            builder.append("Results:").append(LINE_SEPARATOR).append(LINE_SEPARATOR);
            int rowCount = 0;
            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                builder.append("*************************** ");
                builder.append(rs.getRow());
                builder.append(". row ***************************");
                builder.append(LINE_SEPARATOR);
                for (int i = 1; i <= columnCount; i++) {
                    builder.append(rs.getMetaData().getColumnLabel(i));
                    builder.append(": ");
                    builder.append(rs.getObject(i));
                    builder.append(LINE_SEPARATOR);
                }
                builder.append(LINE_SEPARATOR);
                rowCount++;
            }
            builder.append(rowCount);
            builder.append(" rows in set.");
            builder.append(LINE_SEPARATOR);
            log.info(builder.toString());
        }
    }

    private void sleep(long interval) {
        if (interval <= 0) {
            return;
        }
        try {
            Thread.sleep(interval);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String... args) {
        int exitCode = new CommandLine(new DatabaseCommandLine()).execute(args);
        System.exit(exitCode);
    }

}
