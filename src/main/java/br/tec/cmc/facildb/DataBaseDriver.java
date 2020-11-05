/**
 * Copyright 2020 Claudio Montenegro Chaves - CMC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package br.tec.cmc.facildb;

public enum DataBaseDriver {

    H2("org.h2.Driver"),
    MYSQL("com.mysql.jdbc.Driver"),
    ORACLE("oracle.jdbc.driver.OracleDriver"),
    FIREBIRD("org.firebirdsql.jdbc.FBDriver"),
    POSTGESQL("org.postgresql.Driver"),
    MSSQL("com.microsoft.sqlserver.jdbc.SQLServerDriver"),
    MSSQL_JTDS("net.sourceforge.jtds.jdbc.Driver"),
    PROGRESS("com.ddtek.jdbc.openedge.OpenEdgeDriver"),
    PERVASIVE_PSQL("com.pervasive.jdbc.v2.Driver");

    private String driver;

    DataBaseDriver(String driver) {
        this.driver = driver;
    }

    public String driver() {
        return driver;
    }
}
