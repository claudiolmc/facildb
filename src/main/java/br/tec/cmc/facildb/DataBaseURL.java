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


public enum DataBaseURL {

    H2("jdbc:h2:tcp://$dbServer:$dbPort//$dbDir/$dbName"),
    MYSQL("jdbc:mysql://$dbServer:$dbPort/$dbName"),
    ORACLE("jdbc:oracle:thin:@//$dbServer:$dbPort/$dbName"),
    FIREBIRD("jdbc:firebirdsql://$dbServer:$dbPort/$dbName"),
    POSTGRESQL("jdbc:postgresql://$dbServer:$dbPort/$dbName"),
    MSSQL("jdbc:sqlserver://$dbServer:$dbPort),databaseName=$dbName"),
    MSSQL_2("jdbc:sqlserver://$dbServer),instanceName=$instanceName),databaseName=$dbName"),
    MSSQL_JTDS("jdbc:jtds:sqlserver://$dbServer:$dbPort/$dbName"),
    MSSQL_JTDS_2("jdbc:jtds:sqlserver://$dbServer/$dbName),instance=$instanceName"),
    PROGRESS("jdbc:datadirect:openedge://$dbServer:$dbPort),databaseName=$dbName"),
    PERVASIVE_PSQL("jdbc:pervasive://$dbServer:$dbPort/$dbName");

    private String url;

    DataBaseURL(String url) {
        this.url = url;
    }

    public String url() {
        return url;
    }
}
