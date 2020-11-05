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

public enum DataBaseType {

    H2,
    MYSQL,
    ORACLE,
    FIREBIRD,
    POSTGRESQL,
    MSSQL,
    MSSQL_JTDS,
    PROGRESS,
    PERVASIVE_PSQL;
    
    public static DataBaseType fromString(String dbType) {
        
        dbType = dbType.toUpperCase();

        switch(dbType) {
            case "H2":
                return H2;
            case "MYSQL":
                return MYSQL;
            case "ORACLE":
                return ORACLE;
            case "FIREBIRD":
                return FIREBIRD;
            case "POSTGRESQL":
                return POSTGRESQL;
            case "MSSQL":
                return MSSQL;
            case "MSSQL_JTDS":
                return MSSQL_JTDS;  
            case "PROGRESS":
                return PROGRESS;
            case "PERVASIVE_PSQL":
                return PERVASIVE_PSQL;   
            default:
                return POSTGRESQL;
        }
    }
}
