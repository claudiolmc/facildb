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
package br.tec.cmc.facildb.metadata;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetadataDB {

    private DatabaseMetaData dmd;
    private Map<String, String> tableMap;

    public MetadataDB(DatabaseMetaData dmd) {
        this.dmd = dmd;
    }

    public List<String> getCatalogs() throws SQLException {
        List<String> list = new ArrayList<>();
        if (dmd != null) {
            ResultSet rs = dmd.getCatalogs();
            while (rs.next()) {
                list.add(rs.getString("TABLE_CAT"));
            }
            rs.close();
        }
        return list;
    }

    public List<String> getSchemas() throws SQLException {
        List<String> list = new ArrayList<>();
        if (dmd != null) {
            ResultSet rs = dmd.getSchemas();
            while (rs.next()) {
                list.add(rs.getString("TABLE_SCHEM"));
            }
            rs.close();
        }
        return list;
    }

    public List<String> getTableList(String catalog, String schema) throws SQLException {
        List<String> tables = new ArrayList<>();
        if (dmd != null) {
            ResultSet rs = dmd.getTables(catalog, schema, null, null);
            while (rs.next()) {
                String tableType = rs.getString("TABLE_TYPE").toUpperCase();
                if (tableType.equals("TABLE")) {
                    tables.add(rs.getString("TABLE_NAME"));
                }
            }
            rs.close();
        }
        return tables;
    }

    private void getTableListAsMap(String catalog, String schema) throws SQLException {
        if (tableMap == null) {
            List<String> tableList = getTableList(catalog, schema);
            tableMap = new HashMap<>();
            for (String tbName: tableList) {
                tableMap.put(tbName, tbName);
            }
        }
    }

    public Table getTable(String catalog, String schema, String tableName) throws SQLException {

        getTableListAsMap(catalog, schema);

        Table table = new Table();
        table.setCatalog(catalog);
        table.setSchema(schema);
        table.setTableName(tableName);
        table.setTableType("TABLE");

        Map<String, String> pks = getPrimaryKeysAsMap(catalog, schema, tableName);
        Map<String, String> idxs = getIndexedColumnsAsMap(catalog, schema, tableName);
        
        if (dmd != null) {
            ResultSet rs = dmd.getColumns(catalog, schema, tableName, null);

            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String columnType = "";
                int colType = rs.getInt("DATA_TYPE");
                String colTypeName = rs.getString("TYPE_NAME").toLowerCase();
                switch (colType) {
                    case Types.BIGINT:
                        columnType = "Long";
                        break;
                    case Types.INTEGER:
                        columnType = "Integer";
                        break;
                    case Types.SMALLINT:
                        columnType = "Integer";
                        break;
                    case Types.TINYINT:
                        columnType = "Integer";
                        break;
                    case Types.DECIMAL:
                        columnType = "BigDecimal";
                        break;
                    case Types.DOUBLE:
                        columnType = "BigDecimal";
                        break;
                    case Types.FLOAT:
                        columnType = "BigDecimal";
                        break;
                    case Types.NUMERIC:
                        columnType = "BigDecimal";
                        break;
                    case Types.REAL:
                        columnType = "BigDecimal";
                        break;
                    case Types.CHAR:
                        columnType = "String";
                        break;
                    case Types.VARCHAR:
                        columnType = "String";
                        break;
                    case Types.LONGNVARCHAR:
                        columnType = "String";
                        break;
                    case Types.LONGVARCHAR:
                        columnType = "String";
                        break;
                    case Types.NCHAR:
                        columnType = "String";
                        break;
                    case Types.BINARY:
                        columnType = "Boolean";
                        break;
                    case Types.BIT:
                        columnType = "Boolean";
                        break;
                    case Types.VARBINARY:
                        columnType = "Boolean";
                        break;
                    case Types.TIME:
                        columnType = "Date";
                        break;
                    case Types.TIMESTAMP:
                        columnType = "Date";
                        break;
                    case Types.TIME_WITH_TIMEZONE:
                        columnType = "Date";
                        break;
                    case Types.TIMESTAMP_WITH_TIMEZONE:
                        columnType = "Date";
                        break;
                    case Types.CLOB:
                        columnType = "String";
                        break;
                    case Types.ARRAY:
                        columnType = "List";
                        break;
                    case Types.JAVA_OBJECT:
                        columnType = "Object";
                        break;
                }
                if (columnType.equals("")) {
                    switch (colTypeName) {
                        case "int":
                            columnType = "Integer";
                            break;
                        case "date":
                            columnType = "Date";
                            break;
                        case "varchar":
                            columnType = "String";
                            break;
                        case "bit":
                            columnType = "Boolean";
                            break;
                        case "enum":
                            columnType = "String";
                            break;
                    }
                }
                if (columnType.equals("")) {
                    columnType = "String";
                }
                Column column = new Column(columnName, columnType);
                column.setColumnSize(rs.getInt("COLUMN_SIZE"));
                String isnlb = rs.getString("IS_NULLABLE");
                boolean isNullable = false;
                if (isnlb.equalsIgnoreCase("yes")) {
                    isNullable = true;
                }
                column.setNullable(isNullable);
                if (pks.containsKey(columnName)) {
                    column.setPrimaryKey(true);
                }
                if (idxs.containsKey(columnName)) {
                    column.setIndexed(true);
                }

                // Check if column if FK
                if (columnName.endsWith("_id")) {
                    String tbn = columnName.substring(0, columnName.lastIndexOf("_id"));
                    if (tableMap.containsKey(tbn)) {
                        column.setFkTableName(tbn);
                    }
                }
                table.addColumn(column);
            }
            if (rs != null) {
                rs.close();
            }
        }
        return table;
    }

    public List<String> getPrimaryKeys(String catalog, String schema, String tableName) throws SQLException {
        List<String> pks = new ArrayList<>();
        ResultSet rs = dmd.getPrimaryKeys(catalog, schema, tableName);

        while (rs.next()) {
            pks.add(rs.getString("COLUMN_NAME"));
        }
        return pks;
    }

    public List<String> getIndexedColumns(String catalog, String schema, String tableName) throws SQLException {
        List<String> list = new ArrayList<>();
        ResultSet rs = dmd.getIndexInfo(catalog, schema, tableName, false, false);
        while (rs.next()) {
            list.add(rs.getString("COLUMN_NAME"));
        }
        return list;
    }

    private Map<String, String> getPrimaryKeysAsMap(String catalog, String schema, String tableName) throws SQLException {
        Map<String, String> map = new HashMap<>();
        ResultSet rs = dmd.getPrimaryKeys(catalog, schema, tableName);

        while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            map.put(columnName, columnName);
        }
        return map;
    }

    private Map<String, String> getIndexedColumnsAsMap(String catalog, String schema, String tableName) throws SQLException {
        Map<String, String> map = new HashMap<>();
        ResultSet rs = dmd.getIndexInfo(catalog, schema, tableName, false, false);
        while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            if (!columnName.equals("id")) {
                map.put(columnName, columnName);
            }
        }
        return map;
    }

}
