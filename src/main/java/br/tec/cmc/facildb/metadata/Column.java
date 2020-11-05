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

import org.json.JSONObject;

import br.tec.cmc.facildb.util.StringUtil;

public class Column {
    
    private String columnName = "";
    private String columnType = "";
    private int columnSize;
    private String fkTableName;
    private boolean nullable;
    private boolean isPrimaryKey;
    private boolean isIndexed;
    private static final String STRING = "String"; 
    
    public Column() {
    }
    
    public Column(String json) {
        this(new JSONObject(json));
    }
    
    public Column(JSONObject js) {
        this.setColumnName(js.optString("columnName"));
        this.setColumnType(js.optString("columnType"));
        this.setColumnSize(js.optInt("columnSize"));
        this.setNullable(js.optBoolean("nullable"));
    }

    public Column(String columnName, String columnType) {
        setColumnName(columnName);
        setColumnType(columnType);
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getVarName() {
        String varName = StringUtil.varNameToCamelCase(columnName);
        if (varName.equals("class")) {
            varName = "clazz";
        }
        return varName;
    }

    public String getVarNameNoIdSuffix() {
        String name = getVarName();
        int p = name.lastIndexOf("Id");
        if (p > 0) {
            name = name.substring(0, p);
        }
        return name;
    }

    public String getColumnNameCamelCase() {
        return StringUtil.toCamelCase(columnName);
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    public int getColumnSize() {
        return columnSize;
    }

    public void setColumnSize(int columnSize) {
        this.columnSize = columnSize;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        isPrimaryKey = primaryKey;
    }

    public boolean isIndexed() {
        return isIndexed;
    }

    public void setIndexed(boolean indexed) {
        isIndexed = indexed;
    }

    public String getFkTableName() {
        return fkTableName;
    }

    public String getFkTableNameAsCamelCase() {
        return StringUtil.toCamelCase(fkTableName);
    }

    public void setFkTableName(String fkTableName) {
        this.fkTableName = fkTableName;
    }

    public String getNestedId() {
        String id = "";
        String[] values = fkTableName.split("_");
        if (values.length == 1) {
            id = values[0];
            if (id.length() > 4) {
                id = id.substring(0, 4);
            }
        } else {
            for (int i=0; i<values.length; i++) {
                id += values[i].substring(0, 2);
            }
        }
        return id;
    }

    public JSONObject toJSONObject() {
        return new JSONObject(this);
    }
    
    public String toString(int ident) {
        return toJSONObject().toString(ident);
    }

    public String getJsonType(String type) {
        String tp = "";
        switch (type) {
            case STRING:
                tp = STRING;
                break;
            case "Integer":
                tp = "Int";
                break;
            case "BigDecimal":
                tp = "Double";
                break;
            case "Long":
                tp = "Long";
                break;
            case "Boolean":
                tp = "Boolean";
                break;
            case "Date":
                tp = STRING;
                break;
            default:
                tp = STRING;
        }
        return tp;
    }
}
