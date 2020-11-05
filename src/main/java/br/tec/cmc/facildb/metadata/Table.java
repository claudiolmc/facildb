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

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class Table {
    
    private String catalog = "";
    private String schema = "";
    private String tableName = "";
    private String tableType = "";
    private List<Column> columns;

    public Table() {
    }
    
    public Table(String json) {
        this(new JSONObject(json));
    }
    
    public Table(JSONObject js) {
        this.setCatalog(js.optString("catalog"));
        this.setSchema(js.optString("schema"));
        this.setTableName(js.optString("tableName"));
        this.setTableType(js.optString("tableType"));
        JSONArray ar = js.optJSONArray("columns");
        columns = new ArrayList<>();
        if (ar != null) {
            for (int i=0; i<ar.length(); i++) {
                columns.add(new Column(ar.getJSONObject(i)));
            }
        }
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }
    
    public void addColumn(Column column) {
        if (this.columns == null) {
            this.columns = new ArrayList<>();
        }
        this.columns.add(column);
    }
    
    public JSONObject toJSONObject() {
        JSONObject js = new JSONObject();
        js.put("catalog", this.getCatalog());
        js.put("schema", this.getSchema());
        js.put("tableName", this.getTableName());
        js.put("tableType", this.getTableType());
        if (columns != null) {
            JSONArray cols = new JSONArray();
            for (Column column: columns) {
                cols.put(column.toJSONObject());
            }
            js.put("columns", cols);
        }
        return js;
    }

    public String toString() {
        return toJSONObject().toString();
    }
    
    public String toString(int ident) {
        return toJSONObject().toString(ident);
    }
}
