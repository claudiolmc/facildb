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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import br.tec.cmc.facildb.util.DataType;
import br.tec.cmc.facildb.util.DateUtil;

/**
 * FacilDB
 *
 * @author claudio.montenegro@inoutway.com.br
 * @since Aug 14, 2012
 */
public class FacilDB {

    private Connection conn;
    private String uri;
    private List<Object> queryParams;
    private String[] fieldsSEL;
    private String[] sqlAliases;
    private String[] fieldsINS;
    private String[] fieldsUPD;
    private SQLCommand operation;
    private String from = "";
    private String whereSEL = "";
    private String whereUPD = "";
    private String whereDEL = "";
    private String orderBy = "";
    private String tableNames = "";
    private int maxResults;
    private StringBuilder sqlSEL;
    private StringBuilder sqlINS;
    private StringBuilder sqlUPD;
    private StringBuilder sqlDEL;
    private StringBuilder sqlCRE;
    private PreparedStatement prepStatSEL;
    private PreparedStatement prepStatINS;
    private PreparedStatement prepStatUPD;
    private PreparedStatement prepStatDEL;
    private PreparedStatement prepStatCRE;
    private CallableStatement callableStatement;
    private String schema;
    private boolean available;
    private String databaseId;
    private String lastSQL;
    protected DataBaseType dbType;
    protected String procedure;

    private static final String NO_DATA_AVAILABLE = "No data available";
    private static final String TABLE_NAME_NOT_INFORMED = "Table name nto informed";

    public FacilDB() {  
        // Empty constructor
    }
        
    public FacilDB(String uri, String username, String password) throws SQLException {
        setConnection(uri, username, password);
    }
    
    protected void setConnection(String uri, String username, String password) throws SQLException {
        this.uri = uri;
        DriverManager.setLoginTimeout(15);
        conn = DriverManager.getConnection(uri, username, password);
        this.queryParams = new ArrayList<>();
    }
    
    public void setConnection(Connection conn) {
        this.conn = conn;
        this.queryParams = new ArrayList<>();
    }

    public Connection getConnection() {
        return conn;
    }
   
    public String getUri() {
        return uri;
    }

    public String getDatabaseId() {
        return databaseId;
    }

    public void setDatabaseId(String databaseId) {
        this.databaseId = databaseId;
    }
        
    public void closeConnection() throws SQLException {
        if (conn != null) {
            conn.close(); 
        }
    }
    
    public boolean connectionIsValid() throws SQLException {
        boolean resp = false;
        if (conn != null) {
            if (dbType == DataBaseType.MSSQL_JTDS) {
                // JTDS nao suporta o isValid()
                resp = true;
            } else {
                resp = conn.isValid(10);
            }
        }
        return resp;
    }

    public String getLastSQL() {
        return lastSQL;
    }

    public FacilDB sql(String sqls) throws SQLException {
        String sqlsl = sqls.toLowerCase();
        if (sqlsl.indexOf("select") >= 0) {
            this.operation = SQLCommand.SELECT;
            this.sqlSEL = new StringBuilder(sqls);
            if (prepStatSEL != null) prepStatSEL.close();
            prepStatSEL = null;
        } else if (sqlsl.indexOf("insert") >= 0) {
            this.operation = SQLCommand.INSERT;
            this.sqlINS = new StringBuilder(sqls);
            if (prepStatINS!= null) prepStatINS.close();
            prepStatINS = null;
        } else if (sqlsl.indexOf("update") >= 0) {
            this.operation = SQLCommand.UPDATE;
            this.sqlUPD = new StringBuilder(sqls);
            if (prepStatUPD != null) prepStatUPD.close();
            prepStatUPD = null;
        } else if (sqlsl.indexOf("delete") >= 0) {
            this.operation = SQLCommand.DELETE;
            this.sqlDEL = new StringBuilder(sqls);
            if (prepStatDEL != null) prepStatDEL.close();
            prepStatDEL = null;
        } else if (sqlsl.indexOf("create") >= 0) {
            this.operation = SQLCommand.CREATE;
            this.sqlCRE = new StringBuilder(sqls);
            if (prepStatCRE != null) prepStatCRE.close();
            prepStatCRE = null;
        }
        
        this.sqlAliases = null;
        this.whereSEL = null;
        this.queryParams = new ArrayList<>();
        lastSQL = sqls;
        return this;
    }
    
    public FacilDB call(String procedure) throws SQLException {
        this.operation = SQLCommand.PROCEDURE;
        this.procedure = procedure;
        callableStatement = conn.prepareCall("{call " + procedure + "}");
        return this;
    }
    
    public FacilDB param(Object value) {
        return setParam(value);
    }
    
    public FacilDB param(int index, Object value) {
        int tam = this.queryParams.size();
        if (index < tam) {
            if (tam > 0) {
                this.queryParams.remove(index);
            }
            this.queryParams.add(index, value);
        } else if (index == tam) {
            this.queryParams.add(value);
        } else {
            for (int i=tam; i<index; i++) {
                this.queryParams.add(null);
            }
            this.queryParams.add(value);
        }
        return this;
    }
    
    public FacilDB setParam(Object value) {
        this.queryParams.add(value);
        return this;
    }
    
    public FacilDB outParam(Integer index, DataType type) throws SQLException {
        return outParam(index, type, null);
    }
    
    public FacilDB outParam(Integer index, DataType type, Object value) throws SQLException {
        if (this.callableStatement != null) {
            int sqlType = java.sql.Types.VARCHAR;
            switch (type) {
                case BOOLEAN:
                    sqlType = java.sql.Types.BOOLEAN;
                    break;
                case CLOB:
                    sqlType = java.sql.Types.CLOB;
                    break;
                case DATE:
                    sqlType = java.sql.Types.DATE;
                    break;
                case DECIMAL:
                    sqlType = java.sql.Types.DOUBLE;
                    break;
                case INTEGER:
                    sqlType = java.sql.Types.INTEGER;
                    break;
                case LONG:
                    sqlType = java.sql.Types.BIGINT;
                    break;
                case BIT:
                    sqlType = java.sql.Types.BIT;
                    break;
                case CHAR:
                    sqlType = java.sql.Types.CHAR;
                    break;
                case FLOAT:
                    sqlType = java.sql.Types.FLOAT;
                    break;
                case TIME:
                    sqlType = java.sql.Types.TIME;
                    break;
                case TIMESTAMP:
                    sqlType = java.sql.Types.TIMESTAMP;
                    break;
            }
            this.callableStatement.registerOutParameter(index+1, sqlType);
            param(index, value);
        }
        return this;
    }
    
    public Object getOutParam(Integer index) throws SQLException {
        Object value = null;
        if (this.callableStatement != null) {
            value = this.callableStatement.getObject(index+1);
        }
        return value;
    }
    
    public String getStringOutParam(Integer index) throws SQLException {
        String value = null;
        if (this.callableStatement != null) {
            value = this.callableStatement.getString(index+1);
        }
        return value;
    }
    
    public Integer getIntOutParam(Integer index) throws SQLException {
        Integer value = null;
        if (this.callableStatement != null) {
            value = this.callableStatement.getInt(index+1);
        }
        return value;
    }
    
    public Double getDoubleOutParam(Integer index) throws SQLException {
        Double value = null;
        if (this.callableStatement != null) {
            value = this.callableStatement.getDouble(index+1);
        }
        return value;
    }
    
    public Date getDateOutParam(Integer index) throws SQLException {
        Date value = null;
        if (this.callableStatement != null) {
            value = this.callableStatement.getDate(index+1);
        }
        return value;
    }
    
    public Boolean getBooleanOutParam(Integer index) throws SQLException {
        Boolean value = null;
        if (this.callableStatement != null) {
            value = this.callableStatement.getBoolean(index+1);
        }
        return value;
    }
    
    public boolean isAvailable() {
        return available;
    }

    public void setAvailable(boolean available) {
        this.available = available;
    }
    
    public void resetInternalVariables() {
        this.queryParams = new ArrayList<Object>();
        this.fieldsSEL = null;
        this.sqlAliases = null;
        this.fieldsINS = null;
        this.fieldsUPD = null;
        this.operation = SQLCommand.EMPTY;
        this.from = "";
        this.whereSEL = "";
        this.whereUPD = "";
        this.whereDEL = "";
        this.orderBy = "";
        this.tableNames = "";
        this.maxResults = 0;
        this.sqlSEL = new StringBuilder();
        this.sqlINS = new StringBuilder();
        this.sqlUPD = new StringBuilder();
        this.sqlDEL = new StringBuilder();
        this.sqlCRE = new StringBuilder();
        if (this.prepStatSEL != null) {
            try { prepStatSEL.close(); } catch (Exception ee) {;}
            this.prepStatSEL = null;
        }
        if (this.prepStatINS != null) {
            try { prepStatINS.close(); } catch (Exception ee) {;}
            this.prepStatINS = null;
        }
        if (this.prepStatUPD != null) {
            try { prepStatUPD.close(); } catch (Exception ee) {;}
            this.prepStatUPD = null;
        }
        if (this.prepStatDEL != null) {
            try { prepStatDEL.close(); } catch (Exception ee) {;}
            this.prepStatDEL = null;
        }
        if (this.prepStatCRE != null) {
            try { prepStatCRE.close(); } catch (Exception ee) {;}
            this.prepStatCRE = null;
        }
        if (this.callableStatement != null) {
            try { callableStatement.close(); } catch (Exception ee) {;}
            this.callableStatement = null;
        }
    }
    
    public FacilDB fields(String[] fields) {
        return setFields(fields);
    }
    
    @Deprecated
    public FacilDB setFields(String[] fields) {
        switch (this.operation) {
            case SELECT:
                this.fieldsSEL = fields;
                break;
            case INSERT:
                this.fieldsINS = fields;
                break;
            case UPDATE:
                this.fieldsUPD = fields;
                break;
            case PROCEDURE:
                this.fieldsSEL = fields;
                break;
        }
        return this;
    }
    
    public FacilDB fields(String sFields) {
        return setFields(sFields);
    }
    
    @Deprecated
    public FacilDB setFields(String sFields) {
        String[] fields = sFields.split(",");
        for (int i=0; i<fields.length; i++) {
            fields[i] = fields[i].trim();
        }
        return setFields(fields);
    }
    
    public FacilDB sqlAlias(String aliases) {
        String[] als = aliases.split(",");
        for (int i=0; i<als.length; i++) {
            als[i] = als[i].trim();
        }
        return sqlAlias(als);
    }
    
    public FacilDB sqlAlias(String[] aliases) {
        this.sqlAliases = aliases;
        return this;
    }
    
    public FacilDB schema(String schema) {
        this.schema = schema;
        return this;
    }
    
    public FacilDB select(String fields) throws SQLException {
        String[] flds = fields.split(",");
        for (int i=0; i<flds.length; i++) {
            flds[i] = flds[i].trim();
        }
        return select(flds);
    }
    
    public FacilDB select(String[] fields) throws SQLException {
        this.operation = SQLCommand.SELECT;
        this.sqlSEL = new StringBuilder();
        if (prepStatSEL != null) prepStatSEL.close();
        prepStatSEL = null;
        this.fieldsSEL = fields;
        this.sqlAliases = null;
        this.whereSEL = null;
        return this;
    }
    
    public FacilDB maxResults(int maxResults) {
        this.maxResults = maxResults;
        return this;
    }
    
    public FacilDB insert(String tableName) throws SQLException {
        this.operation = SQLCommand.INSERT;
        this.sqlINS = new StringBuilder();
        if (prepStatINS != null) prepStatINS.close();
        prepStatINS = null;
        this.tableNames = tableName;
        return this;
    }
    
    public FacilDB update(String tableName) throws SQLException {
        this.operation = SQLCommand.UPDATE;
        this.sqlUPD = new StringBuilder();
        if (prepStatUPD != null) prepStatUPD.close();
        prepStatUPD = null;
        this.tableNames = tableName;
        this.whereUPD = null;
        return this;
    }
    
    public FacilDB delete(String tableName) throws SQLException {
        this.operation = SQLCommand.DELETE;
        this.sqlDEL = new StringBuilder();
        if (prepStatDEL != null) prepStatDEL.close();
        prepStatDEL = null;
        this.tableNames = tableName;
        this.whereDEL = null;
        return this;
    }
    
    public void operationSelect() {
        this.operation = SQLCommand.SELECT;
    }
    
    public void operationInsert() {
        this.operation = SQLCommand.INSERT;
    }
    
    public void operationUpdate() {
        this.operation = SQLCommand.UPDATE;
    }
    
    public void operationDelete() {
        this.operation = SQLCommand.DELETE;
    }
    
    public void operationCreate() {
        this.operation = SQLCommand.CREATE;
    }
    
    public FacilDB from(String from) {
        this.from = from;
        return this;
    }
    
    public FacilDB where(String where) {
        switch (this.operation) {
            case SELECT:
                this.whereSEL = where;
                break;
            case UPDATE:
                this.whereUPD = where;
                break;
            case DELETE:
                this.whereDEL = where;
                break;
        }
        return this;
    }
    
    public FacilDB orderBy(String orderBy) {
        this.orderBy = orderBy;
        return this;
    }
    
    public void execute() throws SQLException {
        
        int i = 1;
        try {
            switch (this.operation) {
                case INSERT:
                    if (this.sqlINS.length() == 0) {
                        prepareInsert();
                    }
                    if (prepStatINS == null) {
                        prepStatINS = conn.prepareStatement(this.sqlINS.toString());
                    }
                    for (Object value: this.queryParams) {
                        prepStatINS.setObject(i++, value);
                    }
                    prepStatINS.executeUpdate();
                    break;
                case UPDATE:    
                    if (this.sqlUPD.length() == 0) {
                        prepareUpdate();
                    }
                    if (prepStatUPD == null) {
                        prepStatUPD = conn.prepareStatement(this.sqlUPD.toString());
                    }
                    for (Object value: this.queryParams) {
                        prepStatUPD.setObject(i++, value);
                    }
                    prepStatUPD.executeUpdate();
                    break;
                case DELETE:
                    if (this.sqlDEL.length() == 0) {
                        prepareDelete();
                    }
                    if (prepStatDEL == null) {
                        prepStatDEL = conn.prepareStatement(this.sqlDEL.toString());
                    }
                    for (Object value: this.queryParams) {
                        prepStatDEL.setObject(i++, value);
                    }
                    prepStatDEL.executeUpdate();
                    break;
                case CREATE:
                    if (prepStatCRE == null) {
                        prepStatCRE = conn.prepareStatement(this.sqlCRE.toString());
                    }
                    prepStatCRE.execute();
                    break;
                case PROCEDURE:
                    for (Object value: this.queryParams) {
                        this.callableStatement.setObject(i++, value);
                    }
                    this.callableStatement.execute();
                    break;
            }
        } finally {
            this.queryParams = new ArrayList<>();
            if (prepStatINS != null) {
                prepStatINS.close();
                prepStatINS = null;
            }
            if (prepStatUPD != null) {
                prepStatUPD.close();
                prepStatUPD = null;
            }
            if (prepStatDEL != null) {
                prepStatDEL.close();
                prepStatDEL = null;
            }
        }
    }

    public void executeBatch(List<JSONObject> records) throws SQLException {

        try {
            if (this.sqlINS.length() == 0) {
                prepareInsert();
            }
            if (prepStatINS == null) {
                prepStatINS = conn.prepareStatement(this.sqlINS.toString());
            }
            for (JSONObject record: records) {
                for (int i=0; i<record.length(); i++) {
                    try {
                        prepStatINS.setObject(i + 1, record.get(fieldsINS[i]));
                    } catch (Exception e) {
                        throw new SQLException(e.getMessage() + "\r\n" + record.toString());
                    }
                }
                prepStatINS.addBatch();
            }
            prepStatINS.executeBatch();
        } finally {
            this.queryParams = new ArrayList<>();
            if (prepStatINS != null) {
                prepStatINS.close();
                prepStatINS = null;
            }
        }
    }

    public JSONArray query() throws SQLException {
        if (this.operation == SQLCommand.SELECT) {
            return querySelect();
        } else if (this.operation == SQLCommand.PROCEDURE) {
            return queryProcedure(false);
        } 
        throw new SQLException("Operacao invalida!: nao foi indicada SQL_SELECT ou SQL_PROCEDURE: " + this.operation);
    }
            
    private JSONArray querySelect() throws SQLException {

        JSONArray records = new JSONArray();
        
        // Prepara o statement
        if (this.sqlSEL.length() == 0) {
            prepareSelect();
            lastSQL = this.sqlSEL.toString();
        }
        if (prepStatSEL == null) {
            prepStatSEL = conn.prepareStatement(this.sqlSEL.toString());
        }
        // Seta os parametros
        int i = 1;
        for (Object value: this.queryParams) {
            prepStatSEL.setObject(i++, value);
        }
        ResultSet rs = null;
        // Executa a query
        try {
            validSqlAlias();
            rs = prepStatSEL.executeQuery();
            if (rs.isBeforeFirst()) {
                while (rs.next()) {
                    JSONObject jsObj = new JSONObject();
                    for(String alias: sqlAliases) {
                        Object value = rs.getObject(alias);
                        if (value == null) {
                            jsObj.put(alias, JSONObject.NULL);
                        } else if (value instanceof java.sql.Date) {
                            jsObj.put(alias, DateUtil.dateToString((Date)value, DateUtil.PATTERN_DATE_HOUR_MILISECONDS));
                        } else if (value instanceof java.sql.Timestamp) {  
                            jsObj.put(alias, DateUtil.dateToString((Date)value, DateUtil.PATTERN_DATE_HOUR_MILISECONDS));
                        } else if (value instanceof java.sql.Clob) {
                            jsObj.put(alias, rs.getString(alias));
                        } else {
                            jsObj.put(alias, value);	
                        }
                    }
                    records.put(jsObj);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (e != null && e.getMessage() != null && 
                !e.getMessage().startsWith(NO_DATA_AVAILABLE)) {
                throw e;
            }
        } finally {
            this.queryParams = new ArrayList<>();
            if (rs != null) {
                rs.close();
            }
            if (prepStatSEL != null) {
                prepStatSEL.close();
                prepStatSEL = null;
            }
        }
        return records;
    }
    
    public JSONArray queryProcedure(boolean complex) throws SQLException {

        JSONArray records = new JSONArray();
        
        // Seta os parametros
        int i = 1;
        for (Object value: this.queryParams) {
            callableStatement.setObject(i++, value);
        }
        
        ResultSet rs = null;
        
        // Executa a query
        try {
            validSqlAlias();
            
            if (complex) {
    
                int resultNum = 0;
                while (true) {
                    boolean queryResult;
                    int rowsAffected;
                    if (1 == ++resultNum) {
                        try {
                            queryResult = callableStatement.execute();
                        } catch (SQLException e) {
                            continue;
                        }
                    } else {
                        try {
                            queryResult = callableStatement.getMoreResults();
                        }
                        catch (SQLException e) {
                            continue;
                        }
                    }

                    if (queryResult) {
                        rs = callableStatement.getResultSet();
                        if (rs.isBeforeFirst()) {
                            while (rs.next()) {
                                JSONObject jsObj = new JSONObject();
                                for(String alias: sqlAliases) {
                                    Object value = rs.getObject(alias);
                                    if (value == null) {
                                        jsObj.put(alias, JSONObject.NULL);
                                    } else if (value instanceof java.sql.Date) {
                                        jsObj.put(alias, DateUtil.dateToString((Date)value, DateUtil.PATTERN_DATE_HOUR_MILISECONDS));
                                    } else if (value instanceof java.sql.Timestamp) {  
                                        jsObj.put(alias, DateUtil.dateToString((Date)value, DateUtil.PATTERN_DATE_HOUR_MILISECONDS));
                                    } else {
                                        jsObj.put(alias, value);	
                                    }
                                }
                                records.put(jsObj);
                            }
                        }
                        if (rs != null) {
                            rs.close();
                        }
                    } else {
                        rowsAffected = callableStatement.getUpdateCount();
                        if (-1 == rowsAffected) {
                            --resultNum;
                            break;
                        }
                    }
                }        
  
            } else {
                rs = callableStatement.executeQuery();
                if (rs.isBeforeFirst()) {
                    while (rs.next()) {
                        JSONObject jsObj = new JSONObject();
                        for(String alias: sqlAliases) {
                            Object value = rs.getObject(alias);
                            if (value == null) {
                                jsObj.put(alias, JSONObject.NULL);
                            } else if (value instanceof java.sql.Date) {
                                jsObj.put(alias, DateUtil.dateToString((Date)value, DateUtil.PATTERN_DATE_HOUR_MILISECONDS));
                            } else if (value instanceof java.sql.Timestamp) {  
                                jsObj.put(alias, DateUtil.dateToString((Date)value, DateUtil.PATTERN_DATE_HOUR_MILISECONDS));
                            } else {
                                jsObj.put(alias, value);	
                            }
                        }
                        records.put(jsObj);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (e != null && e.getMessage() != null &&
                !e.getMessage().startsWith(NO_DATA_AVAILABLE)) {
                throw e;
            }
        } finally {
            this.queryParams = new ArrayList<>();
            if (rs != null) {
                rs.close();
            }
            if (prepStatSEL != null) {
                prepStatSEL.close();
                prepStatSEL = null;
            }
        }
        return records;
    }
    
    public JSONObject queryUnique() throws SQLException {
        // Prepara o select
        if (this.sqlSEL.length() == 0) {
            prepareSelect();
            lastSQL =  this.sqlSEL.toString();
        }
        if (prepStatSEL == null) {
            prepStatSEL = conn.prepareStatement(this.sqlSEL.toString());
        }
        prepStatSEL = conn.prepareStatement(this.sqlSEL.toString());
        // Seta os parametros
        int i = 1;
        for (Object value: this.queryParams) {
            prepStatSEL.setObject(i++, value);
        }
        validSqlAlias();
        // Executa a query
        JSONObject jsObj = new JSONObject();
        ResultSet rs = null;
        try {
            rs = prepStatSEL.executeQuery();
            if (rs.isBeforeFirst()) {
                rs.next();
                for(String alias: sqlAliases) {
                    Object value = rs.getObject(alias);
                    if (value == null) {
                        jsObj.put(alias, JSONObject.NULL);
                    } else if (value instanceof java.sql.Date) {  
                        jsObj.put(alias, DateUtil.dateToString((Date)value, DateUtil.PATTERN_DATE_HOUR_MILISECONDS));
                    } else if (value instanceof java.sql.Timestamp) {  
                        jsObj.put(alias, DateUtil.dateToString((Date)value, DateUtil.PATTERN_DATE_HOUR_MILISECONDS));
                    } else {
                        jsObj.put(alias, value);	
                    }
                }
            }
            
        } catch (Exception e) {
            if (e.getMessage() == null) {
                throw e;
            }
            if (!e.getMessage().startsWith(NO_DATA_AVAILABLE) && 
                !e.getMessage().startsWith("The result set has no current row")) {
                throw e;
            }
        } finally {
            this.queryParams = new ArrayList<>();
            if (rs != null) {
                rs.close();
            }
            if (prepStatSEL != null) {
                prepStatSEL.close();
                prepStatSEL = null;
            }
        }
        return jsObj;
    }
    
    public long queryCount() throws SQLException {
        if (prepStatSEL == null) {
            prepStatSEL = conn.prepareStatement(this.sqlSEL.toString());
        }
        long count = 0;
        int i = 1;
        for (Object value: this.queryParams) {
            prepStatSEL.setObject(i++, value);
        }
        ResultSet rs = null;
        try {
            rs = prepStatSEL.executeQuery();
            if (rs.isBeforeFirst()) {
                while (rs.next()) {
                    count = rs.getLong(1);
                }
            }
        } finally {
            this.queryParams = new ArrayList<>();
            if (rs != null) {
                rs.close();
            }
            prepStatSEL.close();
            prepStatSEL = null;
        }
        return count;
    }
    
    public void setOutCommit(boolean bl) throws SQLException {
        if (conn != null) {
            this.conn.setAutoCommit(bl);
        }
    }
    
    public void beginTransaction() throws SQLException {
        this.setOutCommit(false);
    }
    
    public void rollback() throws SQLException {
        if (conn != null) {
            this.conn.rollback();
        }
    }
    
    public void commit() throws SQLException {
        if (conn != null) {
            this.conn.commit();
        }
    }
    
    public void commitTransaction() throws SQLException {
        this.commit();
        this.setOutCommit(true);
    }
    
    public String getSelect() throws SQLException {
        prepareSelect();
        return sqlSEL.toString();
    }
    
    public String getInsert() throws SQLException {
        prepareInsert();
        return sqlINS.toString();
    }
    
    public String getUpdate() throws SQLException {
        prepareUpdate();
        return sqlUPD.toString();
    }
    
    public String getDelete() throws SQLException {
        prepareDelete();
        return sqlDEL.toString();
    }
    
    ////////////////////////////////////////////////////////////////////////////
    // PRIVATE METHODS /////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////
    
    private void prepareSelect() throws SQLException {
        sqlSEL.append("\r\nselect ");
        if (maxResults > 0) {
            if (this.dbType == DataBaseType.H2 || 
                this.dbType == DataBaseType.MSSQL || 
                this.dbType == DataBaseType.MSSQL_JTDS || 
                this.dbType == DataBaseType.PERVASIVE_PSQL || 
                this.dbType == DataBaseType.PROGRESS) {
                sqlSEL.append("top ").append(String.valueOf(maxResults)).append(" ");
            } else if (this.dbType == DataBaseType.FIREBIRD) {
                sqlSEL.append("first ").append(String.valueOf(maxResults)).append(" ");
            }
        }
        validSqlAlias();
        if (fieldsSEL.length != sqlAliases.length) {
            throw new SQLException("Numero de campos informados diferente do numero de SQL Aliases");
        }
        int i=0;
        for (String field: fieldsSEL) {
            if (this.dbType == DataBaseType.POSTGRESQL) {
                sqlSEL.append(field).append(" as \"").append(sqlAliases[i++]).append("\",");
            } else {
                sqlSEL.append(field).append(" \"").append(sqlAliases[i++]).append("\",");
            }
        }
        sqlSEL.deleteCharAt(sqlSEL.length()-1);
        if (from != null && !from.isEmpty()) {
            sqlSEL.append("\r\n   from ");
            if (schema != null && !schema.isEmpty()) {
                sqlSEL.append("\"").append(schema).append("\".");
            }
            sqlSEL.append(from)
                  .append("\r\n");
        } else {
            throw new SQLException ("Nao foi informado o FROM para o SELECT.");
        }
        if (whereSEL != null && !whereSEL.isEmpty()) {
            String wh = whereSEL.toLowerCase().trim();
            if (!wh.startsWith("join") && 
                !wh.startsWith("inner") &&
                !wh.startsWith("group")) {
                sqlSEL.append("   where ");
            }
            sqlSEL.append(whereSEL);
        }
        if (maxResults > 0) {
            if (this.dbType == DataBaseType.ORACLE) {
                if (whereSEL == null || whereSEL.isEmpty()) {
                    sqlSEL.append("   where rownum <= ").append(String.valueOf(maxResults));
                } else {
                    sqlSEL.append(" and rownum <= ").append(String.valueOf(maxResults));
                }
            } 
        }
        sqlSEL.append("\r\n");
        if (!orderBy.isEmpty()) {
            sqlSEL.append("   order by ")
                  .append(orderBy)
                  .append("\r\n");
        }
        if (maxResults > 0 &&
            this.dbType == DataBaseType.MYSQL || 
            this.dbType == DataBaseType.POSTGRESQL) {
            sqlSEL.append("    limit ").append(String.valueOf(maxResults)).append("\r\n"); 
        }
        if (this.dbType == DataBaseType.PROGRESS) {
            sqlSEL.append("    with (nolock)\r\n");
        }
    }
    
    private void prepareInsert() throws SQLException {
        sqlINS.append("\r\ninsert into ");
        if (schema != null && !schema.isEmpty()) {
            sqlINS.append("\"").append(schema).append("\".");
        }
        if (tableNames != null && !tableNames.isEmpty()) {
            sqlINS.append(tableNames);
        } else {
            throw new SQLException (TABLE_NAME_NOT_INFORMED);
        }
        sqlINS.append("\r\n   (");
        for (String field: fieldsINS) {
            int p = field.indexOf(".");
            if (p > 0) {
                field = field.substring(p+1);
            }
            sqlINS.append(field).append(",");
        }
        sqlINS.deleteCharAt(sqlINS.length()-1);
        sqlINS.append(")");
        sqlINS.append("\r\n   values (");
        for (int i=0; i<fieldsINS.length; i++) {
            sqlINS.append("?,");
        }
        sqlINS.deleteCharAt(sqlINS.length()-1);
        sqlINS.append(")");
        lastSQL = sqlINS.toString();
    }
    
    private void prepareUpdate() throws SQLException {
        sqlUPD.append("\r\nupdate ");
        if (schema != null && !schema.isEmpty()) {
            sqlUPD.append("\"").append(schema).append("\".");
        }
        if (tableNames != null && !tableNames.isEmpty()) {
            sqlUPD.append(tableNames);
        } else {
            throw new SQLException (TABLE_NAME_NOT_INFORMED);
        }
        sqlUPD.append("\r\n   set ");
        for (String field: fieldsUPD) {
            int p = field.indexOf(".");
            if (p > 0) {
                field = field.substring(p+1);
            }
            sqlUPD.append(field).append("=?, ");
        }
        sqlUPD.deleteCharAt(sqlUPD.length()-1);
        sqlUPD.deleteCharAt(sqlUPD.length()-1);
        sqlUPD.append("\r\n");
        if (!whereUPD.isEmpty()) {
            sqlUPD.append("   where ");
            sqlUPD.append(whereUPD);
            sqlUPD.append("\r\n");
        }
        lastSQL = sqlUPD.toString();
    }
    
    private void prepareDelete() throws SQLException {
        sqlDEL.append("delete from ");
        if (schema != null && !schema.isEmpty()) {
            sqlDEL.append("\"").append(schema).append("\".");
        }
        if (tableNames != null && !tableNames.isEmpty()) {
            sqlDEL.append(tableNames);
        } else {
            throw new SQLException (TABLE_NAME_NOT_INFORMED);
        }
        if (!whereDEL.isEmpty()) {
            sqlDEL.append("\r\n   where ").append(whereDEL);
        }
        lastSQL = sqlDEL.toString();
    }

    private void validSqlAlias() {
        if (sqlAliases == null) {
            sqlAliases = new String[fieldsSEL.length];
            int m = 0;
            for (String name: fieldsSEL) {
                if (this.dbType == DataBaseType.PERVASIVE_PSQL) {
                    name = name.replace("\\.", "_");
                }
                sqlAliases[m++] = name;
            }
        }    
    }
    
}
