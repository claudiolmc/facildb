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

import java.sql.SQLException;

public class FacilOracle extends FacilDB {
    
    public FacilOracle(String dbServer, String dbPort, String dbServiceName, 
                    String dbUser, String dbPassword) throws SQLException {
        
        this.dbType = DataBaseType.ORACLE;
        
        StringBuilder uri = new StringBuilder();
        uri.append("jdbc:oracle:thin:@//")
           .append(dbServer)
           .append(":")
           .append(dbPort)
           .append("/")
           .append(dbServiceName);

        this.setConnection(uri.toString(), dbUser, dbPassword);
    }
}
