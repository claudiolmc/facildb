# FacilDB
Easy and fluent lib to use SQL databases in Java applications. Represents result sets and records as JSONArrays and JSONObjects.

## Supports

- PostgreSQL
- MySQL
- H2
- Oracle
- Microsoft SQLServer
- Firebird
- Progress
- PervarsivePSQL

## Dependencies

```xml
<dependency>
    <groupId>org.json</groupId>
    <artifactId>json</artifactId>
    <version>20200518</version>
</dependency>

Also you need the JDBC driver for your database. Examples:

For **PostgreSQL**:

```xml
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <version>42.2.18</version>
</dependency>
```
For **MySQL**:

```xml
<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>8.0.22</version>
</dependency>
```
For **H2**:

```xml
<dependency>
    <groupId>com.h2database</groupId>
    <artifactId>h2</artifactId>
    <version>1.4.200</version>
    <scope>test</scope>
</dependency>
```
**You can use the latest version of each dependency**.

## Connecting to a Database

### PostgreSQL
```java
// Server, port, database name, username, password
FacilDB db = new FacilPostgreSQL("localhost", "5432", "testdb", "user", "1234");
```

### MySQL
```java
// Server, port, database name, username, password
FacilDB db = new FacilMySQL("localhost", "3306", "testdb", "user", "1234");
```

### H2 (Server Mode)
```java
// Server, port, database dir, database name, username, password
FacilDB db = new FacilH2("localhost", "9092", "/opt/database", "testdb", "user", "1234");
```

### Oracle
```java
// Server, port, database name or SID, username, password
FacilDB db = new FacilOracle("localhost", "1521", "testdb", "user", "1234");
```

### SQL Server with JTDS
```java
// Server, port, instanceName (opcional), database name, username, password
FacilDB db = new FacilSQLServerJTDS("localhost", "1521", "instanceName", "testdb", "user", "1234");
```
If you want to use a **Connection Pool**, you can do:

```java
FacilDB db = new FacilDB();
db.setConnection(connectionFromPool);
```
You can also connect using the JDBC URI:

```java
FacilDB db = new FacilDB("jdbc:postgresql://localhost/testdb", "user", "1234");
```
## Queries and CRUD Operations

Supose a database with these tables:

```sql
create table publisher (
   id bigint not null,
   pub_name varchar(80) not null,
   primary key (id)
);

create table book (
   id bigint not null,
   title varchar(80) not null,
   author varchar(50) not null,
   isbn varchar(15) not null,
   publisher_id  bigint not null,
   primary key (id),
   foreign key (publisher_id) references publisher(id)
)
```

### Simple SELECT
```java
JSONArray list = db.select("title, author, isbn")
                   .from("book")
                   .where("publisher_id=?")
                   .param(1001L)
                   .query();
```

Sample result:

```json
[
   {
      "author": "Joshua Bloch",
      "isbn": "978-0134685991",
      "title": "Effective Java 3rd Edition"
   },
   {
      "author": "Martin Fowler",
      "isbn": "978-0134757599",
      "title": "Refactoring: Improving the Design of Existing Code (2nd Edition) "
   },
   {
      "author": "Kevin Fall and W. Stevens",
      "isbn": "978-0321336316",
      "title": "TCP/IP Illustrated, Volume 1: The Protocols"
   }
]
```

### Single Result
```java
JSONObject record = db.select("id, isbn, title, author")
                      .from("book")
                      .where("id=?")
                      .param(2001L)
                      .queryUnique();
```

Sample result:

```json
{
   "author": "Joshua Bloch",
   "isbn": "978-0134685991",
   "id": 2001,
   "title": "Effective Java 3rd Edition"
}
```
### INSERT
```java
db.insert("book")
  .fields("id, title, author, isbn, publisher_id")
  .param(id)
  .param(title)
  .param(author)
  .param(isbn)
  .param(pubId)
  .execute();
```

### UPDATE
```java
db.update("book")
  .fields("title, isbn")
  .where("id=?")
  .param(newTitle)
  .param(newISBN)
  .param(2004L)
  .execute();
```
### DELETE
```java
db.delete("book")
  .where("id=?")
  .param(2004L)
  .execute();
```

### BATCH INSERT
```java
List<JSONObject> list = new ArrayList<>();

JSONObject rec1 = new JSONObject();
rec1.put("id", 5000L);
rec1.put("pub_name", "Publisher ABC");
list.add(rec1);

JSONObject rec2 = new JSONObject();
rec2.put("id", 5001L);
rec2.put("pub_name", "Publisher XYZ");
list.add(rec2);

JSONObject rec3 = new JSONObject();
rec3.put("id", 5002L);
rec3.put("pub_name", "Publisher WWW");
list.add(rec3);

db.insert("publisher")
  .fields("id, pub_name")
  .executeBatch(list);
```

### SELECT COUNT
```java
long totalBooks = db.sql("select count(*) from book").queryCount();
```

### SELECT JOIN
```java
StringBuilder sb = new StringBuilder();
sb.append("select bk.title as title, bk.author as author, pb.pub_name as publisher");
sb.append("   from book bk");
sb.append("   join publisher pb on bk.publisher_id = pb.id");
sb.append("   where bk.publisher_id = ?");
sb.append("   order by bk.author desc");

JSONArray records  = db.sql(sb.toString())
                       .sqlAlias("title, author, publisher")
                       .param(1001L)
                       .query();
```

### LIMIT AND ORDER BY
```java
JSONArray list = db.select("title, author, isbn")
                   .from("book")
                   .where("publisher_id=?")
                   .orderBy("author, title")
                   .param(1001L)
                   .maxResults(50)
                   .query();
```

### TRANSACTION
```java
db.beginTransaction();
insertPublisher(6000L, "ALL BOOKS");
insertPublisher(6001L, "ONLY BOOKS");
db.commitTransaction();
```

### ROLLBACK
```java
db.beginTransaction();
insertPublisher(6000L, "ALL BOOKS");
insertPublisher(6001L, "ONLY BOOKS");
db.rollback();
```
### SHOW SQL STATEMENTS
```java        
JSONArray list = db.select("title, author, isbn")
                    .from("book")
                    .where("publisher_id=?")
                    .param(1001L)
                    .query();
                    
String lastSQL = db.getLastSQL();
```

### CREATE TABLE
```java
StringBuilder sb = new StringBuilder();
sb.append("create table publisher (");
sb.append("   id bigint not null,");
sb.append("   pub_name varchar(80) not null,");
sb.append("   primary key (id)");
sb.append(")");

db.sql(sb.toString()).execute();
```
## Metadata: Reading Database and Table Information

### GET CATALOGS
```java
MetadataDB metaDb = new MetadataDB(db.getConnection().getMetaData());
List<String> catalogs = metaDb.getCatalogs();
for (String catalog: catalogs) {
    System.out.println(catalog);
}
```

### GET SCHEMAS
```java
MetadataDB metaDb = new MetadataDB(db.getConnection().getMetaData());
List<String> schemas = metaDb.getSchemas();
for (String schema: schemas) {
    System.out.println(schema);
}
```

### GET TABLES
```java
MetadataDB metaDb = new MetadataDB(db.getConnection().getMetaData());
List<String> tables = metaDb.getTableList("", "PUBLIC");
for (String tableName: tables) {
    System.out.println(tableName);
}
```

### TABLE AND COLUMNS
```java
MetadataDB metaDb = new MetadataDB(db.getConnection().getMetaData());
Table table = metaDb.getTable("", "PUBLIC", "BOOK");
List<Column> columns = table.getColumns();

Column colID = columns.get(0);
Assertions.assertEquals("ID", colID.getColumnName());
Assertions.assertEquals(19, colID.getColumnSize());
Assertions.assertEquals("Long", colID.getColumnType());
Assertions.assertTrue(colID.isPrimaryKey());
Assertions.assertTrue(colID.isIndexed());
Assertions.assertFalse(colID.isNullable());

Column colISBN = columns.get(3);
Assertions.assertEquals("ISBN", colISBN.getColumnName());
Assertions.assertEquals(15, colISBN.getColumnSize());
Assertions.assertEquals("String", colISBN.getColumnType());
Assertions.assertFalse(colISBN.isPrimaryKey());
Assertions.assertFalse(colISBN.isIndexed());
Assertions.assertFalse(colISBN.isNullable());
```
### PRIMARY KEYS
```java
MetadataDB metaDb = new MetadataDB(db.getConnection().getMetaData());
List<String> pks = metaDb.getPrimaryKeys("", "PUBLIC", "BOOK");
for (String pk: pks) {
    System.out.println(pk);
}
```
### INDEXED COLUMNS
```java
MetadataDB metaDb = new MetadataDB(db.getConnection().getMetaData());
List<String> idxs = metaDb.getIndexedColumns("", "PUBLIC", "BOOK");
for (String idx: idxs) {
    System.out.println(idx);
}
```
## *********************************************
## AFTER ALL DON'T FORGET TO CLOSE THE CONNECTION
```java
db.closeConnection();
```
## *********************************************

Do you like this lib? Have questions? Any suggestions? Any bug?

Send me a message: claudio.montenegro@gmail.com

## Thanks!