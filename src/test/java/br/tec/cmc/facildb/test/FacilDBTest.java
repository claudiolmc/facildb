package br.tec.cmc.facildb.test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.h2.tools.Server;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import br.tec.cmc.facildb.FacilDB;
import br.tec.cmc.facildb.FacilH2;
import br.tec.cmc.facildb.metadata.Column;
import br.tec.cmc.facildb.metadata.MetadataDB;
import br.tec.cmc.facildb.metadata.Table;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FacilDBTest {
    
    private static FacilDB db = null;
    private static MetadataDB metaDb = null;

    @BeforeAll
    static void setup() throws SQLException, IOException {
        // Delete H2 database file
        String dbDir = System.getProperty("java.io.tmpdir");
        File file = new File(dbDir + "/testdb.mv.db");
        if (file.exists()) {
            Files.delete(Paths.get(dbDir + "/testdb.mv.db"));
        }

        // Start H2 Server
        Server.createTcpServer("-tcpPort", "9092", "-tcpAllowOthers", "-ifNotExists").start();

        // Open connection
        db = new FacilH2("localhost", "9092", dbDir, "testdb", "root", "1234");

        // Get Metadata
        metaDb = new MetadataDB(db.getConnection().getMetaData());
    }

    @Test
    @Order(1)
    void createTablesTest() throws SQLException {
        // Create Publisher Table
        db.sql(sqlCreatePublisherTable()).execute();
        long total = db.sql("select count(*) from publisher").queryCount();
        Assertions.assertEquals(0, total);

        // Create Book Table
        db.sql(sqlCreateBookTable()).execute();
        total = db.sql("select count(*) from book").queryCount();
        Assertions.assertEquals(0, total);
    }

    @Test
    @Order(2)
    void insertTest() throws SQLException {

        insertPublisher(1000L, "Wiley");
        insertPublisher(1001L, "Addison-Wesley");
        insertPublisher(1002L, "Acme Books");

        insertBook(2000L, "Linux Bible", "Christopher Negus", "978-1119578888", 1000L);
        insertBook(2001L, "Effective Java 3rd Edition", "Joshua Bloch", "978-0134685991", 1001L);
        insertBook(2002L, "Refactoring: Improving the Design of Existing Code (2nd Edition) ", "Martin Fowler", "978-0134757599", 1001L);
        insertBook(2003L, "TCP/IP Illustrated, Volume 1: The Protocols", "Kevin Fall and W. Stevens", "978-0321336316", 1001L);
        insertBook(2004L, "Drawing Cartoons", "John Silver", "965-33245667", 1002L);

        long totalPub = db.sql("select count(*) from publisher").queryCount();
        long totalBook = db.sql("select count(*) from book").queryCount();
        
        Assertions.assertEquals(3, totalPub);
        Assertions.assertEquals(5, totalBook);
    }

    @Test
    @Order(3)
    void updateTest() throws SQLException {

        String newTitle = "Drawing Cartoons the Easy Way";

        db.update("book")
          .fields("title")
          .where("id=?")
          .param(newTitle)
          .param(2004L)
          .execute();

        JSONObject record = db.select("id, isbn, title, author")
                              .from("book")
                              .where("id=?")
                              .param(2004L)
                              .queryUnique();

        Assertions.assertEquals(newTitle, record.optString("title"));        
    }

    @Test
    @Order(4)
    void selectTest() throws SQLException {

        JSONArray list = db.select("title, author, isbn")
                           .from("book")
                           .where("publisher_id=?")
                           .param(1001L)
                           .query();

        Assertions.assertEquals(3, list.length());
        Assertions.assertEquals("Martin Fowler", list.optJSONObject(1).optString("author"));        
    }

    @Test
    @Order(5)
    void deleteTest() throws SQLException {

        db.delete("book")
          .where("id=?")
          .param(2004L)
          .execute();

        long totalBook = db.sql("select count(*) from book").queryCount();
        
        Assertions.assertEquals(4, totalBook);    
    }

    @Test
    @Order(6)
    void joinTest() throws SQLException {

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

        Assertions.assertEquals("Martin Fowler", records.optJSONObject(0).optString("author"));    
    }

    @Test
    @Order(7)
    void limitTest() throws SQLException {

        JSONArray records  = db.select("id, title")
                               .from("book")
                               .where("publisher_id=?")
                               .param(1001L)
                               .maxResults(2)
                               .query();

        Assertions.assertEquals(2, records.length());    
    }

    @Test
    @Order(8)
    void batchInsertTest() throws SQLException {

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

        long totalPub = db.sql("select count(*) from publisher").queryCount();

        Assertions.assertEquals(6, totalPub);    
    }

    @Test
    @Order(9)
    void transactionTest() throws SQLException {

        db.beginTransaction();
        insertPublisher(6000L, "ALL BOOKS");
        insertPublisher(6001L, "ONLY BOOKS");
        db.commitTransaction();

        long totalPub = db.sql("select count(*) from publisher").queryCount();

        Assertions.assertEquals(8, totalPub); 
    }

    @Test
    @Order(10)
    void rollbackTest() throws SQLException {

        db.beginTransaction();
        insertPublisher(6003L, "STAR BOOKS");
        insertPublisher(6004L, "SUN BOOKS");
        db.rollback(); 

        long totalPub = db.sql("select count(*) from publisher").queryCount();

        Assertions.assertEquals(8, totalPub); 
    }

    @Test
    @Order(11)
    void getLastSQLTest() throws SQLException {

        JSONArray records = db.select("id, title, author")
                              .from("book")
                              .where("id > 2000")
                              .query();

        String lastSQL = db.getLastSQL();
        Assertions.assertTrue(lastSQL.contains("where id > 2000")); 
    }

    @Test
    @Order(12)
    void matadataGetCatalogsTest() throws SQLException {

        List<String> catalogs = metaDb.getCatalogs();
        Assertions.assertEquals("TESTDB", catalogs.get(0)); 
    }

    @Test
    @Order(13)
    void matadataGetSchemasTest() throws SQLException {

        List<String> schemas = metaDb.getSchemas();
        Assertions.assertEquals("PUBLIC", schemas.get(1)); 
    }

    @Test
    @Order(14)
    void matadataGetTablesTest() throws SQLException {

        List<String> tables = metaDb.getTableList("", "PUBLIC");
        Assertions.assertEquals(2, tables.size());
        Assertions.assertEquals("PUBLISHER", tables.get(1)); 
    }

    @Test
    @Order(15)
    void matadataGetTableTest() throws SQLException {

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
    }

    @Test
    @Order(16)
    void matadataPrimaryKeysTest() throws SQLException {
        List<String> pks = metaDb.getPrimaryKeys("", "PUBLIC", "BOOK");
        Assertions.assertEquals("ID", pks.get(0)); 
    }

    @Test
    @Order(17)
    void matadataIndexedColumnsTest() throws SQLException {
        List<String> idxs = metaDb.getIndexedColumns("", "PUBLIC", "BOOK");
        Assertions.assertEquals("ID", idxs.get(0)); 
    }

    @AfterAll
    static void finish() throws SQLException {
        db.closeConnection();
    }

    private String sqlCreatePublisherTable() {
        StringBuilder sb = new StringBuilder();
        sb.append("create table publisher (");
        sb.append("   id bigint not null,");
        sb.append("   pub_name varchar(80) not null,");
        sb.append("   primary key (id)");
        sb.append(")");
        return sb.toString();
    }

    private String sqlCreateBookTable() {
        StringBuilder sb = new StringBuilder();
        sb.append("create table book (");
        sb.append("   id bigint not null,");
        sb.append("   title varchar(80) not null,");
        sb.append("   author varchar(50) not null,");
        sb.append("   isbn varchar(15) not null,");
        sb.append("   publisher_id  bigint not null,");
        sb.append("   primary key (id),");
        sb.append("   foreign key (publisher_id) references publisher(id)");
        sb.append(")");
        return sb.toString();
    }

    private void insertPublisher(Long id, String pubName) throws SQLException {
        db.insert("publisher")
          .fields("id, pub_name")
          .param(id)
          .param(pubName)
          .execute();
    }

    private void insertBook(Long id, String title, String author, String isbn, Long pubId) throws SQLException {
        db.insert("book")
          .fields("id, title, author, isbn, publisher_id")
          .param(id)
          .param(title)
          .param(author)
          .param(isbn)
          .param(pubId)
          .execute();
    }
}
