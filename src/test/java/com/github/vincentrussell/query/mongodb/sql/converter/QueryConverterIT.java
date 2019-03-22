package com.github.vincentrussell.query.mongodb.sql.converter;

import com.alibaba.fastjson.JSON;
import com.github.vincentrussell.query.mongodb.sql.converter.jdbc.MongodbConnection;
import com.github.vincentrussell.query.mongodb.sql.converter.jdbc.MongodbDataSource;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import org.apache.commons.io.IOUtils;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.net.ServerSocket;
import java.sql.SQLException;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class QueryConverterIT {

    private static final int TOTAL_TEST_RECORDS = 25359;
    private static MongodStarter starter = MongodStarter.getDefaultInstance();
    private static MongodProcess mongodProcess;
    private static MongodExecutable mongodExecutable;
    private static int port = getRandomFreePort();
    private static com.mongodb.client.MongoClient mongoClient;
    private static final String DATABASE = "local";
    private static String COLLECTION = "my_collection";
    private static MongoDatabase mongoDatabase;
    private static MongoCollection mongoCollection;
    private static JsonWriterSettings jsonWriterSettings = new JsonWriterSettings(JsonMode.STRICT, "\t", "\n");

    private static MongodbConnection connection;

    //    @BeforeClass
    public static void beforeClass() throws IOException {
        IMongodConfig mongodConfig = new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(new Net("localhost", port, false))
                .build();

        mongodExecutable = starter.prepare(mongodConfig);
        mongodProcess = mongodExecutable.start();
//        mongoClient = new MongoClient("localhost",port);


        mongoDatabase = mongoClient.getDatabase(DATABASE);
        mongoCollection = mongoDatabase.getCollection(COLLECTION);

        List<Document> documents = new ArrayList<>(TOTAL_TEST_RECORDS);
        try (InputStream inputStream = QueryConverterIT.class.getResourceAsStream("/primer-dataset.json");
             BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {

            String line;
            while ((line = bufferedReader.readLine()) != null) {
                documents.add(Document.parse(line));
            }
        }

        for (Iterator<List<WriteModel>> iterator = Iterables.partition(Lists.transform(documents, new Function<Document, WriteModel>() {
            @Override
            public WriteModel apply(Document document) {
                return new InsertOneModel(document);
            }
        }), 10000).iterator(); iterator.hasNext(); ) {
            mongoCollection.bulkWrite(iterator.next());
        }

        assertEquals(TOTAL_TEST_RECORDS, mongoCollection.count());

    }

    //    @BeforeClass
    public static void start() {
        StringBuilder builder = new StringBuilder();
        builder.append("mongodb://").append("program").append(":").append("piV5LDa44vGAwSq8")
                .append("@").append("172.18.130.50:27017")
                .append("/").append("installment").append("?")
                .append("maxPoolSize=").append(10)
                .append("&").append("waitQueueMultiple=").append(100)
                .append("&").append("safe=").append("true")
                .append("&").append("connectTimeoutMS=").append("10000")
                .append("&").append("waitQueueTimeoutMS=").append("10000")
                .append("&").append("serverSelectionTimeoutMS=").append("30000")
                .append("&").append("authMechanism=").append("SCRAM-SHA-1");

        mongoClient = MongoClients.create(builder.toString());
        COLLECTION = "rpc_logs";
        mongoDatabase = mongoClient.getDatabase("installment");

    }

    @BeforeClass
    public static void startDatasource() throws SQLException {
        MongodbDataSource dataSource = new MongodbDataSource();
        dataSource.setUrl("mongodb://172.18.130.50:27017");
        dataSource.setUsername("program");
        dataSource.setPassword("piV5LDa44vGAwSq8");
        dataSource.setDatabase("installment");
        dataSource.setMaxPoolSize(10);
        dataSource.setWaitQueueMultiple(100);
        dataSource.setSafe("true");
        dataSource.setConnectTimeout(10000);
        dataSource.setServerSelectionTimeout(30000);
        dataSource.setReadPreference("secondaryPreferred");
        dataSource.setAuthMechanism("SCRAM-SHA-1");
        connection = (MongodbConnection) dataSource.getConnection();
    }

    @AfterClass
    public static void afterClass() {
        mongoClient.close();
//        mongodProcess.stop();
//        mongodExecutable.stop();
    }

    @Test
    public void testSelect() throws ParseException {
        String sql = "select * from rpc_logs where  create_at>='2019-03-19 0:00:00' and date(create_at)<'2019-03-20 0:00:00' " +
                "and (update_at > '2018-1-1' or update_at < '2020-1-1' )"
                + " order by create_at desc limit 5,2";
//        sql = "select * from rpc_logs where  date(create_at)>='2019-03-22 0:00:00'  order by create_at desc";
        List list = ResultUtils.exec(mongoDatabase, sql);
        System.out.println(list);
    }

    @Test
    public void testDate() {
        String val = "2018-3-4 0:00:00.434a";
        boolean ret = val.matches("^\\d{4}-\\d{1,2}-\\d{1,2}(\\s\\d{1,2}:\\d{1,2}:\\d{1,2}(.\\d{1,5})?)?$");
        System.out.println(ret);
    }

    @Test
    public void testCount() throws ParseException {
        String sql = "select count(*) from rpc_logs where  date(create_at)>='2019-03-19 0:00:00' and date(create_at)<'2019-03-20 0:00:00' order by create_at desc ";
        sql = "select count(*) from rpc_logs where  create_at>='2019-03-19 0:00:00' and create_at<'2019-03-20 0:00:00' order by create_at desc ";
        Object list = ResultUtils.exec(connection, sql);
        System.out.println(list);
    }

    @Test
    public void testDistinct() throws ParseException {
        String sql = "select distinct data_id  from rpc_logs where  date(create_at)>='2019-03-19 0:00:00' and date(create_at)<'2019-03-20 0:00:00' order by create_at desc ";
        Object list = ResultUtils.exec(mongoDatabase, sql);
        System.out.println(list);
    }

    @Test
    public void testGroupBY() throws ParseException {
        String sql = "select url,status,count(*)  from rpc_logs where  date(create_at)>='2019-03-19 0:00:00' and date(create_at)<'2019-03-20 0:00:00' group by url,status ";
        Object list = ResultUtils.exec(mongoDatabase, sql);
        System.out.println(list);
    }

    @Test
    public void testGroupBY1() throws ParseException {
        String sql = "select status,count(*) as dd  from rpc_logs where  date(create_at)>='2019-03-19 0:00:00' and date(create_at)<'2019-03-20 0:00:00' group by status ";
        Object list = ResultUtils.exec(mongoDatabase, sql);
        System.out.println(list);
    }

    @Test
    public void testDelete() throws ParseException {
        String sql = "delete from rpc_logs where  trace_id='wAPshdPJEZPqMv3fHHe' ";
        Object list = ResultUtils.exec(mongoDatabase, sql);
        System.out.println(list);
    }

    @Test
    public void testInsert() throws ParseException {
        MongoCollection mongoCollection = mongoDatabase.getCollection("rpc_logs");
        Map val = new HashMap() {{
            put("data_id", 11);
            put("trace_id", "mytraceid");
            put("status", "ok");
            put("request_data", new HashMap() {{
                put("a", "aaa");
            }});
            put("create_at", "2019-3-22 12:07:03");
        }};
        mongoCollection.insertOne(new Document(val));
        String sql = "insert into rpc_logs(data_id,trace_id,status,create_at,update_at,request_data) values(11,'2222222','ok',now(),'2019-03-22 12:07:03.000','" + JSON.toJSONString(val) + "')";
        Object list = ResultUtils.exec(mongoDatabase, sql);
        System.out.println(list);
    }

    @Test
    public void testUpdate() throws ParseException {
        MongoCollection mongoCollection = mongoDatabase.getCollection("rpc_logs");
        Map val = new HashMap() {{
            put("data_id", 123);
            put("status", "ok");
            put("request_data", new HashMap() {{
                put("b", "bbb");
            }});

        }};
        Map filter = new HashMap() {{
            put("trace_id", "mytraceid");
        }};
//        Object ret= mongoCollection.updateOne(new Document(filter),new Document("$set",new Document(val)));
        String sql = "update rpc_logs set data_id=234,status='no',update_at='2019-3-23 12:07:03' where trace_id='mytraceid'";
        Object list = ResultUtils.exec(mongoDatabase, sql);
        System.out.println(list);
    }

    @Test
    public void likeQuery() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select * from " + COLLECTION + " where address.street LIKE '%Street'");
        QueryResultIterator<Document> findIterable = queryConverter.run(mongoDatabase);
        List<Document> documents = Lists.newArrayList(findIterable);
        assertEquals(7499, documents.size());
        Document firstDocument = documents.get(0);
        firstDocument.remove("_id");
        assertEquals("{\n" +
                "\t\"address\" : {\n" +
                "\t\t\"building\" : \"351\",\n" +
                "\t\t\"coord\" : [-73.98513559999999, 40.7676919],\n" +
                "\t\t\"street\" : \"West   57 Street\",\n" +
                "\t\t\"zipcode\" : \"10019\"\n" +
                "\t},\n" +
                "\t\"borough\" : \"Manhattan\",\n" +
                "\t\"cuisine\" : \"Irish\",\n" +
                "\t\"grades\" : [{\n" +
                "\t\t\t\"date\" : {\n" +
                "\t\t\t\t\"$date\" : 1409961600000\n" +
                "\t\t\t},\n" +
                "\t\t\t\"grade\" : \"A\",\n" +
                "\t\t\t\"score\" : 2\n" +
                "\t\t}, {\n" +
                "\t\t\t\"date\" : {\n" +
                "\t\t\t\t\"$date\" : 1374451200000\n" +
                "\t\t\t},\n" +
                "\t\t\t\"grade\" : \"A\",\n" +
                "\t\t\t\"score\" : 11\n" +
                "\t\t}, {\n" +
                "\t\t\t\"date\" : {\n" +
                "\t\t\t\t\"$date\" : 1343692800000\n" +
                "\t\t\t},\n" +
                "\t\t\t\"grade\" : \"A\",\n" +
                "\t\t\t\"score\" : 12\n" +
                "\t\t}, {\n" +
                "\t\t\t\"date\" : {\n" +
                "\t\t\t\t\"$date\" : 1325116800000\n" +
                "\t\t\t},\n" +
                "\t\t\t\"grade\" : \"A\",\n" +
                "\t\t\t\"score\" : 12\n" +
                "\t\t}],\n" +
                "\t\"name\" : \"Dj Reynolds Pub And Restaurant\",\n" +
                "\t\"restaurant_id\" : \"30191841\"\n" +
                "}", firstDocument.toJson(jsonWriterSettings));
    }

    @Test
    public void objectIdQuery() throws ParseException {
        mongoCollection.insertOne(new Document("_id", new ObjectId("54651022bffebc03098b4567")).append("key", "value1"));
        mongoCollection.insertOne(new Document("_id", new ObjectId("54651022bffebc03098b4568")).append("key", "value2"));
        try {
            QueryConverter queryConverter = new QueryConverter("select _id from " + COLLECTION
                    + " where ObjectId('_id') = '54651022bffebc03098b4567'");
            QueryResultIterator<Document> findIterable = queryConverter.run(mongoDatabase);
            List<Document> documents = Lists.newArrayList(findIterable);
            assertEquals(1, documents.size());
            assertEquals("{\n" + "\t\"_id\" : {\n" + "\t\t\"$oid\" : \"54651022bffebc03098b4567\"\n"
                    + "\t}\n" + "}", documents.get(0).toJson(jsonWriterSettings));
        } finally {
            mongoCollection.deleteOne(new Document("_id", new ObjectId("54651022bffebc03098b4567")));
            mongoCollection.deleteOne(new Document("_id", new ObjectId("54651022bffebc03098b4568")));
        }
    }

    @Test
    public void objectIdInQuery() throws ParseException {
        mongoCollection.insertOne(new Document("_id", new ObjectId("54651022bffebc03098b4567")).append("key", "value1"));
        mongoCollection.insertOne(new Document("_id", new ObjectId("54651022bffebc03098b4568")).append("key", "value2"));
        try {
            QueryConverter queryConverter = new QueryConverter("select _id from " + COLLECTION
                    + " where ObjectId('_id') IN ('54651022bffebc03098b4567','54651022bffebc03098b4568')");
            QueryResultIterator<Document> findIterable = queryConverter.run(mongoDatabase);
            List<Document> documents = Lists.newArrayList(findIterable);
            assertEquals(2, documents.size());
            assertEquals("{\n" + "\t\"_id\" : {\n" + "\t\t\"$oid\" : \"54651022bffebc03098b4567\"\n"
                    + "\t}\n" + "}", documents.get(0).toJson(jsonWriterSettings));
            assertEquals("{\n" + "\t\"_id\" : {\n" + "\t\t\"$oid\" : \"54651022bffebc03098b4568\"\n"
                    + "\t}\n" + "}", documents.get(1).toJson(jsonWriterSettings));
        } finally {
            mongoCollection.deleteOne(new Document("_id", new ObjectId("54651022bffebc03098b4567")));
            mongoCollection.deleteOne(new Document("_id", new ObjectId("54651022bffebc03098b4568")));
        }
    }

    @Test
    public void likeQueryWithProjection() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select address.building, address.coord from " + COLLECTION + " where address.street LIKE '%Street'");
        QueryResultIterator<Document> findIterable = queryConverter.run(mongoDatabase);
        List<Document> documents = Lists.newArrayList(findIterable);
        assertEquals(7499, documents.size());
        assertEquals("{\n" +
                "\t\"address\" : {\n" +
                "\t\t\"building\" : \"351\",\n" +
                "\t\t\"coord\" : [-73.98513559999999, 40.7676919]\n" +
                "\t}\n" +
                "}", documents.get(0).toJson(jsonWriterSettings));
    }

    @Test
    public void distinctQuery() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select distinct borough from " + COLLECTION + " where address.street LIKE '%Street'");
        QueryResultIterator<String> distinctIterable = queryConverter.run(mongoDatabase);
        List<String> results = Lists.newArrayList(distinctIterable);
        assertEquals(5, results.size());
        assertEquals(Arrays.asList("Manhattan", "Queens", "Brooklyn", "Bronx", "Staten Island"), results);
    }

    @Test
    public void countGroupByQuery() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select borough, count(borough) from " + COLLECTION + " GROUP BY borough");
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(6, results.size());
        assertEquals("[{\n" +
                "\t\"_id\" : \"Missing\",\n" +
                "\t\"count\" : 51\n" +
                "},{\n" +
                "\t\"_id\" : \"Staten Island\",\n" +
                "\t\"count\" : 969\n" +
                "},{\n" +
                "\t\"_id\" : \"Manhattan\",\n" +
                "\t\"count\" : 10259\n" +
                "},{\n" +
                "\t\"_id\" : \"Bronx\",\n" +
                "\t\"count\" : 2338\n" +
                "},{\n" +
                "\t\"_id\" : \"Queens\",\n" +
                "\t\"count\" : 5656\n" +
                "},{\n" +
                "\t\"_id\" : \"Brooklyn\",\n" +
                "\t\"count\" : 6086\n" +
                "}]", toJson(results));
    }

    @Test
    public void countGroupByQuerySortByCount() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select borough, count(borough) from " + COLLECTION + " GROUP BY borough\n" +
                "ORDER BY count(borough) DESC;");
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(6, results.size());
        assertEquals("[{\n" +
                "\t\"_id\" : \"Manhattan\",\n" +
                "\t\"count\" : 10259\n" +
                "},{\n" +
                "\t\"_id\" : \"Brooklyn\",\n" +
                "\t\"count\" : 6086\n" +
                "},{\n" +
                "\t\"_id\" : \"Queens\",\n" +
                "\t\"count\" : 5656\n" +
                "},{\n" +
                "\t\"_id\" : \"Bronx\",\n" +
                "\t\"count\" : 2338\n" +
                "},{\n" +
                "\t\"_id\" : \"Staten Island\",\n" +
                "\t\"count\" : 969\n" +
                "},{\n" +
                "\t\"_id\" : \"Missing\",\n" +
                "\t\"count\" : 51\n" +
                "}]", toJson(results));
    }

    @Test
    public void countGroupByQueryLimit() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select borough, count(borough) from " + COLLECTION + " GROUP BY borough LIMIT 2");
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(2, results.size());
        assertEquals(Arrays.asList(new Document("_id", "Missing").append("count", 51),
                new Document("_id", "Staten Island").append("count", 969)
        ), results);
    }

    @Test
    public void countGroupByQueryMultipleColumns() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select borough, cuisine, count(*) from " + COLLECTION + " GROUP BY borough, cuisine");
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(365, results.size());

        List<Document> filteredResults = Lists.newArrayList(Collections2.filter(results, new Predicate<Document>() {
            @Override
            public boolean apply(Document document) {
                return document.getInteger("count") > 500;
            }
        }));

        assertEquals("[{\n" +
                "\t\"_id\" : {\n" +
                "\t\t\"borough\" : \"Manhattan\",\n" +
                "\t\t\"cuisine\" : \"Chinese\"\n" +
                "\t},\n" +
                "\t\"count\" : 510\n" +
                "},{\n" +
                "\t\"_id\" : {\n" +
                "\t\t\"borough\" : \"Queens\",\n" +
                "\t\t\"cuisine\" : \"American \"\n" +
                "\t},\n" +
                "\t\"count\" : 1040\n" +
                "},{\n" +
                "\t\"_id\" : {\n" +
                "\t\t\"borough\" : \"Manhattan\",\n" +
                "\t\t\"cuisine\" : \"Café/Coffee/Tea\"\n" +
                "\t},\n" +
                "\t\"count\" : 680\n" +
                "},{\n" +
                "\t\"_id\" : {\n" +
                "\t\t\"borough\" : \"Manhattan\",\n" +
                "\t\t\"cuisine\" : \"Italian\"\n" +
                "\t},\n" +
                "\t\"count\" : 621\n" +
                "},{\n" +
                "\t\"_id\" : {\n" +
                "\t\t\"borough\" : \"Brooklyn\",\n" +
                "\t\t\"cuisine\" : \"American \"\n" +
                "\t},\n" +
                "\t\"count\" : 1273\n" +
                "},{\n" +
                "\t\"_id\" : {\n" +
                "\t\t\"borough\" : \"Manhattan\",\n" +
                "\t\t\"cuisine\" : \"American \"\n" +
                "\t},\n" +
                "\t\"count\" : 3205\n" +
                "},{\n" +
                "\t\"_id\" : {\n" +
                "\t\t\"borough\" : \"Queens\",\n" +
                "\t\t\"cuisine\" : \"Chinese\"\n" +
                "\t},\n" +
                "\t\"count\" : 728\n" +
                "},{\n" +
                "\t\"_id\" : {\n" +
                "\t\t\"borough\" : \"Brooklyn\",\n" +
                "\t\t\"cuisine\" : \"Chinese\"\n" +
                "\t},\n" +
                "\t\"count\" : 763\n" +
                "}]", toJson(filteredResults));
    }

    @Test
    public void countQuery() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select count(*) from " + COLLECTION + " where address.street LIKE '%Street'");
        long count = queryConverter.run(mongoDatabase);
        assertEquals(7499, count);
    }

    @Test
    public void deleteQuery() throws ParseException {
        String collection = "new_collection";
        MongoCollection newCollection = mongoDatabase.getCollection(collection);
        try {
            newCollection.insertOne(new Document("_id", "1").append("key", "value"));
            newCollection.insertOne(new Document("_id", "2").append("key", "value"));
            newCollection.insertOne(new Document("_id", "3").append("key", "value"));
            newCollection.insertOne(new Document("_id", "4").append("key2", "value2"));
            assertEquals(3, newCollection.count(new BsonDocument("key", new BsonString("value"))));
            QueryConverter queryConverter = new QueryConverter("delete from " + collection + " where key = 'value'");
            long deleteCount = queryConverter.run(mongoDatabase);
            assertEquals(3, deleteCount);
            assertEquals(1, newCollection.count());
        } finally {
            newCollection.drop();
        }
    }

    private static int getRandomFreePort() {
        Random r = new Random();
        int count = 0;

        while (count < 13) {
            int port = r.nextInt((1 << 16) - 1024) + 1024;

            ServerSocket so = null;
            try {
                so = new ServerSocket(port);
                so.setReuseAddress(true);
                return port;
            } catch (IOException ioe) {

            } finally {
                if (so != null)
                    try {
                        so.close();
                    } catch (IOException e) {
                    }
            }

        }

        throw new RuntimeException("Unable to find port");
    }

    private static String toJson(List<Document> documents) throws IOException {
        StringWriter stringWriter = new StringWriter();
        IOUtils.write("[", stringWriter);
        IOUtils.write(Joiner.on(",").join(Lists.transform(documents, new com.google.common.base.Function<Document, String>() {
            @Override
            public String apply(Document document) {
                return document.toJson(jsonWriterSettings);
            }
        })), stringWriter);
        IOUtils.write("]", stringWriter);
        return stringWriter.toString();
    }
}
