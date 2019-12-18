package com.github.vincentrussell.query.mongodb.sql.converter;

import com.google.common.collect.Lists;
import org.bson.Document;
import qeorm.MongoDbExecutor;
import qeorm.utils.JsonUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Query {

    public static int update(String dbName, String tableName, Document query, Map update) {
        new MongoDbExecutor().getConn(dbName).getDataBase().getCollection(tableName).updateOne(query, new Document("$set", update));
        return 1;
    }

    public static int batchInsert(String dbName, String tableName, Map data) {
        Document doc = new Document();
        doc.putAll(data);
        new MongoDbExecutor().getConn(dbName).getDataBase().getCollection(tableName).insertOne(doc);
        return 1;
    }

    public static int batchInsert(String dbName, String tableName, List<Map> dataList) {
        List<Document> list = new ArrayList<>();
        dataList.forEach(data -> {
            Document doc = new Document();
            doc.putAll(data);
            list.add(doc);
        });
        new MongoDbExecutor().getConn(dbName).getDataBase().getCollection(tableName).insertMany(list);
        return 1;
    }
}
