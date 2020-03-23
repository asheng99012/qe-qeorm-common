package com.github.vincentrussell.query.mongodb.sql.converter;

import com.alibaba.fastjson.JSON;
import com.github.vincentrussell.query.mongodb.sql.converter.jdbc.MongodbConnection;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResultUtils {
    static Logger logger = LoggerFactory.getLogger(ResultUtils.class);

    public static <T> T exec(MongodbConnection connection, String sql) throws ParseException, IOException {
        return exec(connection.getDataBase(), sql);
    }

    public static <T> T exec(MongoDatabase mongoDatabase, String sql) throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter(sql);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        logger.info(byteArrayOutputStream.toString("UTF-8"));
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        SQLCommandType type = mongoDBQueryHolder.getSqlCommandType();
        Object ret = null;
        if (type.equals(SQLCommandType.INSERT) || type.equals(SQLCommandType.DELETE) || type.equals(SQLCommandType.UPDATE)) {
            ret = queryConverter.run(mongoDatabase);
        } else if (type.equals(SQLCommandType.SELECT)) {
            ret = select(queryConverter, mongoDatabase);
        } else {
        }
        return (T) ret;
    }

    public static <T> T select(QueryConverter queryConverter, MongoDatabase mongoDatabase) {
        Object ret = null;
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        if (mongoDBQueryHolder.isDistinct()) {
            QueryResultIterator<String> distinctIterable = queryConverter.run(mongoDatabase);
            final List _ret = (List) Lists.newArrayList(distinctIterable);
            List<Map> list = Lists.newArrayList();
            final String clumon = mongoDBQueryHolder.getProjection().keySet().toArray()[0].toString();
            for (int i = 0; i < _ret.size(); i++) {
                final Object val = _ret.get(i);
                list.add(new HashMap() {{
                    put(clumon, val);
                }});
            }
            ret = list;
        } else if (mongoDBQueryHolder.isCountAll()) {
            ret = queryConverter.run(mongoDatabase);
        } else if (mongoDBQueryHolder.getGroupBys().size() > 0) {
            QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
            List<Document> results = Lists.newArrayList(distinctIterable);
            List<Map> list = Lists.newArrayList();
            if (results.size() > 0) {
                if (Document.class.isInstance(results.get(0).get("_id"))) {
                    for (int i = 0; i < results.size(); i++) {
                        Document doc = results.get(i);
                        Map val = (Map) doc.get("_id");
                        val.put("count", doc.get("count"));
                        list.add(val);
                    }
                } else {
                    String cloumn = mongoDBQueryHolder.getProjection().get("_id").toString().replace("$", "");
                    for (int i = 0; i < results.size(); i++) {
                        Document doc = results.get(i);
                        Map val = Maps.newHashMap();
                        val.put(cloumn, doc.get("_id"));
                        val.put("count", doc.get("count"));
                        list.add(val);
                    }
                }
            }
            ret = list;
        } else {
            QueryResultIterator<Document> findIterable = queryConverter.run(mongoDatabase);
            ret = Lists.newArrayList(findIterable);
        }
        return (T) ret;
    }
}
