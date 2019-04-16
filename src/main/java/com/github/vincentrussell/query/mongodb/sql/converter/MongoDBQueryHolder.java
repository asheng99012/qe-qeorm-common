package com.github.vincentrussell.query.mongodb.sql.converter;

import com.github.vincentrussell.query.mongodb.sql.converter.util.SqlUtils;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.Validate.notNull;

public class MongoDBQueryHolder {
    private final String collection;
    private final SQLCommandType sqlCommandType;
    private Document query = new Document();
    private Document projection = new Document();
    private Document aliseProjection = new Document();
    private Document sort = new Document();
    private boolean distinct = false;
    private boolean countAll = false;
    private List<String> groupBys = new ArrayList<>();
    private long limit = -1;
    private long offset = -1;
    private Document items;

    /**
     * Pojo to hold the MongoDB data
     *
     * @param collection     the collection that the query will be run on.
     * @param sqlCommandType
     */
    public MongoDBQueryHolder(String collection, SQLCommandType sqlCommandType) {
        this.collection = collection.replaceAll("`", "");
        this.sqlCommandType = sqlCommandType;
    }

    /**
     * Get the object used to create a projection
     *
     * @return the fields to be returned by the quer
     */
    public Document getProjection() {
        return projection;
    }

    /**
     * Get the object used to create a query
     *
     * @return the where clause section of the query in mongo formt
     */
    public Document getQuery() {
        return query;
    }

    /**
     * Get the collection to run the query on
     *
     * @return the collection to run the query on
     */
    public String getCollection() {
        return collection;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public void setQuery(Document query) {
        notNull(query, "query is null");
        this.query = query;
        dealQuery(this.query);
    }

    public void setProjection(Document projection) {
        notNull(projection, "projection is null");
        this.projection = projection;
    }

    public Document getAliseProjection() {
        return aliseProjection;
    }

    public void setAliseProjection(Document aliseProjection) {
        this.aliseProjection = aliseProjection;
    }

    public Document getSort() {
        return sort;
    }

    public void setSort(Document sort) {
        notNull(sort, "sort is null");
        this.sort = sort;
    }

    public Document getItems() {
        return items;
    }

    public void setItems(Document items) {
        this.items = items;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    public boolean isCountAll() {
        return countAll;
    }

    public void setCountAll(boolean countAll) {
        this.countAll = countAll;
    }

    public void setGroupBys(List<String> groupBys) {
        this.groupBys = groupBys;
    }

    public List<String> getGroupBys() {
        return groupBys;
    }

    public long getLimit() {
        return limit;
    }

    public void setLimit(long limit) {
        this.limit = limit;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public SQLCommandType getSqlCommandType() {
        return sqlCommandType;
    }

    public void dealQuery(Document doc) {
        for (Map.Entry entry : doc.entrySet()) {
            if(entry.getKey().equals("$in"))continue;
            Object value = entry.getValue();
            if (value instanceof List) {
                List list = (List) value;
                for (int i = 0; i < list.size(); i++) {
                    dealQuery((Document) list.get(i));
                }
            }
            if (value instanceof Document) {
                dealQuery((Document) value);
            }
            if (value instanceof String) {
                String val = (String) value;
                if (val.matches("^\\d{4}-\\d{1,2}-\\d{1,2}(\\s\\d{1,2}:\\d{1,2}:\\d{1,2}(.\\d{1,5})?)?$")) {
                    if (val.indexOf(" ") == -1) {
                        val = val + " 00:00:00";
                    }
                    entry.setValue(SqlUtils.parseNaturalLanguageDate(val));
                }
            }
        }
    }
}
