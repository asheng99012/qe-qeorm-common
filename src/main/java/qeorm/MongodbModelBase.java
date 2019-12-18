package qeorm;

import com.github.vincentrussell.query.mongodb.sql.converter.Query;
import org.bson.Document;
import org.springframework.cglib.beans.BeanMap;
import qeorm.utils.JsonUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by asheng on 2015/7/20 0020.
 */
public class MongodbModelBase extends ModelBase {
    public int insert() {
        TableStruct table = TableStruct.getTableStruct(this.getClass().getName());
        return Query.batchInsert(table.getMasterDbName(), table.getTableName(), fetchRealVal());
    }

    public int update() {
        TableStruct table = TableStruct.getTableStruct(this.getClass().getName());
        Map json = fetchRealVal();
        String key = table.getPrimaryKey();
        return Query.update(table.getMasterDbName(), table.getTableName(), new Document(key, json.get(key)), json);
    }

    public int save() {
        TableStruct table = TableStruct.getTableStruct(this.getClass().getName());
        BeanMap thisMap = BeanMap.create(this);
        if (thisMap.get(table.getPrimaryField()) != null) {
            try {
                ModelBase clone = this.getClass().newInstance();
                BeanMap beanMap = BeanMap.create(clone);
                beanMap.put(table.getPrimaryField(), thisMap.get(table.getPrimaryField()));
                int count = clone.count();
                if (count > 0) {
                    return update();
                } else {
                    return insert();
                }
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e.getCause());
            }
        } else {
            return insert();
        }
    }
}
