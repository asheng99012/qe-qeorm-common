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
        return SqlExecutor.insert(this);
    }

    public int update() {
        return SqlExecutor.update(this);
    }

    public int save() {
        return SqlExecutor.save(this);
    }
}
