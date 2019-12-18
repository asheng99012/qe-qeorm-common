package qeorm;

import com.github.vincentrussell.query.mongodb.sql.converter.ResultUtils;
import com.github.vincentrussell.query.mongodb.sql.converter.jdbc.MongodbConnection;
import com.github.vincentrussell.query.mongodb.sql.converter.jdbc.MongodbDataSource;
import com.mongodb.client.MongoDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcDaoSupport;
import qeorm.utils.JsonUtils;
import qeorm.utils.Wrap;

import java.sql.SQLException;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * Created by ashen on 2017-2-4.
 */
public class MongoDbExecutor extends SqlResultExecutor {
    private Logger logger = LoggerFactory.getLogger(MongoDbExecutor.class);


    @Override
    public <T> T exec(Map<String, Object> map) {
        String sql = createSql(map);
        sql = sql.replace("where 1=1 ", " ");
        logger.info("要在数据库{}上执行的sql：{} , 参数为：{}", getResult().getSqlConfig().getDbName(), sql, JsonUtils.toJson(map));

        try {
            NamedParameterJdbcDaoSupport jdbc = SqlSession.instance.getSupport(getResult().getSqlConfig().getDbName());
            MongodbDataSource dataSource = (MongodbDataSource) jdbc.getDataSource();
            MongodbConnection connection = (MongodbConnection) dataSource.getConnection();
            Object ret = ResultUtils.exec(connection, sql);
            return (T) ret;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    public String createSql(Map<String, Object> map) {
        String sql = getResult().getSql();
        Wrap wrap = Wrap.getWrap(map);
        sql = StringFormat.format(sql, new AbstractRegexOperator() {
            @Override
            public String getPattern() {
                return ":([\\.a-zA-Z\\d_]+)";
            }

            @Override
            public String exec(Matcher m) {
                Object val = wrap.getValue(m.group(1));
                if (val == null) return null;
                if (val instanceof Number || val instanceof Boolean)
                    return String.valueOf(val);
                if (val instanceof Date) {
                    String _val = JsonUtils.toJson(val);
                    val = _val.replaceAll("\"", "");
                }
                if (!(val instanceof String))
                    val = JsonUtils.toJson(val);
                return "'" + val.toString().replaceAll("'", "&#39;") + "'";
            }
        });
        sql = sql.replaceAll("\\n", "<br />");
        return sql;
    }


    public MongodbConnection getConn(String dbName) {
        NamedParameterJdbcDaoSupport jdbc = (NamedParameterJdbcDaoSupport) SqlSession.instance.getSupport(dbName);
        MongodbDataSource dataSource = (MongodbDataSource) jdbc.getDataSource();

        try {
            return (MongodbConnection) dataSource.getConnection();
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }
}
