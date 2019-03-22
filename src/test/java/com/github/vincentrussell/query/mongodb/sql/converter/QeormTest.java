package com.github.vincentrussell.query.mongodb.sql.converter;

import com.alibaba.druid.pool.DruidDataSource;
import com.github.vincentrussell.query.mongodb.sql.converter.jdbc.MongodbDataSource;
import org.apache.commons.lang3.time.DateUtils;
import org.junit.Before;
import org.junit.Test;
import qeorm.SqlExecutor;
import qeorm.SqlSession;

import javax.sql.DataSource;
import java.text.ParseException;
import java.util.*;

public class QeormTest {
    SqlSession session;

    @Before
    public void setup() throws Exception {
        session = new SqlSession();
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
        Map<String, DataSource> dataSourcesMap = new HashMap<>();
        dataSourcesMap.put("mongo", dataSource);
        session.setDataSources(dataSourcesMap);
    }

    @Test
    public void testSelect() throws ParseException {
        Map params = new HashMap() {{
            put("create_at", DateUtils.parseDate("2019-03-10", "yyyy-MM-dd"));
            put("pn", 2);
            put("ps", 5);
        }};
        String sql = "select * from rpc_logs where  create_at>=:create_at  order by create_at desc";
        Object ret = SqlExecutor.execSql(sql, params, Map.class, "mongo");
        sql = "select count(*) from rpc_logs where  create_at>=:create_at  order by create_at desc";
        ret = SqlExecutor.execSqlForObject(sql, params, Integer.class, "mongo");
        System.out.println(ret);
    }

    @Test
    public void testModelSelect() throws ParseException {
        RpcLog insert = new RpcLog();
        insert.setDataId("123456");
        insert.setTraceId("thistraceid");
        insert.setCreateAt(new Date());
        Map params = new HashMap() {{
            put("create_at", DateUtils.parseDate("2019-03-10", "yyyy-MM-dd"));
            put("pn", 2);
            put("ps", 5);
        }};
        Object ret;
        insert.setRequestData(params);
        ret = insert.insert();
        RpcLog log = new RpcLog();
        log.setDataId("123456");
        ret = log.select();
        ret = log.count();
        System.out.println("ok");

    }


}
