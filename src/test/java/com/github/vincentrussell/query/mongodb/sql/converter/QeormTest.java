package com.github.vincentrussell.query.mongodb.sql.converter;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSON;
import com.github.vincentrussell.query.mongodb.sql.converter.jdbc.MongodbDataSource;
import org.apache.commons.lang3.time.DateUtils;
import org.junit.Before;
import org.junit.Test;
import qeorm.CacheManager;
import qeorm.SqlExecutor;
import qeorm.SqlSession;

import javax.sql.DataSource;
import java.text.ParseException;
import java.util.*;

public class QeormTest {
    SqlSession session;

    //    @Before
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

    @Before
    public void setupProduc() throws Exception {
        session = new SqlSession();
        MongodbDataSource dataSource = new MongodbDataSource();
        dataSource.setUrl("mongodb://172.21.100.21:27017,172.21.100.20:27017,172.21.100.22:27017");
        dataSource.setUsername("logs_user");
        dataSource.setPassword("tiXcaDtioasdS");
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
    public void tongji() {
        String sql = "select type as mytype,count(*) as cc from rpc_logs where create_at>'2019-04-01' and handle_len>3000 group by type";
         sql = "select * from rpc_logs order by create_at desc limit 1,10";

//        sql = " SELECT     a.city_name,    sum(CASE WHEN a.monthly_price <= 2000  THEN 1 ELSE 0 END) as '0-2,000', sum(CASE WHEN a.monthly_price > 2000 AND a.monthly_price <= 4000 THEN 1 ELSE 0 END) as '2,000-4,000' FROM webank_shoufang_detail a  ";
        Object ret = SqlExecutor.execSql(sql, null, Map.class, "mongo");
        System.out.println(ret);
        System.out.println(JSON.toJSONString(ret));
    }

    @Test
    public void testSelect() throws ParseException {
        Map params = new HashMap() {{
            put("create_at_start", "2019-04-16 10:24:09");
            put("create_at_end", "2019-04-17 10:24:09");
//            put("pn", 2);
//            put("ps", 5);
        }};
        String sql = "select * from rpc_logs where  create_at>=:create_at  order by create_at desc";
        sql="select type,count(*) FROM rpc_logs where   create_at >{create_at_start} AND create_at < {create_at_end} group by type ";
        sql="select type,count(*) FROM rpc_logs where type in ('com.dankegongyu.risk.sence.item.voice.VoiceRecognition','com.dankegongyu.thirdparty.controller.filter.LogFilter'\n) and  create_at >{create_at_start} AND create_at < {create_at_end}  group by type";

        Object ret = SqlExecutor.execSql(sql, params, Map.class, "mongo");
        sql = "select count(*) from rpc_logs where  create_at>=:create_at  order by create_at desc";
        ret = SqlExecutor.execSqlForObject(sql, params, Integer.class, "mongo");
        System.out.println(ret);
    }

    @Test
    public void testModelSelect() throws ParseException {
        CacheManager.instance.open();
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
