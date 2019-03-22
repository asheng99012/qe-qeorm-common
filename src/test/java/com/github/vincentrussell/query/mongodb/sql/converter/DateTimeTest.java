package com.github.vincentrussell.query.mongodb.sql.converter;

import com.github.vincentrussell.query.mongodb.sql.converter.util.SqlUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;
import qeorm.AbstractRegexOperator;
import qeorm.StringFormat;
import qeorm.utils.JsonUtils;
import qeorm.utils.Wrap;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

public class DateTimeTest {
    @Test
    public void format() {
        String dateStr = "2019-10-28 10:22:22";
        Object dd = SqlUtils.parseNaturalLanguageDate(dateStr);
        //日期解析，通用时间表达式
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd");

        //2019-10-28T10:23:12.000+08:00
        DateTime dateTime = fmt.parseDateTime(dateStr);
        System.out.println(dateTime);
    }

    @Test
    public void testJson() {
        Map map = new HashMap() {{
            put("a", "aa");
            put("int", 1);
            put("dobbu", 2D);
            put("true", true);
            put("false", false);
            put("date", new Date());
            put("json", new HashMap() {{
                put("j1", "dd");
                put("j2", "ff");
            }});
        }};
        String sql = "select * from mytable where a=:a and int=:int and dobbu=:dobbu and true=:true and false=:false and date=:date and json=:json";
        Wrap wrap = Wrap.getWrap(map);
        sql = StringFormat.format(sql, new AbstractRegexOperator() {
            @Override
            public String getPattern() {
                return ":([\\.a-zA-Z\\d_]+)";
            }

            @Override
            public String exec(Matcher m) {
                Object val = wrap.getValue(m.group(1));
                if (val instanceof Number || val instanceof Boolean)
                    return String.valueOf(val);
                if (val instanceof Date) {
                    String _val = JsonUtils.toJson(val);
                    val = _val.replaceAll("\"", "");
                }
                if (!(val instanceof String))
                    val = JsonUtils.toJson(val);
                return "'" + val + "'";
            }
        });

        System.out.println(sql);

    }
}
