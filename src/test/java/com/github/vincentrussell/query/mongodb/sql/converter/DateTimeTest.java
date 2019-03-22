package com.github.vincentrussell.query.mongodb.sql.converter;

import com.github.vincentrussell.query.mongodb.sql.converter.util.SqlUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

public class DateTimeTest {
    @Test
    public void format(){
        String dateStr = "2019-10-28 10:22:22";
       Object dd=  SqlUtils.parseNaturalLanguageDate(dateStr);
        //日期解析，通用时间表达式
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd");

        //2019-10-28T10:23:12.000+08:00
        DateTime dateTime = fmt.parseDateTime(dateStr);
        System.out.println(dateTime);
    }
}
