package com.jte.sync2any.util;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;

public class DateUtils {
    public static final String STANDARD="yyyy-MM-dd HH:mm:ss";
    public static final String SHORT="MM-dd HH:mm:ss";
    public static final String MMdd="MM-dd";

    /**
     * 获取n天后的最后时刻
     * @return
     */
    public static Date getNextEndDay(int n)
    {
        ZoneId zoneId = ZoneId.systemDefault();
        LocalDateTime localDateTime = LocalDateTime.now().plusDays(n).with(LocalTime.MAX);
        ZonedDateTime zdt = localDateTime.atZone(zoneId);
        return Date.from(zdt.toInstant());
    }

    /**
     * 将日期格式化为字符串
     * @param date
     * @param pattern
     * @return
     */
    public static String formatDate(Date date,String pattern){
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.format(date);
    }



}
