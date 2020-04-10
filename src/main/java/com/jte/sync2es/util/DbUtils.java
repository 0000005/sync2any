package com.jte.sync2es.util;

import org.apache.commons.lang3.StringUtils;

public class DbUtils {
    /**
     * 去处字符串首尾的单引号
     * @param origin
     * @return
     */
    public static String delQuote(String origin)
    {
        if(StringUtils.isBlank(origin))
        {
            return origin;
        }
        if(origin.indexOf("'")==0&&origin.lastIndexOf("'")==(origin.length()-1))
        {
            return origin.substring(1,origin.length()-1);
        }
        return origin;
    }

    /**
     * 去处字符串首尾的单引号
     * @param origin
     * @return
     */
    public static Object delQuote(Object origin)
    {
        if(origin instanceof String)
        {
            return delQuote(((String)origin));
        }
        return origin;
    }
}
