package com.jte.sync2any.util;

import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

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
     * 从数据库配置信息中解析出 host、端口等信息
     * @param url
     * @return
     */
    public static Map<String,String> getParamFromUrl(String url)
    {
        Map<String,String> param= new HashMap<>();
        String cleanURI = url.substring(5);
        URI uri = URI.create(cleanURI);
        param.put("type",uri.getScheme());
        param.put("host",uri.getHost());
        param.put("port",new Integer(uri.getPort()).toString());
        param.put("param",uri.getPath());
        return param;
    }

}
