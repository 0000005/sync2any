package com.jte.sync2any.util;

import cn.hutool.http.HttpUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

/**
 * @author JerryYin
 * @since 2021-04-27 10:34
 */
@Slf4j
public class AlertUtils {
    private static String url="https://sc.ftqq.com/";

    public static void sendAlert(String secret,String text){
        Map<String ,Object> param = new HashMap<>();
        param.put("text",text);
        String content = HttpUtil.get(url+secret+".send",param);
        log.warn("send msg result:{}",content);
    }
}
