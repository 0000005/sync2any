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
    private static String url = "http://ticket.jintiane.cn/notify.php";
    private static String sendkey = "jte";

    public static void sendAlert(String touid, String text) {
        Map<String, Object> param = new HashMap<>();
        param.put("sendkey", sendkey);
        param.put("touid", touid);
        param.put("text", text);
        String content = HttpUtil.get(url, param);
        log.warn("send msg result:{}", content);
    }
}
