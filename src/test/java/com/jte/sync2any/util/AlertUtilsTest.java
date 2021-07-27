package com.jte.sync2any.util;

import org.junit.Test;

/**
 * @author JerryYin
 * @since 2021-04-27 16:52
 */

public class AlertUtilsTest {

    @Test
    public void testSendAlert(){
        AlertUtils.sendAlert("YinWenJun","人人都说中国话");
    }
}
