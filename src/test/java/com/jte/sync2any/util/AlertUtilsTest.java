package com.jte.sync2any.util;

import org.junit.Test;

/**
 * @author JerryYin
 * @since 2021-04-27 16:52
 */

public class AlertUtilsTest {

    @Test
    public void testSendAlert(){
        AlertUtils.sendAlert("SCU72614T8b2e9f4c628209429a1c81fd40a175be5e041dbec6d33","人人都说中国话");
    }
}
