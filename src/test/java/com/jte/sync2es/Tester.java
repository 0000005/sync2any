package com.jte.sync2any;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

//@Transactional(rollbackFor = Exception.class)
@RunWith(SpringRunner.class)
@SpringBootTest(classes = sync2anyApplication.class)
//@Rollback
public class Tester {
    @Test
    public void empty(){}
}
