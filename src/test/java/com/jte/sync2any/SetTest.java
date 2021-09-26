package com.jte.sync2any;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author JerryYin
 * @since 2021-05-08 17:37
 */
public class SetTest {

    @Test
    public void test(){
        Set<String> s = new HashSet<>();
        s.add("1");
        s.add("2");
        s.add("3");

        for(Iterator<String> it = s.iterator(); it.hasNext();){
            if(it.next().equals("2")){
                it.remove();
            }
        }

        Assert.assertEquals(2,s.size());

    }

    @Test
    public void sortTest(){
        List<String> s = new ArrayList<>();
        s.add("group_code");
        s.add("id");
        System.out.println(s.stream().sorted().collect(Collectors.toList()));

    }
}
