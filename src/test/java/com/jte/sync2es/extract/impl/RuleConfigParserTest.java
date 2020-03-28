package com.jte.sync2es.extract.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jte.sync2es.Tester;
import com.jte.sync2es.conf.RuleConfigParser;
import org.junit.Test;

import javax.annotation.Resource;

public class RuleConfigParserTest extends Tester {
    @Resource
    RuleConfigParser ruleParser;

    @Test
    public void initRulesTest() throws JsonProcessingException {
        ruleParser.initRules();

        String mapAsJson = new ObjectMapper().writeValueAsString(RuleConfigParser.RULES_MAP.asMap());
        System.out.println(mapAsJson);
    }

}
