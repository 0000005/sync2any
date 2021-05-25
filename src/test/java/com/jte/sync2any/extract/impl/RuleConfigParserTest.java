package com.jte.sync2any.extract.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jte.sync2any.Tester;
import com.jte.sync2any.conf.RuleConfigParser;
import org.junit.Test;

import javax.annotation.Resource;

public class RuleConfigParserTest extends Tester {
    @Resource
    RuleConfigParser ruleParser;

    @Test
    public void initRulesTest() throws JsonProcessingException {
        ruleParser.initAllRules();

        String mapAsJson = new ObjectMapper().writeValueAsString(RuleConfigParser.RULES_MAP.asMap());
        System.out.println(mapAsJson);
    }

}
