package com.jte.sync2es.extract.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jte.sync2es.Tester;
import com.jte.sync2es.extract.RuleParser;
import org.junit.Test;

import javax.annotation.Resource;

import static com.jte.sync2es.extract.RuleParser.RULES_MAP;

public class RuleParserImplTest extends Tester {
    @Resource
    RuleParser ruleParser;

    @Test
    public void initRulesTest() throws JsonProcessingException {
        ruleParser.initRules();

        String mapAsJson = new ObjectMapper().writeValueAsString(RULES_MAP.asMap());
        System.out.println(mapAsJson);
    }

}
