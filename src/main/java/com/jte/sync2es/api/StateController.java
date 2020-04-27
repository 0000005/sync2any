package com.jte.sync2es.api;

import com.jte.sync2es.conf.RuleConfigParser;
import com.jte.sync2es.model.mysql.TableMeta;
import com.jte.sync2es.util.DateUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;

import java.util.*;

@Slf4j
@Controller
public class StateController {

    /**
     * 获取所有任务的状态
     * 同步任务列表：
     * 每个同步任务包含：主题名称，主题组名称，数据库名，同步表名，同步字段名，状态【正在同步、异常停止同步】，同步延迟，最近同步时间，异常停止同步原因（如果发生的话）
     * @return
     */
    @GetMapping("index")
    public String allState(ModelMap modelMap)
    {
        List<Map<String,String>> mapList = new ArrayList<>();
        Map<String, TableMeta> ruleMap=RuleConfigParser.RULES_MAP.asMap();
        int i =1;
        for(String key:ruleMap.keySet())
        {
            TableMeta meta = ruleMap.get(key);
            long delay=meta.getLastSyncTime()-meta.getLastDataManipulateTime();
            Map<String,String> row = new HashMap<>(10);
            row.put("index",i+"");
            row.put("dbName",meta.getDbName());
            row.put("tableName",meta.getTableName());
            row.put("esIndexName",meta.getEsIndexName());
            row.put("topicName",meta.getTopicName());
            row.put("topicGroup",meta.getTopicGroup());
            row.put("delay",delay/1000+"");
            row.put("state",meta.getState().desc());
            row.put("lastSyncTime",DateUtils.formatDate(new Date(meta.getLastSyncTime()),DateUtils.SHORT));
            row.put("tpq",meta.getTpq()+"");
            row.put("errorReason",meta.getErrorReason()+"");
            i++;
            mapList.add(row);
        }
        modelMap.addAttribute("data", mapList);
        return "index";
    }
}
