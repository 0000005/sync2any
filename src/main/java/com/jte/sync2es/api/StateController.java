package com.jte.sync2es.api;

import com.jte.sync2es.model.core.Result;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/state")
@Slf4j
public class StateController {

    /**
     * 获取所有任务的状态
     * 同步任务列表：
     * 每个同步任务包含：主题名称，主题组名称，数据库名，同步表名，同步字段名，状态【正在同步、异常停止同步】，同步延迟，最近同步时间，异常停止同步原因（如果发生的话）
     * @return
     */
    @GetMapping("all")
    public Result allState()
    {
        
        return null;
    }
}
