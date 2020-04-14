package com.jte.sync2es.model.core;

import com.jte.sync2es.core.ResultCode;
import com.jte.sync2es.util.JsonUtil;

/**
 * 统一API响应结果封装
 */
public class Result<T> {
    private String code;
    private String message;
    private T data;

    public Result setCode(ResultCode resultCode) {
        this.code = resultCode.code();
        return this;
    }

    public Result setCode(String resultCode) {
        this.code = resultCode;
        return this;
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public Result setMessage(String message) {
        this.message = message;
        return this;
    }

    public T getData() {
        return data;
    }

    public Result setData(T data) {
        this.data = data;
        return this;
    }

    @Override
    public String toString() {
        return JsonUtil.objectToJson(this);
    }
}
