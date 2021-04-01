package com.jte.sync2any.model.core;

import lombok.Data;

/**
 * 统一API响应结果封装
 */
@Data
public class Result<T> {
    private String code;
    private String message;
    private T data;
}
