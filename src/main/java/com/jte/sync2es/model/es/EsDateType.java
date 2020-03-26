package com.jte.sync2es.model.es;

import java.util.HashMap;
import java.util.Map;

public enum EsDateType {
    NULL("null"),
    BOOLEAN("boolean"),
    BYTE("byte"),
    SHORT("short"),
    INTEGER("integer"),
    LONG("long"),
    DOUBLE("double"),
    FLOAT("float"),
    HALF_FLOAT("half_float"),
    SCALED_FLOAT("scaled_float"),
    KEYWORD("keyword"),
    TEXT("text"),
    BINARY("binary"),
    DATA("date"),
    IP("ip");

    private String dataType;
    private static final Map<String, EsDateType> lookup = new HashMap<>();

    static {
        for (EsDateType dateType : EsDateType.values()) {
            lookup.put(dateType.getDataType(), dateType);
        }
    }

    public static EsDateType getDataType(String dataType) {
        return lookup.get(dataType);
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    EsDateType(String dataType) {
        this.dataType = dataType;
    }

}
