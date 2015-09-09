package org.commom.inmemorydb.structure.impl;

import org.commom.inmemorydb.structure.Column;

/**
 * Created by amd on 9/9/15.
 */
public class ColumnImpl implements Column {

    String columnName;
    String dataType;
    Object defaultValue;
    Boolean isIndexed;
    Boolean isId;
    int position;

    public ColumnImpl(String columnName, String dataType, Object defaultValue, Boolean isIndexed, Boolean isId, int position) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.defaultValue = defaultValue;
        this.isIndexed = isIndexed;
        this.isId = isId;
        this.position=position;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
    }

    public Boolean getIsIndexed() {
        return isIndexed;
    }

    public void setIsIndexed(Boolean isIndexed) {
        this.isIndexed = isIndexed;
    }

    public Boolean getIsId() {
        return isId;
    }

    public void setIsId(Boolean isId) {
        this.isId = isId;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }
}
