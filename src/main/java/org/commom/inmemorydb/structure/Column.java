package org.commom.inmemorydb.structure;

/**
 * Created by amd on 9/9/15.
 */
public interface Column {

    public String getColumnName() ;

    public void setColumnName(String columnName) ;

    public String getDataType() ;

    public void setDataType(String dataType) ;

    public Object getDefaultValue() ;

    public void setDefaultValue(Object defaultValue) ;

    public Boolean getIsIndexed() ;

    public void setIsIndexed(Boolean isIndexed) ;

    public Boolean getIsId() ;

    public void setIsId(Boolean isId) ;

    public void setPosition(int position);

    public int getPosition();

}
