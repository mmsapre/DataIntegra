package com.integration.em.detect;

import com.integration.em.datatypes.ColumnType;

public interface DetectType {

    ColumnType detectTypeForColumn(Object[] attributeValues, String attributeLabel);
}
