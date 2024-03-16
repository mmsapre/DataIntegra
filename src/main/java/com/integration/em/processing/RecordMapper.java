package com.integration.em.processing;

import java.io.Serializable;

public interface RecordMapper<RecordType, OutputRecordType> extends Serializable {

    void mapRecord(RecordType record, DataIterator<OutputRecordType> resultCollector);
}
