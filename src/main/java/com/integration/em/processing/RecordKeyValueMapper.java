package com.integration.em.processing;

import com.integration.em.tables.Pair;

import java.io.Serializable;

public interface RecordKeyValueMapper<KeyType, RecordType, OutputRecordType> extends Serializable {

    void mapRecordToKey(RecordType record, DataIterator<Pair<KeyType, OutputRecordType>> resultCollector);

}
