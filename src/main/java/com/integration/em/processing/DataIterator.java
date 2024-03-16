package com.integration.em.processing;

import java.io.Serializable;

public interface DataIterator <RecordType> extends Serializable {

    void initialise();

    void next(RecordType record);

    void finalise();
}
