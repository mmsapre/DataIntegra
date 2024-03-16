package com.integration.em.processing;

public class ProcessableCollector<RecordType> implements DataIterator<RecordType> {

    private Processable<RecordType> result;

    public Processable<RecordType> getResult() {
        return result;
    }


    public void setResult(Processable<RecordType> result) {
        this.result = result;
    }

    @Override
    public void initialise() {
    }

    @Override
    public void next(RecordType record) {
        result.add(record);
    }

    @Override
    public void finalise() {
    }
}
