package com.integration.em.processing;

public class ThreadSafeProcessableCollector<RecordType> extends ProcessableCollector<RecordType> {

    private ThreadBoundObject<Processable<RecordType>> intermediateResults;

    @Override
    public void initialise() {
        super.initialise();

        intermediateResults = new ThreadBoundObject<>((t)->new ProcessableCollection<>());
    }

    @Override
    public void next(RecordType record) {
        Processable<RecordType> localResult = intermediateResults.get();

        if(record!=null) {
            localResult.add(record);
        }
    }

    @Override
    public void finalise() {
        Processable<RecordType> result = getResult();

        for(Processable<RecordType> partialResult : intermediateResults.getAll()) {
            result = result.append(partialResult);
        }

        setResult(result);
    }
}
