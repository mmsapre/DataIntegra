package com.integration.em.processing;

import com.integration.em.parallel.Consumer;
import com.integration.em.parallel.Parallel;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ParallelProcessableCollection<RecordType> extends ProcessableCollection<RecordType> {

    public ParallelProcessableCollection() {
        super(new ConcurrentLinkedQueue<RecordType>());
    }

    public ParallelProcessableCollection(Collection<RecordType> elements) {
        super(new ConcurrentLinkedQueue<RecordType>(elements));
    }

    public ParallelProcessableCollection(Processable<RecordType> elements) {
        super(new ConcurrentLinkedQueue<RecordType>(elements.get()));
    }

    @Override
    public  <OutputRecordType> Processable<OutputRecordType> createProcessable(OutputRecordType dummyForTypeInference) {
        return new ParallelProcessableCollection<>();
    }

    @Override
    public <OutputRecordType> Processable<OutputRecordType> createProcessableFromCollection(
            Collection<OutputRecordType> data) {
        return new ParallelProcessableCollection<>(data);
    }

    @Override
    public void foreach(final DataIterator<RecordType> iterator) {
        iterator.initialise();

        new Parallel<RecordType>().tryForeach(get(), new Consumer<RecordType>() {

            @Override
            public void execute(RecordType parameter) {
                iterator.next(parameter);
            }
        });
        iterator.finalise();
    }

    @Override
    public void foreach(Action<RecordType> action) {
        new Parallel<RecordType>().tryForeach(get(), (r)->action.execute(r));
    }

    public Collection<Collection<RecordType>> partitionRecords() {
        int numPartitions = (Runtime.getRuntime().availableProcessors() * 10);
        numPartitions = Math.min(size(), numPartitions);

        List<Collection<RecordType>> partitions = new LinkedList<>();
        for(int i = 0; i < numPartitions; i++) {
            partitions.add(new LinkedList<>());
        }
        int pIdx = 0;

        Iterator<RecordType> it = get().iterator();

        while(it.hasNext()) {
            partitions.get(pIdx++).add(it.next());

            if(pIdx==numPartitions) {
                pIdx=0;
            }
        }

        return partitions;
    }

    @Override
    public <OutputRecordType> Processable<OutputRecordType> map(final RecordMapper<RecordType, OutputRecordType> transformation) {
        final ProcessableCollector<OutputRecordType> resultCollector = new ThreadSafeProcessableCollector<>();

        resultCollector.setResult(createProcessable((OutputRecordType)null));

        resultCollector.initialise();

        new Parallel<Collection<RecordType>>().tryForeach(partitionRecords(), new Consumer<Collection<RecordType>>() {

            @Override
            public void execute(Collection<RecordType> parameter) {
                for(RecordType r : parameter) {
                    transformation.mapRecord(r, resultCollector);
                }
            }

        }, String.format("ParallelProcessableCollection.map: %d elements", size()));

        resultCollector.finalise();

        return resultCollector.getResult();
    }
}
