package com.integration.em.processing;


import com.integration.em.tables.Pair;
import com.integration.em.utils.Function;
import com.integration.em.utils.ProgressReporter;

import java.util.*;

public class ProcessableCollection<RecordType> implements Processable<RecordType> {

    private static final long serialVersionUID = 1L;
    protected Collection<RecordType> elements;

    public ProcessableCollection() {
        elements = new LinkedList<>();
    }

    public ProcessableCollection(Collection<RecordType> elements) {
        if (elements != null) {
            this.elements = elements;
        } else {
            elements = new LinkedList<>();
        }
    }

    public ProcessableCollection(Processable<RecordType> elements) {
        if (elements != null) {
            this.elements = elements.get();
        } else {
            this.elements = new LinkedList<>();
        }
    }

    public void add(RecordType element) {
        elements.add(element);
    }

    @Override
    public void addAll(Collection<RecordType> elements) {
        if (elements != null && elements.size() > 0) {
            this.elements.addAll(elements);
        }
    }

    public Collection<RecordType> get() {
        return elements;
    }

    public int size() {
        return elements.size();
    }

    public void merge(Processable<RecordType> other) {
        if (other != null) {
            for (RecordType elem : other.get()) {
                add(elem);
            }
        }
    }

    public void remove(RecordType element) {
        elements.remove(element);
    }

    public void remove(Collection<RecordType> element) {
        elements.removeAll(element);
    }

    @Override
    public Processable<RecordType> copy() {
        return createProcessableFromCollection(get());
    }

    public RecordType firstOrNull() {
        Collection<RecordType> data = get();
        if (data == null || data.size() == 0) {
            return null;
        } else {
            return data.iterator().next();
        }
    }


    @Override
    public <OutputRecordType> Processable<OutputRecordType>
    createProcessable(
            OutputRecordType dummyForTypeInference) {
        return new ProcessableCollection<>();
    }

    @Override
    public <OutputRecordType> Processable<OutputRecordType> createProcessableFromCollection(
            Collection<OutputRecordType> data) {
        return new ProcessableCollection<>(data);
    }


    @Override
    public Processable<RecordType>
    assignUniqueRecordIds(
            Function<RecordType, Pair<Long, RecordType>> assignUniqueId) {
        long id = 0;

        Processable<RecordType> result = createProcessable(null);

        for (RecordType record : get()) {
            RecordType r = assignUniqueId.execute(new Pair<Long, RecordType>(id++, record));
            result.add(r);
        }

        return result;
    }


    @Override
    public void
    foreach(
            DataIterator<RecordType> iterator) {

        iterator.initialise();

        for (RecordType r : get()) {
            iterator.next(r);
        }

        iterator.finalise();
    }

    @Override
    public void foreach(Action<RecordType> action) {
        for (RecordType r : get()) {
            action.execute(r);
        }
    }


    @Override
    public <OutputRecordType>
    Processable<OutputRecordType>
    map(
            RecordMapper<RecordType, OutputRecordType> transformation) {

        ProgressReporter progress = new ProgressReporter(size(), "");

        ProcessableCollector<OutputRecordType> resultCollector = new ProcessableCollector<>();

        resultCollector.setResult(createProcessable(null));

        resultCollector.initialise();

        for (RecordType record : get()) {
            transformation.mapRecord(record, resultCollector);

            progress.incrementProgress();
            progress.report();
        }

        resultCollector.finalise();

        return resultCollector.getResult();
    }


    @Override
    public <OutputRecordType> Processable<OutputRecordType> map(Function<OutputRecordType, RecordType> transformation) {
        return map((RecordType record, DataIterator<OutputRecordType> resultCollector) -> {
            OutputRecordType result = transformation.execute(record);
            if (result != null) {
                resultCollector.next(result);
            }
        });
    }


    protected <KeyType, ElementType>
    Map<KeyType, List<ElementType>>
    hashRecords(
            Processable<ElementType> dataset,
            Function<KeyType, ElementType> hash) {
        HashMap<KeyType, List<ElementType>> hashMap = new HashMap<>();

        for (ElementType record : dataset.get()) {
            KeyType key = hash.execute(record);

            if (key != null) {
                List<ElementType> records = hashMap.get(key);
                if (records == null) {
                    records = new ArrayList<>();
                    hashMap.put(key, records);
                }

                records.add(record);
            }
        }

        return hashMap;
    }


    @Override
    public <KeyType>
    Processable<Pair<RecordType, RecordType>>
    symmetricJoin(Function<KeyType, RecordType> joinKeyGenerator) {
        return symmetricJoin(joinKeyGenerator, new ProcessableCollector<Pair<RecordType, RecordType>>());
    }


    @Override
    public <KeyType>
    Processable<Pair<RecordType, RecordType>>
    symmetricJoin(
            Function<KeyType, RecordType> joinKeyGenerator,
            final ProcessableCollector<Pair<RecordType, RecordType>> collector) {

        Map<KeyType, List<RecordType>> joinKeys = hashRecords(this, joinKeyGenerator);

        collector.setResult(createProcessable(null));
        collector.initialise();

        for (List<RecordType> block : joinKeys.values()) {
            for (int i = 0; i < block.size(); i++) {
                for (int j = i + 1; j < block.size(); j++) {
                    if (i != j) {
                        collector.next(new Pair<>(block.get(i), block.get(j)));
                    }
                }
            }
        }

        collector.finalise();

        return collector.getResult();
    }


    @Override
    public <KeyType>
    Processable<Pair<RecordType, RecordType>>
    join(
            Processable<RecordType> dataset2,
            Function<KeyType, RecordType> joinKeyGenerator) {
        return join(dataset2, joinKeyGenerator, joinKeyGenerator);
    }


    @Override
    public <KeyType, RecordType2>
    Processable<Pair<RecordType, RecordType2>>
    join(
            Processable<RecordType2> dataset2,
            Function<KeyType, RecordType> joinKeyGenerator1,
            Function<KeyType, RecordType2> joinKeyGenerator2) {

        final Map<KeyType, List<RecordType>> joinKeys1 = hashRecords(this, joinKeyGenerator1);
        final Map<KeyType, List<RecordType2>> joinKeys2 = hashRecords(dataset2, joinKeyGenerator2);

        Processable<Pair<RecordType, RecordType2>> result = createProcessableFromCollection(joinKeys1.keySet()).map(new RecordMapper<KeyType, Pair<RecordType, RecordType2>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void mapRecord(KeyType key1, DataIterator<Pair<RecordType, RecordType2>> resultCollector) {
                List<RecordType> block = joinKeys1.get(key1);
                List<RecordType2> block2 = joinKeys2.get(key1);

                if (block2 != null) {

                    for (RecordType r1 : block) {
                        for (RecordType2 r2 : block2) {
                            resultCollector.next(new Pair<>(r1, r2));
                        }
                    }

                }
            }
        });

        return result;
    }


    @Override
    public <KeyType>
    Processable<Pair<RecordType, RecordType>>
    leftJoin(
            Processable<RecordType> dataset2,
            Function<KeyType, RecordType> joinKeyGenerator) {
        return leftJoin(dataset2, joinKeyGenerator, joinKeyGenerator);
    }


    @Override
    public <KeyType, RecordType2>
    Processable<Pair<RecordType, RecordType2>>
    leftJoin(
            Processable<RecordType2> dataset2,
            Function<KeyType, RecordType> joinKeyGenerator1,
            Function<KeyType, RecordType2> joinKeyGenerator2) {


        Map<KeyType, List<RecordType2>> joinKeys2 = hashRecords(dataset2, joinKeyGenerator2);

        Processable<Pair<RecordType, RecordType2>> result = map(new RecordMapper<RecordType, Pair<RecordType, RecordType2>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void mapRecord(RecordType r1, DataIterator<Pair<RecordType, RecordType2>> resultCollector) {
                List<RecordType2> block = joinKeys2.get(joinKeyGenerator1.execute(r1));

                if (block != null) {
                    for (RecordType2 r2 : block) {
                        resultCollector.next(new Pair<>(r1, r2));
                    }
                } else {
                    resultCollector.next(new Pair<>(r1, null));
                }
            }
        });

        return result;
    }

    @Override
    public <KeyType, OutputRecordType>
    Processable<Group<KeyType, OutputRecordType>>
    group(RecordKeyValueMapper<KeyType, RecordType, OutputRecordType> groupBy) {

        GroupCollector<KeyType, OutputRecordType> groupCollector = new GroupCollector<>();

        groupCollector.initialise();

        for (RecordType r : get()) {
            groupBy.mapRecordToKey(r, groupCollector);
        }

        groupCollector.finalise();

        return groupCollector.getResult();
    }


    @Override
    public <KeyType, OutputRecordType, ResultType>
    Processable<Pair<KeyType, ResultType>>
    aggregate(
            RecordKeyValueMapper<KeyType, RecordType, OutputRecordType> groupBy,
            DataAggregator<KeyType, OutputRecordType, ResultType> aggregator) {

        AggregateCollector<KeyType, OutputRecordType, ResultType> aggregateCollector = new AggregateCollector<>();

        aggregateCollector.setAggregator(aggregator);
        aggregateCollector.initialise();

        ProgressReporter prg = new ProgressReporter(size(), "Aggregating");
        for (RecordType r : get()) {
            groupBy.mapRecordToKey(r, aggregateCollector);
            prg.incrementProgress();
            prg.report();
        }

        aggregateCollector.finalise();

        return aggregateCollector.getAggregationResult();
    }


    @Override
    public <KeyType extends Comparable<KeyType>>
    Processable<RecordType>
    sort(Function<KeyType, RecordType> sortingKey) {
        return sort(sortingKey, true);
    }


    @Override
    public <KeyType extends Comparable<KeyType>>
    Processable<RecordType>
    sort(
            final Function<KeyType, RecordType> sortingKey,
            final boolean ascending) {
        ArrayList<RecordType> list = new ArrayList<>(get());

        Collections.sort(list, new Comparator<RecordType>() {

            @Override
            public int compare(RecordType o1, RecordType o2) {
                return (ascending ? 1 : -1) * sortingKey.execute(o1).compareTo(sortingKey.execute(o2));
            }
        });

        Processable<RecordType> result = createProcessable(null);
        for (RecordType elem : list) {
            result.add(elem);
        }

        return result;
    }


    @Override
    public Processable<RecordType>
    where(Function<Boolean, RecordType> criteria) {
        Processable<RecordType> result = createProcessable(null);

        for (RecordType element : get()) {
            if (criteria.execute(element)) {
                result.add(element);
            }
        }

        return result;
    }


    @Override
    public <KeyType, RecordType2, OutputRecordType>
    Processable<OutputRecordType>
    coGroup(
            Processable<RecordType2> data2,
            final Function<KeyType, RecordType> groupingKeyGenerator1,
            final Function<KeyType, RecordType2> groupingKeyGenerator2,
            final RecordMapper<Pair<Iterable<RecordType>, Iterable<RecordType2>>, OutputRecordType> resultMapper) {
        Processable<Group<KeyType, RecordType>> group1 = group(new RecordKeyValueMapper<KeyType, RecordType, RecordType>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void mapRecordToKey(RecordType record, DataIterator<Pair<KeyType, RecordType>> resultCollector) {
                resultCollector.next(new Pair<KeyType, RecordType>(groupingKeyGenerator1.execute(record), record));
            }
        });

        Processable<Group<KeyType, RecordType2>> group2 = data2.group(new RecordKeyValueMapper<KeyType, RecordType2, RecordType2>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void mapRecordToKey(RecordType2 record, DataIterator<Pair<KeyType, RecordType2>> resultCollector) {
                resultCollector.next(new Pair<KeyType, RecordType2>(groupingKeyGenerator2.execute(record), record));
            }
        });

        Processable<Pair<Group<KeyType, RecordType>, Group<KeyType, RecordType2>>> joined = group1.join(group2, new Function<KeyType, Group<KeyType, RecordType>>() {


            private static final long serialVersionUID = 1L;

            @Override
            public KeyType execute(Group<KeyType, RecordType> input) {
                return input.getKey();
            }
        }, new Function<KeyType, Group<KeyType, RecordType2>>() {


            private static final long serialVersionUID = 1L;

            @Override
            public KeyType execute(Group<KeyType, RecordType2> input) {
                return (KeyType) input.getKey();
            }
        });

        return joined.map(new RecordMapper<Pair<Group<KeyType, RecordType>, Group<KeyType, RecordType2>>, OutputRecordType>() {


            private static final long serialVersionUID = 1L;

            @Override
            public void mapRecord(Pair<Group<KeyType, RecordType>, Group<KeyType, RecordType2>> record,
                                  DataIterator<OutputRecordType> resultCollector) {
                resultMapper.mapRecord(new Pair<Iterable<RecordType>, Iterable<RecordType2>>(record.getFirst().getRecords().get(), record.getSecond().getRecords().get()), resultCollector);
            }
        });
    }

    @Override
    public Processable<RecordType>
    append(Processable<RecordType> data2) {
        Processable<RecordType> result = createProcessable(null);

        result.addAll(get());
//		for(RecordType r : get()) {
//			result.add(r);
//		}

        if (data2 != null) {
//			for(RecordType r : data2.get()) {
//				result.add(r);
//			}
            result.addAll(data2.get());
        }

        return result;
    }


    @Override
    public Processable<RecordType>
    distinct() {
        return createProcessableFromCollection(new ArrayList<>(new HashSet<>(get())));
    }


    @Override
    public Processable<RecordType>
    take(int numberOfRecords) {
        Processable<RecordType> result = createProcessable(null);

        Iterator<RecordType> it = get().iterator();

        while (it.hasNext() && result.size() < numberOfRecords) {
            result.add(it.next());
        }

        return result;
    }
}
