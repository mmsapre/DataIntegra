package com.integration.em.processing;

import com.integration.em.tables.Pair;
import com.integration.em.utils.Function;

import java.io.Serializable;
import java.util.Collection;

public interface Processable<RecordType> extends Serializable {

    void add(RecordType element);
    void addAll(Collection<RecordType> elements);
    Collection<RecordType> get();
    int size();

    void remove(RecordType element);
    void remove(Collection<RecordType> element);

    Processable<RecordType> copy();

    RecordType firstOrNull();

    <OutputRecordType> Processable<OutputRecordType> createProcessable(OutputRecordType dummyForTypeInference);

    <OutputRecordType> Processable<OutputRecordType> createProcessableFromCollection(Collection<OutputRecordType> data);

    Processable<RecordType> assignUniqueRecordIds(Function<RecordType, Pair<Long, RecordType>> assignUniqueId);

    void foreach(DataIterator<RecordType> iterator);

    void foreach(Action<RecordType> action);

    <OutputRecordType> Processable<OutputRecordType> map(RecordMapper<RecordType, OutputRecordType> transformation);

    <OutputRecordType> Processable<OutputRecordType> map(Function<OutputRecordType, RecordType> transformation);

    <KeyType> Processable<Pair<RecordType, RecordType>> symmetricJoin(Function<KeyType, RecordType> joinKeyGenerator);


    <KeyType> Processable<Pair<RecordType, RecordType>> symmetricJoin(Function<KeyType, RecordType> joinKeyGenerator, ProcessableCollector<Pair<RecordType, RecordType>> collector);


    <KeyType> Processable<Pair<RecordType, RecordType>> join(Processable<RecordType> dataset2, Function<KeyType, RecordType> joinKeyGenerator);


    <KeyType, RecordType2> Processable<Pair<RecordType, RecordType2>> join(Processable<RecordType2> dataset2,
                                                                           Function<KeyType, RecordType> joinKeyGenerator1, Function<KeyType, RecordType2> joinKeyGenerator2);

    <KeyType> Processable<Pair<RecordType, RecordType>> leftJoin(Processable<RecordType> dataset2,
                                                                 Function<KeyType, RecordType> joinKeyGenerator);


    <KeyType, RecordType2> Processable<Pair<RecordType, RecordType2>> leftJoin(Processable<RecordType2> dataset2,
                                                                               Function<KeyType, RecordType> joinKeyGenerator1, Function<KeyType, RecordType2> joinKeyGenerator2);

    <KeyType, OutputRecordType> Processable<Group<KeyType, OutputRecordType>> group(RecordKeyValueMapper<KeyType, RecordType, OutputRecordType> groupBy);


    <KeyType, OutputRecordType, ResultType> Processable<Pair<KeyType, ResultType>> aggregate(
            RecordKeyValueMapper<KeyType, RecordType, OutputRecordType> groupBy,
            DataAggregator<KeyType, OutputRecordType, ResultType> aggregator);


    <KeyType extends Comparable<KeyType>> Processable<RecordType> sort(Function<KeyType, RecordType> sortingKey);

    <KeyType extends Comparable<KeyType>> Processable<RecordType> sort(Function<KeyType, RecordType> sortingKey, boolean ascending);


    Processable<RecordType> where(Function<Boolean, RecordType> criteria);


    <KeyType, RecordType2, OutputRecordType> Processable<OutputRecordType> coGroup(
            Processable<RecordType2> data2,
            Function<KeyType, RecordType> groupingKeyGenerator1, Function<KeyType, RecordType2> groupingKeyGenerator2,
            RecordMapper<Pair<Iterable<RecordType>, Iterable<RecordType2>>, OutputRecordType> resultMapper);


    Processable<RecordType> append(Processable<RecordType> data2);


    Processable<RecordType> distinct();


    Processable<RecordType> take(int numberOfRecords);

}
