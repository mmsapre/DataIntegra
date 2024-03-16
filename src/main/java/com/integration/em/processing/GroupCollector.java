package com.integration.em.processing;


import com.integration.em.tables.Pair;

import java.io.Serializable;
import java.util.HashMap;

public class GroupCollector<KeyType,RecordType> implements DataIterator<Pair<KeyType, RecordType>>, Serializable {

    private HashMap<KeyType, Processable<RecordType>> groups;
    private Processable<Group<KeyType, RecordType>> result;


    public Processable<Group<KeyType, RecordType>> getResult() {
        return result;
    }

    @Override
    public void initialise() {
        groups = new HashMap<>();
        result = new ProcessableCollection<>();
    }

    @Override
    public void next(Pair<KeyType, RecordType> record) {
        Processable<RecordType> list = groups.get(record.getFirst());
        if(list==null) {
            list = new ProcessableCollection<>();
            groups.put(record.getFirst(), list);
        }

        list.add(record.getSecond());
    }

    @Override
    public void finalise() {
        for(KeyType key : groups.keySet()) {
            Group<KeyType, RecordType> g = new Group<>(key, groups.get(key));
            result.add(g);
        }
    }
}
