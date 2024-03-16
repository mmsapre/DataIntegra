package com.integration.em.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Record extends AbstractRecord<Attribute> {

    private Map<Attribute, String> values;
    private Map<Attribute, List<String>> lists;

    public Record(String identifier) {
        super(identifier, "");
        values = new HashMap<>();
        lists = new HashMap<>();
    }

    public String getValue(Attribute attribute) {
        return values.get(attribute);
    }

    public void setValue(Attribute attribute, String value) {
        values.put(attribute, value);
    }

    public Record(String identifier, String provenance) {
        super(identifier, provenance);
        values = new HashMap<>();
        lists = new HashMap<>();
    }

    public Map<Attribute, String> getValues() {
        return values;
    }

    public void setValues(Map<Attribute, String> values) {
        this.values = values;
    }

    public Map<Attribute, List<String>> getLists() {
        return lists;
    }

    public void setLists(Map<Attribute, List<String>> lists) {
        this.lists = lists;
    }

    @Override
    public boolean hasValue(Attribute attribute) {
        return (values.containsKey(attribute) && values.get(attribute)!=null)
                || (lists.containsKey(attribute) && lists.get(attribute)!=null);
    }

    @Override
    public String toString() {
        return values.toString();
    }
}
