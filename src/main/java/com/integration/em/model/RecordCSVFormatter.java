package com.integration.em.model;

import java.util.*;

public class RecordCSVFormatter extends CSVDataSetFormatter<Record, Attribute>{
    @Override
    public String[] getHeader(List<Attribute> orderedHeader) {
        List<String> names = new ArrayList<>();

        for (Attribute att : orderedHeader) {
            names.add(att.getIdentifier());
        }

        return names.toArray(new String[names.size()]);
    }

    @Override
    public String[] format(Record record, DataSet<Record, Attribute> dataset, List<Attribute> orderedHeader) {
        List<String> values = new ArrayList<>(dataset.getSchema().size());

        List<Attribute> names = orderAttributes(dataset, orderedHeader);

        for (Attribute name : names) {
            values.add(record.getValue(name));
        }

        return values.toArray(new String[values.size()]);
    }

    private List<Attribute> orderAttributes(DataSet<Record, Attribute> dataset, List<Attribute> orderedHeader) {
        List<Attribute> attributes = new ArrayList<>();

        if (orderedHeader == null) {
            for (Attribute elem : dataset.getSchema().get()) {
                attributes.add(elem);
            }

            Collections.sort(attributes, new Comparator<Attribute>() {

                @Override
                public int compare(Attribute o1, Attribute o2) {
                    return o1.toString().compareTo(o2.toString());
                }
            });
        } else {
            Collection<Attribute> schemaAtt = dataset.getSchema().get();

            for (int i = 0; i < orderedHeader.size(); i++) {
                Iterator<Attribute> schemaIter = schemaAtt.iterator();
                while (schemaIter.hasNext()) {
                    Attribute elem = schemaIter.next();
                    if (orderedHeader.get(i).equals(elem)) {
                        attributes.add(elem);
                    }
                }
            }
        }
        return attributes;
    }
}
