package com.integration.em.rules.compare;

import com.integration.em.model.Aligner;
import com.integration.em.model.Matchable;

import java.io.Serializable;

public interface IComparator<RecordType extends Matchable, SchemaElementType extends Matchable> extends Serializable {

    double compare(RecordType record1, RecordType record2,
                   Aligner<SchemaElementType, Matchable> schemaAligner);

    default boolean hasMissingValue(RecordType record1, RecordType record2,
                                    Aligner<SchemaElementType, Matchable> schemaAligner) { return false; };

    default SchemaElementType getFirstSchemaElement(RecordType record) {
        return null;
    }

    default SchemaElementType getSecondSchemaElement(RecordType record) {
        return null;
    }

    default IComparatorLogger getComparisonLog() {
        return null;
    }

    default void setComparisonLog(IComparatorLogger iComparatorLogger)
    {
    }

    default String getName(Aligner<SchemaElementType, Matchable> schemaAligner)
    {
        return this.getClass().getSimpleName();
    }


}
