package com.integration.em.merge;

import com.integration.em.model.Aligner;
import com.integration.em.model.Matchable;

public abstract class EvaluationRule<RecordType extends Matchable, SchemaElementType extends Matchable> {

    public abstract boolean isEqual(RecordType record1, RecordType record2, SchemaElementType schemaElement);

    public abstract boolean isEqual(RecordType record1, RecordType record2, Aligner<SchemaElementType, Matchable> schemaAligner);

}
