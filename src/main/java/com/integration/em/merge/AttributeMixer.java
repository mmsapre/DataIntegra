package com.integration.em.merge;

import com.integration.em.model.Aligner;
import com.integration.em.model.Matchable;
import com.integration.em.model.Mixed;
import com.integration.em.model.RecordGroup;
import com.integration.em.processing.Processable;

public abstract class AttributeMixer<RecordType extends Matchable & Mixed<SchemaElementType>, SchemaElementType extends Matchable> {

    private AttributeMixedLogger attributeMixedLogger;

    private boolean debugResults;

    public AttributeMixedLogger getAttributeMixedLogger() {
        return attributeMixedLogger;
    }

    public void setAttributeMixedLogger(AttributeMixedLogger attributeMixedLogger) {
        this.attributeMixedLogger = attributeMixedLogger;
    }

    public boolean isDebugResults() {
        return debugResults;
    }

    public void setDebugResults(boolean debugResults) {
        this.debugResults = debugResults;
    }

    public abstract void merge(RecordGroup<RecordType, SchemaElementType> group, RecordType recordYpe, Processable<Aligner<SchemaElementType, Matchable>> alignerProcessable, SchemaElementType schemaElement);

    public abstract boolean hasValue(RecordType record, Aligner<SchemaElementType, Matchable> correspondence);


    public abstract Double getConsistency(RecordGroup<RecordType, SchemaElementType> group, EvaluationRule<RecordType, SchemaElementType> rule, Processable<Aligner<SchemaElementType, Matchable>> schemaCorrespondences, SchemaElementType schemaElement);

}
