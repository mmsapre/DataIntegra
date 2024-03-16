package com.integration.em.merge;

import com.integration.em.model.Aligner;
import com.integration.em.model.Matchable;
import com.integration.em.model.Mixed;
import com.integration.em.model.RecordGroup;
import com.integration.em.processing.Processable;

public class AttributeMixedTask<RecordType extends Matchable & Mixed<SchemaElementType>, SchemaElementType extends Matchable> {

    private SchemaElementType schemaElement;
    private AttributeMixer<RecordType, SchemaElementType> attributeMixer;
    private Processable<Aligner<SchemaElementType, Matchable>> alignerProcessable;
    private EvaluationRule<RecordType, SchemaElementType> evaluationRule;

    public SchemaElementType getSchemaElement() {
        return schemaElement;
    }

    public void setSchemaElement(SchemaElementType schemaElement) {
        this.schemaElement = schemaElement;
    }

    public AttributeMixer<RecordType, SchemaElementType> getAttributeMixer() {
        return attributeMixer;
    }

    public void setAttributeMixer(AttributeMixer<RecordType, SchemaElementType> attributeMixer) {
        this.attributeMixer = attributeMixer;
    }

    public Processable<Aligner<SchemaElementType, Matchable>> getAlignerProcessable() {
        return alignerProcessable;
    }

    public void setAlignerProcessable(Processable<Aligner<SchemaElementType, Matchable>> alignerProcessable) {
        this.alignerProcessable = alignerProcessable;
    }

    public EvaluationRule<RecordType, SchemaElementType> getEvaluationRule() {
        return evaluationRule;
    }

    public void setEvaluationRule(EvaluationRule<RecordType, SchemaElementType> evaluationRule) {
        this.evaluationRule = evaluationRule;
    }

    public void execute(RecordGroup<RecordType,SchemaElementType> recordGroup,RecordType recordType){
        attributeMixer.merge(recordGroup,recordType,alignerProcessable,schemaElement);
    }
}
