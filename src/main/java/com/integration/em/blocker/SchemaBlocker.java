package com.integration.em.blocker;

import com.integration.em.blocker.generators.BlockingKeyGenerator;
import com.integration.em.model.Matchable;

public class SchemaBlocker<SchemaElementType extends Matchable, AlignerType extends Matchable>
        extends StandardBlocker<SchemaElementType, SchemaElementType, SchemaElementType, AlignerType> {

    public SchemaBlocker(
            BlockingKeyGenerator<SchemaElementType, AlignerType, SchemaElementType> blockingFunction) {
        super(blockingFunction);
    }

    public SchemaBlocker(
            BlockingKeyGenerator<SchemaElementType, AlignerType, SchemaElementType> blockingFunction,
            BlockingKeyGenerator<SchemaElementType, AlignerType, SchemaElementType> secondBlockingFunction) {
        super(blockingFunction, secondBlockingFunction);
    }
}
