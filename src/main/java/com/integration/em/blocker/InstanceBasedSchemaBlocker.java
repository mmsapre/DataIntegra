package com.integration.em.blocker;

import com.integration.em.blocker.generators.BlockingKeyGenerator;
import com.integration.em.model.Matchable;
import com.integration.em.model.MatchableValue;

public class InstanceBasedSchemaBlocker<RecordType extends Matchable, SchemaElementType extends Matchable>
        extends ValueBasedBlocker<RecordType, SchemaElementType, SchemaElementType>{

    public InstanceBasedSchemaBlocker(BlockingKeyGenerator<RecordType, MatchableValue, SchemaElementType> blockingFunction) {
        super(blockingFunction);
    }

    public InstanceBasedSchemaBlocker(BlockingKeyGenerator<RecordType, MatchableValue, SchemaElementType> blockingFunction, BlockingKeyGenerator<RecordType, MatchableValue, SchemaElementType> secondBlockingFunction) {
        super(blockingFunction, secondBlockingFunction);
    }

}
