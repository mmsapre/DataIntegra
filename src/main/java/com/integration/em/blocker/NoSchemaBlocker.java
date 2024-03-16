package com.integration.em.blocker;

import com.integration.em.blocker.generators.StaticBlockingKeyGenerator;
import com.integration.em.model.Matchable;

public class NoSchemaBlocker<RecordType extends Matchable, SchemaElementType extends Matchable> extends
        StandardBlocker<RecordType, SchemaElementType, RecordType, SchemaElementType> {

    public NoSchemaBlocker() {
        super(new StaticBlockingKeyGenerator<RecordType, SchemaElementType>());
    }

}
