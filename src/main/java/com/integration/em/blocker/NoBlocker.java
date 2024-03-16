package com.integration.em.blocker;

import com.integration.em.blocker.generators.StaticBlockingKeyGenerator;
import com.integration.em.model.Matchable;

public class NoBlocker<RecordType extends Matchable, SchemaElementType extends Matchable> extends
        StandardBlocker<RecordType, SchemaElementType, RecordType, SchemaElementType> {

    public NoBlocker() {
        super(new StaticBlockingKeyGenerator<RecordType, SchemaElementType>());
    }

}
