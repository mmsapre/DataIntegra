package com.integration.em.merge;

import com.integration.em.model.Matchable;
import com.integration.em.model.MixValue;
import com.integration.em.model.Mixed;
import com.integration.em.model.MixedValue;

import java.util.Collection;

public abstract class ConflictResolutionFunction<ValueType, RecordType extends Matchable & Mixed<SchemaElementType>, SchemaElementType extends Matchable> {

    public abstract MixedValue<ValueType, RecordType, SchemaElementType> resolveConflict(Collection<MixValue<ValueType, RecordType, SchemaElementType>> values);

}
