
package com.integration.em.utils;


import com.integration.em.model.Aligner;
import com.integration.em.model.Matchable;

import java.io.Serializable;

public interface Comparator<RecordType extends Matchable, SchemaElementType extends Matchable> extends Serializable {

	double compare(RecordType record1, RecordType record2,
			Aligner<SchemaElementType, Matchable> schemaElementTypeMatchableAligner);

	default boolean hasMissingValue(RecordType record1, RecordType record2,
				   Aligner<SchemaElementType, Matchable> schemaElementTypeMatchableAligner) { return false; };


	default SchemaElementType getFirstSchemaElement(RecordType record) {
		return null;
	}


	default SchemaElementType getSecondSchemaElement(RecordType record) {
		return null;
	}


	default ComparatorLogger getComparisonLog() {
		return null;
	}


	default void setComparisonLog(ComparatorLogger comparatorLog) 
	{
	}
	

	default String getName(Aligner<SchemaElementType, Matchable> schemaElementTypeMatchableAligner)
	{
		return this.getClass().getSimpleName();
	}
}
