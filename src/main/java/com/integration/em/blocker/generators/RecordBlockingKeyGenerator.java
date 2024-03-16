package com.integration.em.blocker.generators;

import com.integration.em.model.Matchable;

public abstract class RecordBlockingKeyGenerator<RecordType extends Matchable, AlignerType extends Matchable> 	extends BlockingKeyGenerator<RecordType, AlignerType, RecordType>
{
}
