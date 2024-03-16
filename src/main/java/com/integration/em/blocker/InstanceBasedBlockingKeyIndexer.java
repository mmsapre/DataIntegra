package com.integration.em.blocker;

import com.integration.em.blocker.generators.BlockingKeyGenerator;
import com.integration.em.model.Aligner;
import com.integration.em.model.Matchable;
import com.integration.em.model.MatchableValue;
import com.integration.em.processing.Processable;
import com.integration.em.processing.ProcessableCollection;
import com.integration.em.similarity.VectorSpaceSimilarity;

public class InstanceBasedBlockingKeyIndexer<RecordType extends Matchable, SchemaElementType extends Matchable, BlockedType extends Matchable>
        extends BlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, MatchableValue> {


    public InstanceBasedBlockingKeyIndexer(BlockingKeyGenerator<RecordType, MatchableValue, BlockedType> blockedTypeBlockingKeyGenerator, BlockingKeyGenerator<RecordType, MatchableValue, BlockedType> secondBlockingFunction, VectorSpaceSimilarity similarityFunction, VectorCreationMethod vectorCreationMethod, double similarityThreshold) {
        super(blockedTypeBlockingKeyGenerator, secondBlockingFunction, similarityFunction, vectorCreationMethod, similarityThreshold);
    }


    @Override
    protected Processable<Aligner<MatchableValue, Matchable>> createCausalAligners(BlockedType record1,
                                                                                   BlockedType record2,
                                                                                   BlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, MatchableValue>.BlockingVector vector1,
                                                                                   BlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, MatchableValue>.BlockingVector vector2) {

        Processable<Aligner<MatchableValue, Matchable>> causes = new ProcessableCollection<>();

        for(String s : vector1.keySet()) {
            Double v1 = vector1.get(s);
            Double v2 = vector2.get(s);

            if(v2!=null) {
                MatchableValue v = new MatchableValue(s, "", "");
                Aligner<MatchableValue, Matchable> cor = new Aligner<>(v, v, getVectorSpaceSimilarity().calculateDimensionScore(v1, v2));
                causes.add(cor);
            }
        }

        return causes;

    }
}
