package com.integration.em.matches;

import com.integration.em.similarity.MatrixSimilarity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class BestMatch extends MatrixMatching{

    private boolean forceOneToOneMapping = true;

    public boolean isForceOneToOneMapping() {
        return forceOneToOneMapping;
    }

    public void setForceOneToOneMapping(boolean forceOneToOneMapping) {
        this.forceOneToOneMapping = forceOneToOneMapping;
    }

    public <T extends Comparable<? super T>> MatrixSimilarity<T> match(MatrixSimilarity<T> matrixSimilarity) {

        MatrixSimilarity<T> sim = getMatrixSimilarityFactory().createMatrixSimilarity(matrixSimilarity.getFirstDim().size(), matrixSimilarity.getSecondDim().size());

        Set<T> alreadyMatched = new HashSet<T>();

        ArrayList<T> dimension = new ArrayList<>(matrixSimilarity.getFirstDim());
        Collections.sort(dimension);
        for(T instance : dimension) {

            double max = 0.0;
            T best = null;
            ArrayList<T> matches = new ArrayList<>(matrixSimilarity.getMatches(instance));
            Collections.sort(matches);
            for(T candidate : matches) {

                if(!alreadyMatched.contains(candidate) && matrixSimilarity.get(instance, candidate)>max) {
                    max = matrixSimilarity.get(instance, candidate);
                    best = candidate;
                }

            }

            for(T instance2 : matrixSimilarity.getFirstDim()) {

                if(instance2!=instance && !alreadyMatched.contains(instance2) && matrixSimilarity.get(instance2, best)!=null && matrixSimilarity.get(instance2, best)>max) {
                    best = null;
                    break;
                }

            }

            if(best!=null) {
                sim.set(instance, best, max);

                if(isForceOneToOneMapping()) {
                    alreadyMatched.add(instance);
                    alreadyMatched.add(best);
                }
            }
        }

        return sim;
    }
}
