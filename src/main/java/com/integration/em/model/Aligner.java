package com.integration.em.model;

import com.integration.em.processing.Processable;
import com.integration.em.processing.ProcessableCollection;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Comparator;

@Slf4j
public class Aligner<RecordType extends Matchable, CausalType extends Matchable> implements Serializable {


    private Processable<Aligner<CausalType, Matchable>> causalAligners;
    private RecordType firstRecordType;
    private RecordType secondRecordType;
    private double similarityScore;
    private Object provenance;

    public Aligner() {

    }

    public Aligner(RecordType first, RecordType second, double similarityScore) {
        firstRecordType = first;
        secondRecordType = second;
        this.similarityScore = similarityScore;
    }

    public Aligner(RecordType first, RecordType second, double similarityScore, Processable<Aligner<CausalType, Matchable>> correspondences) {
        firstRecordType = first;
        secondRecordType = second;
        this.similarityScore = similarityScore;
        this.causalAligners = correspondences;
    }

    public Aligner(RecordType first, RecordType second, double similarityScore, Processable<Aligner<CausalType, Matchable>> correspondences, Object provenance) {
        firstRecordType = first;
        secondRecordType = second;
        this.similarityScore = similarityScore;
        this.causalAligners = correspondences;
        this.provenance = provenance;
    }

    public void setCausalAligners(Processable<Aligner<CausalType, Matchable>> causalAligners) {
        this.causalAligners = causalAligners;
    }

    public RecordType getFirstRecordType() {
        return firstRecordType;
    }

    public void setFirstRecordType(RecordType firstRecordType) {
        this.firstRecordType = firstRecordType;
    }

    public RecordType getSecondRecordType() {
        return secondRecordType;
    }

    public void setSecondRecordType(RecordType secondRecordType) {
        this.secondRecordType = secondRecordType;
    }

    public double getSimilarityScore() {
        return similarityScore;
    }

    public void setSimilarityScore(double similarityScore) {
        this.similarityScore = similarityScore;
    }

    public Object getProvenance() {
        return provenance;
    }

    public void setProvenance(Object provenance) {
        this.provenance = provenance;
    }

    public String getIdentifiers() {
        return String.format("%s/%s", getFirstRecordType().getIdentifier(), getSecondRecordType().getIdentifier());
    }

    public Processable<Aligner<CausalType, Matchable>> getCausalAligners() {
        return causalAligners;
    }

    public <T extends Matchable> Processable<Aligner<CausalType, T>> castCausalAligner() {
        Processable<Aligner<CausalType, T>> result = new ProcessableCollection<>();

        for(Aligner<CausalType, Matchable> cor : getCausalAligners().get()) {

            @SuppressWarnings("unchecked")
            Aligner<CausalType, T> cast = (Aligner<CausalType, T>) cor;

            result.add(cast);
        }

        return result;
    }

    public static <RecordType extends Matchable, SchemaElementType extends Matchable> Aligner<RecordType, SchemaElementType> combine(Aligner<RecordType, SchemaElementType> first, Aligner<RecordType, SchemaElementType> second) {
        Processable<Aligner<SchemaElementType, Matchable>> cors = new ProcessableCollection<>(first.getCausalAligners()).append(second.getCausalAligners());
        return new Aligner<>(first.getFirstRecordType(), second.getFirstRecordType(), first.getSimilarityScore() * second.getSimilarityScore(), cors);
    }

    public static class BySimilarityComparator<RecordType extends Matchable, CausalType extends Matchable> implements Comparator<Aligner<RecordType, CausalType>> {

        private boolean descending = false;

        public BySimilarityComparator() {
            this.descending = false;
        }

        public BySimilarityComparator(boolean descending) {
            this.descending = descending;
        }


        @Override
        public int compare(Aligner<RecordType, CausalType> o1, Aligner<RecordType, CausalType> o2) {
            int comp = Double.compare(o1.getSimilarityScore(), o2.getSimilarityScore());

            if(descending) {
                return -comp;
            } else {
                return comp;
            }
        }





    }

    public static <RecordType extends Matchable, CausalType extends Matchable> void changeDirection(Processable<Aligner<RecordType, CausalType>> alignerProcessable) {
        if(alignerProcessable==null) {
            return;
        } else {
            for(Aligner<RecordType, CausalType> cor : alignerProcessable.get()) {
                cor.changeDirection();
            }
        }
    }
    public void changeDirection() {
        RecordType tmp = getFirstRecordType();
        setFirstRecordType(getSecondRecordType());
        setSecondRecordType(tmp);

        for(Aligner<CausalType, Matchable> cor : getCausalAligners().get()) {
            cor.changeDirection();
        }
    }

    public static <RecordType extends Matchable, CausalType extends Matchable> void setDirectionByDataSourceIdentifier(Processable<Aligner<RecordType, CausalType>> alignerProcessable) {
        if(alignerProcessable==null) {
            return;
        } else {
            for(Aligner<RecordType, CausalType> cor : alignerProcessable.get()) {
                cor.setDirectionByDataSourceIdentifier();
            }
        }
    }

    public void setDirectionByDataSourceIdentifier() {

        if(getFirstRecordType().getDataSourceIdentifier()>getSecondRecordType().getDataSourceIdentifier()) {
            RecordType tmp = getFirstRecordType();
            setFirstRecordType(getSecondRecordType());
            setSecondRecordType(tmp);
        }

        if(getCausalAligners()!=null) {
            for(Aligner<CausalType, Matchable> cor : getCausalAligners().get()) {
                cor.setDirectionByDataSourceIdentifier();
            }
        }
    }

    public static <RecordType extends Matchable, AlgType extends Aligner<RecordType, ? extends Matchable>> Processable<Aligner<RecordType, Matchable>> toMatchable(Processable<AlgType> processables) {
        if(processables==null) {
            return null;
        } else {
            Processable<Aligner<RecordType, Matchable>> result = processables.createProcessable((Aligner<RecordType, Matchable>)null);
            for(AlgType algType : processables.get()) {

                Aligner<RecordType, Matchable> simple = new Aligner<RecordType, Matchable>(algType.getFirstRecordType(), algType.getSecondRecordType(), algType.getSimilarityScore(), toMatchable2(algType.getCausalAligners()));
                result.add(simple);
            }
            return result;
        }
    }

    public static <RecordType extends Matchable, AlgType extends Aligner<RecordType, Matchable>> Processable<Aligner<Matchable, Matchable>> toMatchable2(Processable<AlgType> processables) {
        if(processables==null) {
            return null;
        } else {
            Processable<Aligner<Matchable, Matchable>> result = new ProcessableCollection<>();
            for(AlgType algType : processables.get()) {

                Aligner<Matchable, Matchable> simpler = new Aligner<Matchable, Matchable>(algType.getFirstRecordType(), algType.getSecondRecordType(), algType.getSimilarityScore(), algType.getCausalAligners());

                result.add(simpler);
            }
            return result;
        }
    }
    public boolean equals(Object obj) {
        if(obj instanceof Aligner<?,?>) {
            Aligner<?,?> cor2 = (Aligner<?,?>)obj;
            return getFirstRecordType().equals(cor2.getFirstRecordType()) && getSecondRecordType().equals(cor2.getSecondRecordType());
        } else {
            return super.equals(obj);
        }
    }

    @Override
    public int hashCode() {
        return 997 * (getFirstRecordType().hashCode()) ^ 991 * (getSecondRecordType().hashCode());
    }

    public static class RecordId implements Matchable {

        private String identifier;

        @Override
        public String getIdentifier() {
            return identifier;
        }


        public void setIdentifier(String identifier) {
            this.identifier = identifier;
        }


        @Override
        public String getProvenance() {
            return null;
        }

        public RecordId(String identifier) {
            this.identifier = identifier;
        }

        @Override
        public String toString() {
            return identifier;
        }

        @Override
        public int hashCode() {
            return identifier.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof RecordId) {
                return identifier.equals(((RecordId)obj).getIdentifier());
            } else {
                return false;
            }
        }
    }
}
