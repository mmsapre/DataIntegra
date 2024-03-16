package com.integration.em.utils.mining;

import java.util.*;

public class SequencePatternMining<TData> {

    public static class Sequence<TData> {
        List<TData> elements;
        int count;
        public Sequence(List<TData> elements, int count) {
            this.elements = elements;
            this.count = count;
        }
        public void incrementCount() {
            count++;
        }
        public void append(List<TData> elements) {
            this.elements.addAll(elements);
        }
        public void append(TData element) {
            this.elements.add(element);
        }
        public int getSize() {
            return elements.size();
        }
        public List<TData> getElements() {
            return elements;
        }
        public int getCount() {
            return count;
        }

        protected Set<Sequence<TData>> createOneElementSequences(Collection<List<TData>> transactions) {
            Map<TData, Sequence<TData>> mapElementToSequence = new HashMap<>();

            for(List<TData> elements : transactions) {

                for(TData element : elements) {

                    Sequence<TData> seq = mapElementToSequence.get(element);

                    if(seq==null) {
                        List<TData> elem = new LinkedList<>();
                        elem.add(element);
                        seq = new Sequence<>(elem, 0);
                        mapElementToSequence.put(element, seq);
                    }

                }

            }

            return new HashSet<>(mapElementToSequence.values());
        }
        public boolean matches(List<TData> elements) {

            if(elements.containsAll(this.elements)) {

                int subseqStart = 0;
                int patternPos = 0;

                while(subseqStart+getSize()<elements.size()) {
                    if(elements.get(subseqStart+patternPos).equals(this.elements.get(patternPos))) {
                        patternPos++;

                        if(patternPos==this.elements.size()) {
                            return true;
                        }
                    } else {
                        subseqStart++;
                        patternPos=0;
                    }
                }
            }

            return false;
        }

        public int getMatchIndex(List<TData> elements) {

            if(elements.containsAll(this.elements)) {

                int subseqStart = 0;
                int patternPos = 0;

                while(subseqStart+getSize()<elements.size()) {
                    if(elements.get(subseqStart+patternPos).equals(this.elements.get(patternPos))) {
                        patternPos++;

                        if(patternPos==this.elements.size()) {
                            return subseqStart;
                        }
                    } else {
                        subseqStart++;
                        patternPos=0;
                    }
                }
            }

            return -1;
        }

        @SuppressWarnings("rawtypes")
        @Override
        public boolean equals(Object obj) {
            if(obj instanceof Sequence) {
                return elements.equals(((Sequence) obj).elements);
            }
            return super.equals(obj);
        }

        @Override
        public int hashCode() {
            return elements.hashCode();
        }
    }

    public static class SequentialRule<TData> {
        Sequence<TData> condition;
        Sequence<TData> consequent;
        int allElementsSupportCount;
        public SequentialRule(Sequence<TData> condition, Sequence<TData> consequent, int allElementsSupportCount) {
            this.condition = condition;
            this.consequent = consequent;
            this.allElementsSupportCount = allElementsSupportCount;
        }
        public double getConfidence() {
            return (double)allElementsSupportCount / (double)condition.getCount();
        }
        public Sequence<TData> getCondition() {
            return condition;
        }
        public Sequence<TData> getConsequent() {
            return consequent;
        }
        public int getAllElementsSupportCount() {
            return allElementsSupportCount;
        }
    }

    public Set<Sequence<TData>> createTwoElementSequences(Set<Sequence<TData>> oneElementSequences) {

        Set<Sequence<TData>> sequences = new HashSet<>();

        for(Sequence<TData> seq1 : oneElementSequences) {
            for(Sequence<TData> seq2 : oneElementSequences) {
                if(!seq1.equals(seq2)) {

                    Sequence<TData> mergedSeq = new Sequence<>(new LinkedList<>(seq1.elements), 0);
                    mergedSeq.append(seq2.elements);

                    sequences.add(mergedSeq);
                }
            }
        }

        return sequences;
    }


    protected Set<Sequence<TData>> mergeSequences(Set<Sequence<TData>> sequencesToMerge) {

        Set<Sequence<TData>> sequences = new HashSet<>();

        for(Sequence<TData> seq1 : sequencesToMerge) {

            List<TData> subseq1 = new ArrayList<>(seq1.elements);
            subseq1.remove(0);

            for(Sequence<TData> seq2 : sequencesToMerge) {

                List<TData> subseq2 = new ArrayList<>(seq2.elements);
                subseq2.remove(subseq2.size()-1);
                if(!seq1.equals(seq2) && subseq1.equals(subseq2)) {

                    Sequence<TData> mergedSeq = new Sequence<>(new LinkedList<>(seq1.elements), 0);
                    mergedSeq.append(seq2.elements.get(seq2.elements.size()-1));

                    sequences.add(mergedSeq);

                }
            }
        }

        return sequences;
    }

    public void calculateSupportCount(Set<Sequence<TData>> sequences, Collection<List<TData>> transactions) {

        for(List<TData> elements : transactions) {

            for(Sequence<TData> sequence : sequences) {

                if(sequence.matches(elements)) {
                    sequence.incrementCount();
                }

            }

        }

    }

    protected void pruneInfrequentSequences(int minSupportCount, Set<Sequence<TData>> sequences) {
        Iterator<Sequence<TData>> it = sequences.iterator();
        while(it.hasNext()) {
            if(it.next().count<minSupportCount) {
                it.remove();
            }
        }
    }

    public Set<Sequence<TData>> calculateSequentialPatterns(Collection<List<TData>> transactions) {
        Set<Sequence<TData>> allSequences = new HashSet<>();

        Set<Sequence<TData>> oneElementSequences = createOneElementSequences(transactions);
        calculateSupportCount(oneElementSequences, transactions);
        allSequences.addAll(oneElementSequences);

        Set<Sequence<TData>> twoElementSequences = createTwoElementSequences(oneElementSequences);
        calculateSupportCount(twoElementSequences, transactions);
        pruneInfrequentSequences(1, twoElementSequences);
        allSequences.addAll(twoElementSequences);

        Set<Sequence<TData>> lastSequences = twoElementSequences;
        Set<Sequence<TData>> newSequences = null;

        do {
            newSequences = mergeSequences(lastSequences);
            calculateSupportCount(newSequences, transactions);
            pruneInfrequentSequences(1, newSequences);
            allSequences.addAll(newSequences);
            lastSequences = newSequences;
        } while(lastSequences.size()>0);

        return allSequences;
    }

    public Set<SequentialRule<TData>> calculateSequentialRules(Set<Sequence<TData>> sequences) {

        Set<SequentialRule<TData>> rules = new HashSet<>();

        Map<List<TData>, Sequence<TData>> subsequenceToSequenceMap = new HashMap<>();

        for(Sequence<TData> sequence : sequences) {
            subsequenceToSequenceMap.put(sequence.getElements(), sequence);
        }

        for(Sequence<TData> sequence : sequences) {
            if(sequence.getSize()>1) {

                for(int i = sequence.getSize()-1; i>0; i--) {
                    List<TData> conditionElements = new LinkedList<>(sequence.getElements());

                    List<TData> consequentElements = new LinkedList<>();

                    do {
                        TData lastElement = conditionElements.remove(conditionElements.size()-1);

                        consequentElements.add(0, lastElement);

                    } while(conditionElements.size()>i);

                    Sequence<TData> condition = subsequenceToSequenceMap.get(conditionElements);
                    Sequence<TData> consequent = subsequenceToSequenceMap.get(consequentElements);

                    rules.add(new SequentialRule<>(condition, consequent, sequence.getCount()));

                }

            }
        }

        return rules;
    }

    protected Set<Sequence<TData>> createOneElementSequences(Collection<List<TData>> transactions) {
        Map<TData, Sequence<TData>> mapElementToSequence = new HashMap<>();

        for(List<TData> elements : transactions) {

            for(TData element : elements) {

                Sequence<TData> seq = mapElementToSequence.get(element);

                if(seq==null) {
                    List<TData> elem = new LinkedList<>();
                    elem.add(element);
                    seq = new Sequence<>(elem, 0);
                    mapElementToSequence.put(element, seq);
                }

            }

        }

        return new HashSet<>(mapElementToSequence.values());
    }
}
