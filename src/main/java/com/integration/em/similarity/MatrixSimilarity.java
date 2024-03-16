package com.integration.em.similarity;

import com.integration.em.model.Aligner;
import com.integration.em.model.Matchable;
import com.integration.em.utils.Func;

import java.util.*;

public abstract class MatrixSimilarity<T> {

    public abstract Double get(T first, T second);

    public abstract void set(T first, T second, Double similarity);

    public abstract Collection<T> getFirstDim();

    public abstract Collection<T> getSecondDim();

    public abstract Collection<T> getMatches(T first);

    public abstract Collection<T> getMatchesAboveThreshold(T first,double similarityThreshold);


    public void add(T first, T second, Double value) {
        Double existing = get(first, second);

        if(existing==null) {
            existing = 0.0;
        }

        set(first, second, existing + value);
    }

    public void normalize(double normalizingFactor) {
        for (T value : getFirstDim()) {
            for (T secondValue : getMatches(value)) {
                double d = get(value, secondValue) / normalizingFactor;
                set(value, secondValue, d);
            }
        }
    }

    public void multiplyScalar(double scalar) {
        if(scalar==1.0) {
            return;
        }
        for (T value : getFirstDim()) {
            for (T secondValue : getMatches(value)) {
                double d = get(value, secondValue) * scalar;
                set(value, secondValue, d);
            }
        }
    }

    public void normalize() {

        Double max = getMaxValue();

        if(max!=null) {
            normalize(max);
        }
    }

    public void makeStochastic() {
        double sum = 0.0;

        for(T row : getFirstDim()) {
            for(T col : getMatches(row)) {
                if(get(row, col)!=null) {
                    sum += get(row, col);
                }

            }
        }
        for(T row : getFirstDim()) {
            for(T col : getMatches(row)) {
                if(get(row, col)!=null) {
                    set(row, col, get(row, col)/sum);
                }

            }
        }
    }

    public void makeRowStochastic() {
        for(T row : getFirstDim()) {
            double sum = 0.0;

            for(T col : getMatches(row)) {
                sum += get(row, col);
            }

            for(T col : getMatches(row)) {
                set(row, col, get(row, col) / sum);
            }
        }
    }


    public void makeColumnStochastic() {
        for(T col : getSecondDim()) {
            double sum = 0.0;
            for(T row : getFirstDim()) {
                sum += get(row, col);
            }
            for(T row : getFirstDim()) {
                set(row, col, get(row, col) / sum);

            }
        }

    }

    public void invert() {
        for(T first : getFirstDim()) {
            for(T second : getMatches(first)) {
                Double sim = get(first, second);
                if(sim!=null && sim > 0.0) {
                    set(first, second, 1.0/sim);
                }
            }

        }
    }

    public MatrixSimilarity<T> makeBinary(double threshold) {
        for(T first : getFirstDim()) {
            for(T second : getMatches(first)) {
                if(get(first, second)>threshold) {
                    set(first, second, 1.0);
                } else {
                    set(first, second, 0.0);
                }
            }

        }

        return this;
    }

    public Collection<Double> getRowSums() {

        ArrayList<Double> sums = new ArrayList<Double>(getFirstDim().size());
        for(T row : getFirstDim()) {
            double sum = 0.0;

            for(T col : getMatches(row)) {
                sum += get(row, col);
            }
            sums.add(sum);
        }
        return sums;
    }

    public Collection<Double> getColSums() {

        ArrayList<Double> sums = new ArrayList<Double>(getSecondDim().size());
        for(T col : getSecondDim()) {
            double sum = 0.0;
            for(T row : getFirstDim()) {

                Double d = get(row, col);

                if(d!=null) {
                    sum += get(row, col);
                }
            }

            sums.add(sum);
        }

        return sums;
    }

    public Double getSum() {
        double sum = 0.0;
        for(Double d : getColSums()) {
            sum += d;
        }
        return sum;
    }

    public void prune(double belowThreshold) {
        for(T first : getFirstDim()) {
            for(T second : getMatches(first)) {
                if(get(first, second)<belowThreshold) {
                    set(first, second, null);
                }
            }
        }

    }

    public int getNumberOfElements() {
        return getFirstDim().size() * getSecondDim().size();
    }

    public int getNumberOfNonZeroElements() {
        int i = 0;
        for (T value : getFirstDim()) {
            i += getMatchesAboveThreshold(value, 0.0).size();
        }
        return i;
    }

    private HashMap<T, Object> labels = new HashMap<T, Object>();

    public Object getLabel(T instance) {
        if (labels.containsKey(instance)) {
            return labels.get(instance);
        } else {
            return "";
        }
    }

    public void setLabel(T instance, Object label) {
        labels.put(instance, label);
    }

    protected String padRight(String s, int n) {
        if(n==0) {
            return "";
        }
        if (s.length() > n) {
            s = s.substring(0, n);
        }
        s = s.replace("\n", " ");
        return String.format("%1$-" + n + "s", s);
    }

    protected static String padLeft(String s, int n) {
        if(n==0) {
            return "";
        }
        if (s.length() > n) {
            s = s.substring(0, n);
        }
        s = s.replace("\n", " ");
        return String.format("%1$" + n + "s", s);
    }

    public String getOutput() {
        return getOutput(null, null);
    }

    public String getOutput(int colWidth) {
        return getOutput(null, null, colWidth);
    }

    public <U> String getOutput(Collection<Object> filterFirstDimension, Collection<Object> filterSecondDimension) {
        return getOutput(filterFirstDimension, filterSecondDimension, 20);
    }

    public <U> String getOutput(Collection<Object> filterFirstDimension, Collection<Object> filterSecondDimension, int colWidth) {
        String output = "";
        output = getFirstDim().size() + " x "
                + getSecondDim().size();

        Collection<T> firstDim = new ArrayList<T>(getFirstDim().size());

        for(T inst : getFirstDim()) {
            firstDim.add(inst);
        }

        Collection<T> secondDim = new ArrayList<T>(getSecondDim().size());

        for(T inst : getSecondDim()) {
            secondDim.add(inst);
        }

        if(filterFirstDimension!=null) {
            Iterator<T> it = firstDim.iterator();
            while(it.hasNext()) {
                T current = it.next();

                if(!filterFirstDimension.contains(current)) {
                    it.remove();
                }
            }

            // add more elements till we reach 100
            it = getFirstDim().iterator();
            while(firstDim.size()<100 && it.hasNext()) {
                T current = it.next();

                if(!filterFirstDimension.contains(current)) {
                    firstDim.add(current);
                }
            }
        }

        if(filterSecondDimension!=null) {
            Iterator<T> it = secondDim.iterator();
            while(it.hasNext()) {
                T current = it.next();

                if(!filterSecondDimension.contains(current)) {
                    it.remove();
                }
            }

            it = getSecondDim().iterator();
            while(secondDim.size()<100 && it.hasNext()) {
                T current = it.next();

                if(!filterSecondDimension.contains(current)) {
                    secondDim.add(current);
                }
            }
        }

        if(firstDim.size()>100) {
            output += "First  too large, showing only the first 100 elements.";

            Collection<T> tmp = new ArrayList<T>(100);
            Iterator<T> it = firstDim.iterator();
            for(int i=0;i<100;i++) {
                tmp.add(it.next());
            }

            firstDim = tmp;
        }

        if(secondDim.size()>100) {
            output += "Second  too large, showing only the first 100 elements.";

            Collection<T> tmp = new ArrayList<T>(100);
            Iterator<T> it = secondDim.iterator();
            for(int i=0;i<100;i++) {
                tmp.add(it.next());
            }
            secondDim = tmp;
        }


        LinkedList<Integer> columnWidths = new LinkedList<Integer>();

        // measure size of columns
        if (labels.size() > 0) {

            int max = 0;
            for (T value : firstDim) {
                if(labels.get(value)!=null) {
                    max = Math.max(labels.get(value).toString().length(), max);
                }
            }
            columnWidths.add(Math.min(max, colWidth));

            max = 0;
            for (T value : firstDim) {
                max = Math.max(value.toString().length(), max);
            }
            columnWidths.add(Math.min(max, colWidth));

            for (T value : secondDim) {
                int lblLength = 0;
                if(getLabel(value)!=null) {
                    lblLength = getLabel(value).toString().length();
                }
                columnWidths.add(Math.min(Math.max(lblLength, value.toString().length()), colWidth));
            }
        }
        else
        {
            int max = 0;
            for (T value : firstDim) {
                if(value!=null) {
                    max = Math.max(value.toString().length(), max);
                }
            }
            columnWidths.add(Math.min(max, colWidth));

            for (T value : secondDim) {
                if(value==null) {
                    columnWidths.add(colWidth);
                } else {
                    columnWidths.add(Math.min(value.toString().length(), colWidth));
                }
            }
        }

        output += "\n";

        if (labels.size() > 0) {
            // print second dimension labels
            output += padLeft("", columnWidths.get(0)) + " | ";
            output += padLeft("", columnWidths.get(1)) + " | ";
            int i=2;
            for (T value : secondDim) {
                output += padLeft(getLabel(value).toString(), columnWidths.get(i++)) + " | ";
            }
            output += "\n";
            output += padLeft("", columnWidths.get(0)) + " | ";
            output += padLeft("", columnWidths.get(1)) + " | ";
        }
        else {
            output += padLeft("", columnWidths.get(0)) + " | ";
        }

        // print second dimension values
        int colIdx = labels.size()>0 ? 2 : 1;
        for (T value : secondDim) {
            output += padLeft(value + "", columnWidths.get(colIdx++)) + " | ";
        }
        output += "\n";

        // print first dimension labels & values
        colIdx=0;
        for (T value : firstDim) {
            if (labels.size() > 0) {
                output += padLeft(getLabel(value)+"", columnWidths.get(0)) + " | ";
                colIdx=1;
            }
            output += padLeft(value + "", columnWidths.get(colIdx)) + " | ";

            // print scores
            int colIdx2 = colIdx+1;
            for (T secondValue : secondDim) {
                if (get(value, secondValue) == null) {
                    output += padRight("", columnWidths.get(colIdx2++)) + " | ";
                } else {
                    output += padRight(
                            String.format("%1$,.8f", get(value, secondValue)),
                            columnWidths.get(colIdx2++)) + " | ";
                }
            }
            output += "\n";
        }
        return output;
    }

    public String getOutput2(Collection<Object> filterFirstDimension, Collection<Object> filterSecondDimension) {
        String output = "";
        output = getFirstDim().size() + " x " + getSecondDim().size();

        Collection<T> firstDim = new ArrayList<T>(getFirstDim().size());

        for(T inst : getFirstDim()) {
            firstDim.add(inst);
        }

        Collection<T> secondDim = new ArrayList<T>(getSecondDim().size());

        for(T inst : getSecondDim()) {
            secondDim.add(inst);
        }

        if(filterFirstDimension!=null) {
            Iterator<T> it = firstDim.iterator();
            while(it.hasNext()) {
                T current = it.next();

                if(!filterFirstDimension.contains(current)) {
                    it.remove();
                }
            }

            // add more elements till we reach 100
            it = getFirstDim().iterator();
            while(firstDim.size()<100 && it.hasNext()) {
                T current = it.next();

                if(!filterFirstDimension.contains(current)) {
                    firstDim.add(current);
                }
            }
        }

        if(filterSecondDimension!=null) {
            Iterator<T> it = secondDim.iterator();
            while(it.hasNext()) {
                T current = it.next();

                if(!filterSecondDimension.contains(current)) {
                    it.remove();
                }
            }

            it = getSecondDim().iterator();
            while(secondDim.size()<100 && it.hasNext()) {
                T current = it.next();

                if(!filterSecondDimension.contains(current)) {
                    secondDim.add(current);
                }
            }
        }

        if(firstDim.size()>100) {
            output += "First dimension too large, showing only the first 100 elements.";

            Collection<T> tmp = new ArrayList<T>(100);
            Iterator<T> it = firstDim.iterator();
            for(int i=0;i<100;i++) {
                tmp.add(it.next());
            }

            firstDim = tmp;
        }

        if(secondDim.size()>100) {
            output += "Second dimension too large, showing only the first 100 elements.";

            Collection<T> tmp = new ArrayList<T>(100);
            Iterator<T> it = secondDim.iterator();
            for(int i=0;i<100;i++) {
                tmp.add(it.next());
            }
            secondDim = tmp;
        }

        int colWidth = 20;
        LinkedList<Integer> columnWidths = new LinkedList<Integer>();

        // measure size of columns


        int max = 0;
        for (T value : firstDim) {
            if(value!=null) {
                max = Math.max(value.toString().length(), max);
            }
        }
        columnWidths.add(Math.min(max, colWidth * 3));

        max = 0;
        for (T value : firstDim) {
            max = Math.max(value.toString().length(), max);
        }
        columnWidths.add(Math.min(max, colWidth));

        for (T value : secondDim) {
            columnWidths.add(Math.min(value.toString().length(), colWidth));
        }



        // print second dimension labels
        output += padLeft("", columnWidths.get(0)) + " | ";
        output += padLeft("", columnWidths.get(1)) + " | ";
        int i=2;
        for (T value : secondDim) {
            output += padLeft(value.toString(), columnWidths.get(i++)) + " | ";
        }
        output += "\n";
        output += padLeft("", columnWidths.get(0)) + " | ";
        output += padLeft("", columnWidths.get(1)) + " | ";


        // print second dimension values
        int colIdx = 2;
        for (T value : secondDim) {
            output += padLeft(value.toString(), columnWidths.get(colIdx++)) + " | ";
        }
        output += "\n";

        // print first dimension labels & values
        colIdx=0;
        for (T value : firstDim) {

            output += padRight(value.toString(), columnWidths.get(0)) + " | ";
            colIdx=1;

            output += padLeft(value + "", columnWidths.get(colIdx)) + " | ";

            // print scores
            int colIdx2 = colIdx+1;
            for (T secondValue : secondDim) {
                if (get(value, secondValue) == null) {
                    output += padRight("", columnWidths.get(colIdx2++)) + " | ";
                } else {
                    output += padRight(
                            String.format("%1$,.8f", get(value, secondValue)),
                            columnWidths.get(colIdx2++)) + " | ";
                }
            }
            output += "\n";
        }
        return output;
    }

    public String listPairs() {
        StringBuilder sb = new StringBuilder();

        for(T first : getFirstDim()) {

            for(T second : getMatches(first)) {

                sb.append(first);
                sb.append("\t");
                sb.append(second);
                sb.append("\t");
                sb.append(get(first, second));
                sb.append("\n");

            }

        }

        return sb.toString();
    }

    public void printStatistics(String label) {
        double percent = (double) getNumberOfNonZeroElements()
                / (double) getNumberOfElements();


    }

    public MatrixSimilarity<T> copy() {
        MatrixSimilarity<T> m = createEmptyCopy();

        for(T first : getFirstDim()) {
            for(T second : getSecondDim()) {
                m.set(first, second, get(first, second));
            }
        }

        return m;
    }

    protected abstract MatrixSimilarity<T> createEmptyCopy();

    public Double getMaxValue() {
        Double d = null;

        for(T first : getFirstDim()) {
            for(T second : getMatches(first)) {
                if(d==null) {
                    d = get(first, second);
                } else {
                    d = Math.max(d, get(first, second));
                }
            }
        }

        return d;
    }

    public static <T extends Matchable, U extends Matchable> MatrixSimilarity<T> fromCorrespondences(Collection<Aligner<T, U>> aligners, MatrixSimilarityFactory factory) {
        MatrixSimilarity<T> m = factory.createMatrixSimilarity(0, 0);

        for(Aligner<T, U> aligner : aligners) {
            m.add(aligner.getFirstRecordType(), aligner.getSecondRecordType(), aligner.getSimilarityScore());
        }

        return m;
    }

    public HasMatchPredicate<T> getHasMatchPredicate() {
        return new HasMatchPredicate<>(this);
    }

    public static class HasMatchPredicate<T> implements Func<Boolean, T> {

        private MatrixSimilarity<T> m;

        public HasMatchPredicate(MatrixSimilarity<T> m) {
            this.m = m;
        }


        @Override
        public Boolean invoke(T in) {
            return m.getMatches(in).size()>0;
        }

    }
}
