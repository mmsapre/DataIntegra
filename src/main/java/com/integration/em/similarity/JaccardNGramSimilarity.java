package com.integration.em.similarity;

import com.wcohen.ss.Jaccard;
import com.wcohen.ss.tokens.NGramTokenizer;
import com.wcohen.ss.tokens.SimpleTokenizer;

public class JaccardNGramSimilarity extends Similarity<String>{

    private int nGramLen = 3;

    public JaccardNGramSimilarity(int nGramLen) {
        this.nGramLen = nGramLen;
    }

    @Override
    public double calculate(String first, String second) {
        if(first == null || second == null) {
            return 0.0;
        }

        NGramTokenizer tok = new NGramTokenizer(nGramLen, nGramLen, false, SimpleTokenizer.DEFAULT_TOKENIZER);
        Jaccard j = new Jaccard(tok);
        return j.score(first, second);
    }
}
