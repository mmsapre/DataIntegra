package com.integration.em.similarity;

import com.wcohen.ss.Jaccard;
import com.wcohen.ss.tokens.SimpleTokenizer;
import org.apache.commons.lang3.StringUtils;
public class TokenizingJaccardSimilarity extends SimilarityMeasure<String> {

	private static final long serialVersionUID = 1L;


	@Override
	public double calculate(String first, String second) {
		if(StringUtils.isEmpty(first) || StringUtils.isEmpty(second)) {
			return 0.0;
		} else {
			Jaccard j = new Jaccard(new SimpleTokenizer(true, true));
			return j.score(first, second);
		}
	}

}
