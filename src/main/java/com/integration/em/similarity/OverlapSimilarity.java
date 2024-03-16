package com.integration.em.similarity;


import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class OverlapSimilarity extends SimilarityMeasure<List<String>> {
	
	private static final long serialVersionUID = 1L;

	@Override
	public double calculate(List<String> first, List<String> second) {

		Set<String> firstSet = new HashSet<>(first);
		Set<String> secondSet = new HashSet<>(second);

		int min = Math.min(firstSet.size(), secondSet.size());
		int matches = 0;

		if(min==0) {
			return 1.0;
		} else {
			for (String s1 : firstSet) {
				for (String s2 : secondSet) {
					if (s1.equals(s2)) {
						matches++;
						continue;
					}
				}
			}

			return (double) matches / (double) min;
		}
	}

}
