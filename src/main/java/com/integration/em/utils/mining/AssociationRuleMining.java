package com.integration.em.utils.mining;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
public class AssociationRuleMining<TItem> {

    public Map<Set<TItem>, Set<TItem>> calculateAssociationRulesForColumnPositions(Map<Set<TItem>, Integer> itemSets) {

        final Map<Set<TItem>, Set<TItem>> rules = new HashMap<>();

        for(Set<TItem> itemset : itemSets.keySet()) {
            if(itemset.size()>1) {

                for(TItem item : itemset) {
                    Set<TItem> condition = new HashSet<>(itemset);
                    condition.remove(item);
                    Set<TItem> consequent = new HashSet<>();
                    consequent.add(item);


                    rules.put(condition, consequent);
                }

            }
        }

        return rules;
    }
}
