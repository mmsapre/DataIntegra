package com.integration.em.utils.mining;

import com.integration.em.tables.Tbl;
import com.integration.em.utils.Distribution;
import com.integration.em.utils.MapUtils;
import com.integration.em.utils.Q;

import java.util.*;
public class FrequentItemsMining<TItem> {

    public Map<Set<TItem>, Integer> calculateFrequentItemSetsOfColumnPositions(Collection<Tbl> tbls, Set<Collection<TItem>> transactions) {

        Map<Set<TItem>, Integer> itemSets = new HashMap<>();

        Map<TItem, Set<Collection<TItem>>> transactionIndex = new HashMap<>();

        for(Collection<TItem> transaction : transactions) {
            Distribution<TItem> itemDistribution = Distribution.fromCollection(transaction);

            for(TItem item : itemDistribution.getElements()) {

                Set<TItem> itemSet = Q.toSet(item);

                MapUtils.add(itemSets, itemSet, itemDistribution.getFrequency(item));

                Set<Collection<TItem>> t = MapUtils.getFast(transactionIndex, item, (i) -> new HashSet<>());
                t.add(transaction);
            }
        }

        boolean hasChanges = false;

        Set<Set<TItem>> OneItemSets = new HashSet<>(itemSets.keySet());
        Set<Set<TItem>> lastItemSets = itemSets.keySet();
        Set<Set<TItem>> currentItemSets = new HashSet<>();

        do {

            for(Set<TItem> itemset1 : lastItemSets) {

                for(Set<TItem> itemset2 : OneItemSets) {

                    if(!itemset1.equals(itemset2) && !itemset1.containsAll(itemset2)) {

                        Set<TItem> itemset = new HashSet<>();

                        itemset.addAll(itemset1);
                        itemset.addAll(itemset2);

                        currentItemSets.add(itemset);
                    }

                }
            }

            Iterator<Set<TItem>> it = currentItemSets.iterator();
            while(it.hasNext()) {
                Set<TItem> itemSet = it.next();

                Set<Collection<TItem>> commonTransactions = null;

                for(TItem item : itemSet) {

                    Set<Collection<TItem>> transactionsWithItem = transactionIndex.get(item);

                    if(commonTransactions==null) {
                        commonTransactions = transactionsWithItem;
                    } else {
                        commonTransactions = Q.intersection(commonTransactions, transactionsWithItem);
                    }

                    if(commonTransactions.size()==0) {
                        break;
                    }
                }

                if(commonTransactions.size()==0) {
                    it.remove();
                } else {
                    itemSets.put(itemSet, commonTransactions.size());
                }
            }

            hasChanges = currentItemSets.size()>0;
            lastItemSets = currentItemSets;
            currentItemSets = new HashSet<>();

        } while(hasChanges);

        return itemSets;
    }
}
