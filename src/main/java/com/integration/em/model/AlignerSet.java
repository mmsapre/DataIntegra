package com.integration.em.model;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import com.integration.em.cluster.ConnectComponentCluster;
import com.integration.em.processing.Processable;
import com.integration.em.utils.Q;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
@Slf4j
public class AlignerSet<RecordType extends Matchable & Mixed<SchemaElementType>, SchemaElementType extends Matchable> {

    private Collection<RecordGroup<RecordType, SchemaElementType>> recordGroups;
    private Map<String, RecordGroup<RecordType, SchemaElementType>> recordIndex;

    private RecordGroupFactory<RecordType, SchemaElementType> groupFactory;

    public AlignerSet() {
        recordGroups = new LinkedList<>();
        recordIndex = new HashMap<>();
        groupFactory = new RecordGroupFactory<>();
    }

    public void setGroupFactory(RecordGroupFactory<RecordType, SchemaElementType> groupFactory) {
        this.groupFactory = groupFactory;
    }


    public Collection<RecordGroup<RecordType, SchemaElementType>> getRecordGroups() {
        return recordGroups;
    }

    public void loadAligners(File alignerFile, MixedDataSet<RecordType, SchemaElementType> mixedDataSet, MixedDataSet<RecordType, SchemaElementType> secondMixedDataSet) throws IOException {
        CSVReader reader = new CSVReader(new FileReader(alignerFile));

        String[] values = null;

        while ((values = reader.readNext()) != null) {
            if (mixedDataSet.getRecord(values[0]) == null) {
                log.error(String.format(
                        "Record %s not found in first dataset", values[0]));
                continue;
            }
            if (secondMixedDataSet.getRecord(values[1]) == null) {
                log.error(String.format(
                        "Record %s not found in second dataset", values[0]));
                continue;
            }


            RecordGroup<RecordType, SchemaElementType> grp1 = recordIndex.get(values[0]);
            RecordGroup<RecordType, SchemaElementType> grp2 = recordIndex.get(values[1]);

            if (grp1 == null && grp2 == null) {
                RecordGroup<RecordType, SchemaElementType> grp = groupFactory.createRecordGroup();
                grp.addRecord(values[0], mixedDataSet);
                grp.addRecord(values[1], secondMixedDataSet);
                recordIndex.put(values[0], grp);
                recordIndex.put(values[1], grp);
                recordGroups.add(grp);
            } else if (grp1 != null && grp2 == null) {
                grp1.addRecord(values[1], secondMixedDataSet);
                recordIndex.put(values[1], grp1);
            } else if (grp1 == null && grp2 != null) {
                grp2.addRecord(values[0], mixedDataSet);
                recordIndex.put(values[0], grp2);
            } else {
                grp1.mergeWith(grp2);

                for (String id : grp2.getRecordIds()) {
                    recordIndex.put(id, grp1);
                }
            }
        }

        reader.close();
    }

    public void loadAligners(File alignerFile, MixedDataSet<RecordType, SchemaElementType> mixedDataSet) throws IOException {
        CSVReader reader = new CSVReader(new FileReader(alignerFile));

        String[] values = null;
        int skipped = 0;

        while ((values = reader.readNext()) != null) {
            if (mixedDataSet.getRecord(values[0]) == null) {
                skipped++;
                continue;
            }
            RecordGroup<RecordType, SchemaElementType> grp2 = recordIndex.get(values[1]);

            if (grp2 == null) {
                // no existing groups, create a new one
                RecordGroup<RecordType, SchemaElementType> grp = groupFactory.createRecordGroup();
                grp.addRecord(values[0], mixedDataSet);
                recordIndex.put(values[1], grp);
                recordGroups.add(grp);
            } else {
                grp2.addRecord(values[0], mixedDataSet);
                recordIndex.put(values[0], grp2);
            }
        }

        reader.close();

        if (skipped>0) {
            log.error(String.format("Skipped %,d records (not found in provided dataset)", skipped));
        }
    }

    public void createFromAligners(Processable<Aligner<RecordType, Matchable>> alignerProcessable, MixedDataSet<RecordType, SchemaElementType> mixedDataSet, MixedDataSet<RecordType, SchemaElementType> secondMixedDataSet) {

        Map<String, MixedDataSet<RecordType, SchemaElementType>> idToDataSet = new HashMap<>();

        ConnectComponentCluster<RecordType> clu = new ConnectComponentCluster<>();
        for(Aligner<RecordType, Matchable> aligner : alignerProcessable.get()) {
            clu.addEdge(new Triple<RecordType, RecordType, Double>(aligner.getFirstRecordType(), aligner.getSecondRecordType(), aligner.getSimilarityScore()));
            idToDataSet.put(aligner.getFirstRecordType().getIdentifier(), mixedDataSet);
            idToDataSet.put(aligner.getSecondRecordType().getIdentifier(), secondMixedDataSet);
        }
        Map<Collection<RecordType>, RecordType> clusters = clu.createResult();

        for(Collection<RecordType> cluster : clusters.keySet()) {
            RecordGroup<RecordType, SchemaElementType> grp = groupFactory.createRecordGroup();

            for(RecordType r : cluster) {
                grp.addRecord(r.getIdentifier(), idToDataSet.get(r.getIdentifier()));
                recordIndex.put(r.getIdentifier(), grp);
            }

            recordGroups.add(grp);
        }
    }

    public void writeGroupSizeDistribution(File outputFile) throws IOException {
        Map<Integer, Integer> sizeDist = new HashMap<>();

        for (RecordGroup<RecordType, SchemaElementType> grp : recordGroups) {
            int size = grp.getSize();

            Integer count = sizeDist.get(size);

            if (count == null) {
                count = 0;
            }

            sizeDist.put(size, ++count);
        }

        CSVWriter writer = new CSVWriter(new FileWriter(outputFile));

        writer.writeNext(new String[] { "Group Size", "Frequency" });
        for (int size : sizeDist.keySet()) {
            writer.writeNext(new String[] { Integer.toString(size),
                    Integer.toString(sizeDist.get(size)) });
        }

        writer.close();
    }

    public void printGroupSizeDistribution()  {
        Map<Integer, Integer> sizeDist = new HashMap<>();

        for (RecordGroup<RecordType, SchemaElementType> grp : recordGroups) {
            int size = grp.getSize();

            Integer count = sizeDist.get(size);

            if (count == null) {
                count = 0;
            }

            sizeDist.put(size, ++count);
        }
        log.info("Group Size Distribtion of " + recordGroups.size() + " groups:");
        log.info("\tGroup Size \t| Frequency ");
        log.info("\t————————————————————————————");

        for (int size : Q.sort(sizeDist.keySet())) {
            String sizeStr = Integer.toString(size);
            String countStr = Integer.toString(sizeDist.get(size));
            log.info("\t\t" + sizeStr + "\t| \t" + countStr);
        }

    }
}
