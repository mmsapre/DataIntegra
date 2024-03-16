package com.integration.em.model;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import com.integration.em.model.Record;
import com.integration.em.model.CSVMatchableReader;
@Slf4j
public class CSVRecordReader extends CSVMatchableReader<Record, Attribute> {

	private int idIndex = -1;
	private Map<String, Attribute> attributeMapping;
	private Attribute[] attributes = null;


	public CSVRecordReader(int idColumnIndex) {
		this.idIndex = idColumnIndex;
	}

	public CSVRecordReader(int idColumnIndex, Map<String, Attribute> attributeMapping) {
		this.idIndex = idColumnIndex;
		this.attributeMapping = attributeMapping;
		this.attributes = null;
	}


	@Override
	protected void readLine(File file, int rowNumber, String[] values, DataSet<Record, Attribute> dataset) {

		Set<String> ids = new HashSet<>();

		if (rowNumber == 0) {

			attributes = new Attribute[values.length];

			for (int i = 0; i < values.length; i++) {
				String v = values[i];
				String attributeId = String.format("%s_Col%d", file.getName(), i);
				Attribute a = null;
				if (this.attributeMapping == null) {
					a = new Attribute(attributeId, file.getAbsolutePath());
				} else {
					a = this.attributeMapping.get(v);
					if (a == null) {
						a = new Attribute(attributeId, file.getAbsolutePath());
					}
				}

				attributes[i] = a;
				a.setName(v);
				dataset.addAttribute(a);
			}

		} else {

			String id = String.format("%s_%d", file.getName(), rowNumber);

			if (idIndex >= 0 && values[idIndex] != null) {
				id = values[idIndex];

				if (ids.contains(id)) {
					String replacementId = String.format("%s_%d", file.getName(), rowNumber);
					log.error(String.format("Id '%s' (line %d) already exists, using '%s' instead!", id, rowNumber,
							replacementId));
					id = replacementId;
				}

				ids.add(id);
			}

			Record r = new Record(id, file.getAbsolutePath());

			for (int i = 0; i < values.length; i++) {
				Attribute a;
				String v = values[i];

				if (attributes != null && attributes.length > i) {
					a = attributes[i];
				} else {
					String attributeId = String.format("%s_Col%d", file.getName(), i);
					a = dataset.getAttribute(attributeId);
				}

				if (v.isEmpty()) {
					v = null;
				}

				r.setValue(a, v);
			}

			dataset.add(r);

		}

	}

}
