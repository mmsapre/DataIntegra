package com.integration.em.model;

public class MatchableValue implements Matchable{

    private final Object value;
    private final String recordId;
    private final String attributeId;
    public MatchableValue(Object value, String recordId, String attributeId) {
        this.value = value;
        this.recordId = recordId;
        this.attributeId = attributeId;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String getIdentifier() {
        return value.toString();
    }

    @Override
    public String getProvenance() {
        return null;
    }
    public String getRecordId() {
        return recordId;
    }

    public String getAttributeId() {
        return attributeId;
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((attributeId == null) ? 0 : attributeId.hashCode());
        result = prime * result + ((recordId == null) ? 0 : recordId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MatchableValue other = (MatchableValue) obj;
        if (attributeId == null) {
            if (other.attributeId != null)
                return false;
        } else if (!attributeId.equals(other.attributeId))
            return false;
        if (recordId == null) {
            return other.recordId == null;
        } else return recordId.equals(other.recordId);
    }
}
