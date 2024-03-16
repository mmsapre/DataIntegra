package com.integration.em.model;

import java.io.Serializable;

public abstract class AbstractRecord<SchemaElementType> implements Matchable, Mixed<SchemaElementType>, Serializable {

    protected String identifier;
    protected String provenance;

    public AbstractRecord() {
    }

    public AbstractRecord(String identifier, String provenance) {
        this.identifier = identifier;
        this.provenance = provenance;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String getProvenance() {
        return provenance;
    }

    @Override
    public abstract boolean hasValue(SchemaElementType attribute);

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((identifier == null) ? 0 : identifier.hashCode());
        return result;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof AbstractRecord))
            return false;
        AbstractRecord other = (AbstractRecord) obj;
        if (identifier == null) {
            if (other.identifier != null)
                return false;
        } else if (!identifier.equals(other.identifier))
            return false;
        return true;
    }
}
