package com.integration.em.tables;

import com.integration.em.parsers.JsonTableSchema;

import java.io.File;

public class JsonTblWriter implements TblWriter{

    private boolean writeMapping;

    public void setWriteMapping(boolean writeMapping) {
        this.writeMapping = writeMapping;
    }

    @Override
    public File write(Tbl t, File f) throws Exception {
        return null;
    }

    public File getFileName(File f) {
        if(!f.getName().endsWith(".json")) {
            return new File(f.getAbsolutePath() + ".json");
        } else {
            return f;
        }
    }
    private void writeProvenance(Tbl t, JsonTableSchema data) {
        int provCount = 0;
        String[][] prov = new String[t.getSize()][];
        for(TblRow r : t.getTblRowArrayList()) {
            prov[r.getRowNumber()] = r.getProvenance().toArray(new String[r.getProvenance().size()]);
            provCount+=r.getProvenance().size();
        }
        if(provCount>0) {
            data.setRowProvenance(prov);
        }

        provCount=0;
        prov = new String[t.getColumns().size()][];
        for(TblColumn c : t.getColumns()) {
            prov[c.getColumnIndex()] = c.getProvenance().toArray(new String[c.getProvenance().size()]);
            provCount+=c.getProvenance().size();
        }
        if(provCount>0) {
            data.setColumnProvenance(prov);
        }
    }
}
