package com.integration.em.tables;

import java.io.File;

public interface TblWriter {

    File write(Tbl t, File f) throws Exception;

}
