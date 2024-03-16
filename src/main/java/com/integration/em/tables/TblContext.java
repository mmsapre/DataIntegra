package com.integration.em.tables;

import java.io.Serializable;

public class TblContext implements Serializable {

    private String url;
    private String pageTitle;
    private String tableTitle;
    private int tableNum;
    private String textBeforeTable;
    private String textAfterTable;
    private String timestampBeforeTable;
    private String timestampAfterTable;
    private String lastModified;


    public String getUrl() {
        return url;
    }
    public void setUrl(String url) {
        this.url = url;
    }


    public String getPageTitle() {
        return pageTitle;
    }
    public void setPageTitle(String pageTitle) {
        this.pageTitle = pageTitle;
    }


    public String getTableTitle() {
        return tableTitle;
    }
    public void setTableTitle(String tableTitle) {
        this.tableTitle = tableTitle;
    }


    public int getTableNum() {
        return tableNum;
    }
    public void setTableNum(int tableNum) {
        this.tableNum = tableNum;
    }


    public String getTextBeforeTable() {
        return textBeforeTable;
    }
    public void setTextBeforeTable(String textBeforeTable) {
        this.textBeforeTable = textBeforeTable;
    }


    public String getTextAfterTable() {
        return textAfterTable;
    }
    public void setTextAfterTable(String textAfterTable) {
        this.textAfterTable = textAfterTable;
    }


    public String getTimestampBeforeTable() {
        return timestampBeforeTable;
    }
    public void setTimestampBeforeTable(String timestampBeforeTable) {
        this.timestampBeforeTable = timestampBeforeTable;
    }


    public String getTimestampAfterTable() {
        return timestampAfterTable;
    }
    public void setTimestampAfterTable(String timestampAfterTable) {
        this.timestampAfterTable = timestampAfterTable;
    }


    public String getLastModified() {
        return lastModified;
    }
    public void setLastModified(String lastModified) {
        this.lastModified = lastModified;
    }
}
