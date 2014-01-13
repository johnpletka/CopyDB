package core;

/**
 * User: jpletka
 * Date: 1/10/14
 * Time: 12:48 PM
 */
public class TableCopyStrategy {
    private String tableName;
    private boolean active;
    private String diffModel;
    private String diffColumn;
    private String insertMode;

    public TableCopyStrategy(){}
    public TableCopyStrategy(String tableName, boolean active, String diffModel, String diffColumn, String insertMode) {
        this.tableName = tableName;
        this.active = active;
        this.diffModel = diffModel;
        this.diffColumn = diffColumn;
        this.insertMode = insertMode;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public String getDiffModel() {
        return diffModel;
    }

    public void setDiffModel(String diffModel) {
        this.diffModel = diffModel;
        if(diffModel == null ||
            (!"all".equals(diffModel) &&
             !"rowcount".equals(diffModel) &&
             !"column".equals(diffModel))){
            throw new RuntimeException("Invalid value for diffModel.  Expected {all,rowcount,column}. Got "+diffModel);
        }
    }

    public String getDiffColumn() {
        return diffColumn;
    }

    public void setDiffColumn(String diffColumn) {
        this.diffColumn = diffColumn;
    }

    public String getInsertMode() {
        return insertMode;
    }

    public void setInsertMode(String insertMode) {
        this.insertMode = insertMode;
        if(insertMode == null ||
            (!"append".equals(insertMode) &&
             !"delete".equals(insertMode))){
            throw new RuntimeException("Invalid value for insertMode.  Expected {append,delete}. Got "+insertMode);
        }
    }
    public String toString(){
        return tableName;
    }
}
