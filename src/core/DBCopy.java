package core;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.sql.*;
import java.util.*;

/**
 * User: jpletka
 * Date: 1/9/14
 * Time: 4:28 PM
 */
public class DBCopy {

    Properties prop = null;
    Connection fromCon = null;
    Connection toCon = null;
    int batchsize = 1000;
    public DBCopy(Properties prop) throws Exception{
        this.prop = prop;
        if(this.prop == null){
            throw new RuntimeException("Properties file can not be null");
        }
        verifyRequiredProperties(prop,new String[]{"fromdb.driver","todb.driver","fromdb.url","fromdb.uid","fromdb.pwd","todb.url","todb.uid","todb.pwd"});
        setupConnections();
        if(prop.contains("batchsize")){
            this.batchsize = Integer.parseInt(prop.getProperty("batchsize"));
        }
    }
    public void verifyRequiredProperties(Properties p,String[] required){
        for(int i=0;i<required.length;i++){
            if(p.getProperty(required[i]) == null || p.getProperty(required[i]).trim().length() == 0){
                throw new RuntimeException("Missing required property "+required[i]);
            }
        }

    }
    public void setupConnections() throws Exception{
        DriverManager.registerDriver((Driver) Class.forName(prop.getProperty("fromdb.driver")).newInstance());
        if(!prop.getProperty("fromdb.driver").equals(prop.getProperty("todb.driver"))) {
            DriverManager.registerDriver((Driver) Class.forName(prop.getProperty("todb.driver")).newInstance());
        }
        fromCon = DriverManager.getConnection(prop.getProperty("fromdb.url"),prop.getProperty("fromdb.uid"),prop.getProperty("fromdb.pwd"));
        toCon = DriverManager.getConnection(prop.getProperty("todb.url"),prop.getProperty("todb.uid"),prop.getProperty("todb.pwd"));

    }
    public void go() throws Exception{
        List<TableCopyStrategy> tables = getTables();
        System.out.println("Disabling constraints");
        disableConstraints(true);
        int count = 1;
        for(TableCopyStrategy table : tables){
            if(table.isActive()){
                System.out.write((String.valueOf(count++)+": ").getBytes());
                copyTable(table);
            }else{
                System.out.println("Skipping "+table.getTableName());
            }
        }
        System.out.println("Enabling constraints");
        disableConstraints(false);
        System.out.println("Successfully transferred "+tables.size()+" tables");

    }
    public void disableConstraints(boolean disable) throws SQLException{
        String cmd = "";
        if(disable){
            cmd = "exec dbo.sp_MSforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL'";
        }else{
            cmd = "exec dbo.sp_MSforeachtable 'ALTER TABLE ? CHECK CONSTRAINT ALL'";
        }
        System.out.println("calling "+cmd);
        CallableStatement stmt = toCon.prepareCall(cmd);
        stmt.execute();
    }
    public int getRowCount(Connection conn, String tableName) throws SQLException{
        ResultSet rs = null;
        Statement stmt = null;
        try{
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select count(*) from "+tableName);
            if(rs.next()){
                return rs.getInt(1);
            }else{
                return -1;
            }
        }finally{
            if(rs != null) rs.close();
            if(stmt != null) stmt.close();
        }
    }
    public Object getMax(Connection con,String table, String col) throws SQLException{
        ResultSet rs = null;
        Statement stmt = null;
        try{
            stmt = con.createStatement();
            rs = stmt.executeQuery("select max("+col+") as max_val from " + table);
            rs.next();
            return rs.getObject(1);
        }finally{
            if(rs != null) rs.close();
            if(stmt != null) stmt.close();
        }
    }

    public void copyTable(TableCopyStrategy table) throws SQLException{
        PreparedStatement selectStmt = null;
        if("rowcount".equals(table.getDiffModel())){
            int fromCount = getRowCount(fromCon,table.getTableName());
            int toCount = getRowCount(toCon,table.getTableName());
            if(fromCount == toCount){
                System.out.println("Skipping "+table.getTableName()+" because rowcount is identical");
                return;
            }
            selectStmt = fromCon.prepareStatement("select * from "+table.getTableName());

        }
        if("column".equals(table.getDiffModel())){
            Object fromMax = getMax(fromCon,table.getTableName(),table.getDiffColumn());
            Object toMax  = getMax(toCon,table.getTableName(),table.getDiffColumn());
            if(fromMax == null){
                System.out.println("Skipping "+table.getTableName()+" because max("+table.getDiffColumn()+") is null");
                return;
            }
            if(toMax != null){
                if(fromMax.equals(toMax)){
                    System.out.println("Skipping "+table.getTableName()+" because max("+table.getDiffColumn()+") is "+fromMax.toString()+" in both");
                    return;
                }
            }
            selectStmt = fromCon.prepareStatement("select * from "+table.getTableName()+" where "+table.getDiffColumn()+" > ?");
            selectStmt.setObject(1,fromMax);
        }
        if("all".equals(table.getDiffModel())){
            selectStmt = fromCon.prepareStatement("select * from "+table.getTableName());
        }
        if("delete".equals(table.getInsertMode())){
            System.out.println("Deleting previous data from "+table.getTableName());
            PreparedStatement delStmt = toCon.prepareStatement("delete from "+table.getTableName());
            delStmt.execute();
            delStmt.close();

        }
        System.out.println("Beginning copying "+table.getTableName());
        ResultSet fromRs = null;
        PreparedStatement insertStmt = null;
        try{
            fromRs = selectStmt.executeQuery();
            String insertSql = "insert into "+table.getTableName()+" (";
            for(int i=1; i<= fromRs.getMetaData().getColumnCount();i++){
                if(i>1){
                    insertSql += ",";
                }
                insertSql += fromRs.getMetaData().getColumnName(i);
            }
            insertSql += ") values(";
            for(int i=0;i<fromRs.getMetaData().getColumnCount();i++){
                if(i>0){
                    insertSql += ",";
                }
                insertSql += "?";
            }
            insertSql += ")";
            System.out.println("Insert Statement: "+insertSql);
            insertStmt = toCon.prepareStatement(insertSql);
            int batchCounter = 0;
            while(fromRs.next()){
                for(int i=1; i<= fromRs.getMetaData().getColumnCount();i++){
                    insertStmt.setObject(i, fromRs.getObject(i));
                }
                insertStmt.addBatch();
                insertStmt.clearParameters();
                batchCounter++;
                if(batchCounter >= batchsize){
                    insertStmt.executeBatch();
                    insertStmt.clearBatch();
                }
            }
            int[] keys = insertStmt.executeBatch();
            System.out.println("Inserted "+keys.length+" rows");
        }finally{
            if(fromRs != null) fromRs.close();
            if(insertStmt != null) insertStmt.close();
        }

    }
    public void outputTables(Connection conn) throws SQLException{
        ResultSet rs = null;
        try{

            DatabaseMetaData meta = conn.getMetaData();
            rs = meta.getTables(null, null, null,new String[] {"TABLE"});
            Set<String> tableSet = new TreeSet<String>();
            while (rs.next()) {
                tableSet.add(rs.getString("TABLE_NAME"));
            }
            for(String val : tableSet) System.out.println(val);
        }finally{
            if(rs != null) rs.close();
        }
    }
    public List<TableCopyStrategy> getTables(){
        List<TableCopyStrategy> tables = new ArrayList<TableCopyStrategy>();
        for(String key : prop.stringPropertyNames()){
            if(key.startsWith("table.")){
                TableCopyStrategy tc = new TableCopyStrategy();
                tc.setTableName(key.substring(key.indexOf(".")+1));
                String val[] = prop.getProperty(key).split(",");
                if(val.length != 4){
                    throw new RuntimeException("Invalid config string for table "+key+" expected 4 values, but got "+val.length);
                }
                try{
                    tc.setActive(Boolean.parseBoolean(val[0]));
                }catch(Exception e){
                    throw new RuntimeException(val[0]+" is not a valid boolean for table "+key);
                }
                tc.setDiffModel(val[1]);
                tc.setDiffColumn(val[2]);
                tc.setInsertMode(val[3]);
                tables.add(tc);
            }
        }
        return tables;
    }


    public static void main(String[] args){
        try{
            if(args.length != 1){
                System.out.println("Syntax: DBCopy db_copy.properties");
                System.exit(0);
            }
            Properties p = new Properties();
            try{
                p.load(new FileInputStream(args[0]));
            }catch(Exception e){
                System.out.println("Unable to file file "+args[0]);
                System.exit(0);
            }
            DBCopy dbCopy = new DBCopy(p);
            dbCopy.go();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
