package dbEngine;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.TreeMap;

/**
 * This is a database engine that is loosely based on MySQL
 * @version 1.0
 * @author FenglanYang
 *
 */
public class FLYBase {
    
    private static String version = "1.0";
    private static String prompt = "flysql> ";
    
    private static String dataFolderName = "data";  // the folder where all the schema files should be stored in
    static String infoSchemaFolderName = dataFolderName + "/information_schema";    // the folder where the information_schema tables should be stored
    static String schemataTableFileName = infoSchemaFolderName + "/information_schema.schemata.tbl";
    static String tablesTableFileName = infoSchemaFolderName + "/information_schema.tables.tbl";
    static String columnsTableFileName = infoSchemaFolderName + "/information_schema.columns.tbl";
    
    static String dbActive = "information_schema";  // the currently active (in use) schema (database)
    
    // **************************************************************************
    //  STATIC METHOD DEFINTIONS
    // **************************************************************************
    
    /**
     * Display the welcome "splash screen"
     */
    protected static void splashScreen() {
        System.out.println(line("*",80));
        System.out.println("Welcome to FLYBase");
        version();
        System.out.println("Type \"help;\" to display supported commands.");
        System.out.println(line("*",80));
    }
    
    /**
     * @param s The String to be repeated
     * @param num The number of time to repeat String s.
     * @return String A String object, which is the String s appended to itself num times.
     */
    protected static String line(String s,int num) {
        String a = "";
        for(int i=0;i<num;i++) {
            a += s;
        }
        return a;
    }
    
    protected static String tbSperateLine(int num) {
        String a = "+" + line("-", num);  
        return a;
    }
    
    /**
     * get the version of the database engine
     */
    protected static void version() {
        System.out.println("FLYBase v" + version + "\n");
    }
    
    /**
     *  Help: Display supported commands
     */
    protected static void help() {
        System.out.println(line("*",80));
        System.out.println("\tshow schemas;                 Show the schemas.");
        System.out.println("\tuse <schema_name>;            Use a specific schema.");
        System.out.println("\tshow tables;                  Show the tables of the currently active schema.");
        System.out.println("\tcreate schema <schema_name>;  Create a new schema.");
        System.out.println("\t<create-table command>;       Create a new table under the currently active schema.");
        System.out.println("\t<insert-into-table command>;  Insert a new row to a specific table.");
        System.out.println("\t<select-from-where query>;    Select one or more row(s) from a specific table.");
        System.out.println("\thelp;                         Show this help information");
        System.out.println("\texit;                         Exit the program");
        System.out.println(line("*",80));
    }
    
    /**
     * check if the information_schema schema exists
     * @return
     */
    protected static boolean infoSchemaExists() {
        String infoFolderName = dataFolderName + "/information_schema";
        File schemataTableFile = new File(infoFolderName + "/information_schema.schemata.tbl");
        File tablesTableFile = new File(infoFolderName + "/information_schema.tables.tbl");
        File columnsTableFile = new File(infoFolderName + "/information_schema.columns.tbl");
        if ( schemataTableFile.exists() && tablesTableFile.exists() && columnsTableFile.exists() ) {
            return true;
        } else {
            return false;
        }
    }
    
    /**
     * print a syntax error information
     */
    protected static void syntaxError() {
        System.out.println("Syntax Error!");
    }
    
    
    // **************************************************************************
    //  STATIC METHOD USED TO PROCESS DIFFERENT REQUESTS
    // **************************************************************************
    
    /**
     * process the show schemas request
     */
    protected static void showSchemas() {
        try {
            System.out.println(tbSperateLine(66));
            //System.out.println("| Database" + line(" ", 56) + " |");
            System.out.println("| Database");
            System.out.println(tbSperateLine(66));
            
            RandomAccessFile schemataTableFile = new RandomAccessFile(schemataTableFileName, "rw");
            RandomAccessFile tablesTableFile = new RandomAccessFile(tablesTableFileName, "rw");
            
            tablesTableFile.seek(28);   // TABLE_ROWS of SCHEMATA table of information_schema has offset 1+18+1+8=28
            long schemaNum = tablesTableFile.readLong();    // the number of schemas including the information_schema stored in the database engine
            for (long i = 1; i <= schemaNum; i++) { // print all the schemas
                System.out.print("| ");
                byte varcharLength = schemataTableFile.readByte();
                for(int j = 0; j < varcharLength; j++) {
                    System.out.print((char)schemataTableFile.readByte());
                }
                System.out.println(" ");
            }  
            System.out.println(tbSperateLine(66));
            schemataTableFile.close();
            tablesTableFile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * check whether the passed schema exists
     * @param db
     * @return
     */
    protected static boolean existsSchema(String db) {
        boolean schemaFound = false;
        try {
            RandomAccessFile schemataTableFile = new RandomAccessFile(schemataTableFileName, "rw");
            RandomAccessFile tablesTableFile = new RandomAccessFile(tablesTableFileName, "rw");
            
            tablesTableFile.seek(28);   // TABLE_ROWS of SCHEMATA table of information_schema has offset 1+18+1+8=28
            long schemaNum = tablesTableFile.readLong();    // the number of schemas including the information_schema stored in the database
            for (long i = 1; i <= schemaNum; i++) { // scan all the schema names to check whether the schema is included in the database
                byte varcharLength = schemataTableFile.readByte();
                String currSchemaName = "";
                for(int j = 0; j < varcharLength; j++) {
                    currSchemaName += (char)schemataTableFile.readByte();
                }
                if (currSchemaName.equalsIgnoreCase(db)) {    // the schema is found in the storage
                    schemaFound = true;
                    break;  // no need to continue checking
                }
            }
            schemataTableFile.close();
            tablesTableFile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return schemaFound;
    }
    
    /**
     * process the request to use a specific schema (database)
     * @param dbToUse is the database to be used
     */
    protected static void useSchema(String dbToUse) {
        if (existsSchema(dbToUse)) {
            dbActive = dbToUse;
            System.out.println("The schema " + dbToUse + " you requested is now active.");
        } else {
            System.out.println("Request Rejected! The schema you tried to use does not exist.");
        }
    }
    
    /**
     * process the show schemas request. show all tables of the currently active schema (database)
     */
    protected static void showTables() {
        try {
            System.out.println(tbSperateLine(66));
            System.out.println("| Tables_in_" + dbActive);
            System.out.println(tbSperateLine(66));
            
            RandomAccessFile schemataTableFile = new RandomAccessFile(schemataTableFileName, "rw");
            RandomAccessFile tablesTableFile = new RandomAccessFile(tablesTableFileName, "rw");
            
            tablesTableFile.seek(62);   // TABLE_ROWS of TABLES table of information_schema has offset 1+18+1+8+8+1+18+1+6=62
            long tablesNum = tablesTableFile.readLong();    // the number of tables stored in the database engine
            tablesTableFile.seek(0);    // set the file pointer back to the beginning of the file and then scan
            for (long i = 1; i <= tablesNum; i++) { // print all the tables of the currently active schema
                byte varcharLengthSchema = tablesTableFile.readByte();
                String currSchemaName = "";
                for(int j = 0; j < varcharLengthSchema; j++) {
                    currSchemaName += (char)tablesTableFile.readByte();
                }
                if (currSchemaName.equalsIgnoreCase(dbActive)) {    // current schema is the active schema, so print the corresponding table name in this row
                    System.out.print("| ");
                    byte varcharLengthTable = tablesTableFile.readByte();
                    for(int j = 0; j < varcharLengthTable; j++) {
                        System.out.print((char)tablesTableFile.readByte());
                    }
                    System.out.println(" ");
                    tablesTableFile.seek(tablesTableFile.getFilePointer() + 8);    // set the file pointer to the next row
                } else {    // current schema is not the active schema, so continue to check the next row
                    byte varcharLengthTable = tablesTableFile.readByte();
                    tablesTableFile.seek(tablesTableFile.getFilePointer() + varcharLengthTable + 8);    // set the file pointer to the next row
                }
            }  
            System.out.println(tbSperateLine(66));
            schemataTableFile.close();
            tablesTableFile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * process the request to create a new schema (database)
     * @param dbToCreate
     */
    protected static void createSchema(String dbToCreate) {
        if (existsSchema(dbToCreate)) { // the schema (database) already exists
            System.out.println("Request Rejected! The schema you tried to create already exists.");
        } else {    // create the schema
            RandomAccessFile schemataTableFile;
            RandomAccessFile tablesTableFile;
            try {
                // update the SCHEMATA table
                schemataTableFile = new RandomAccessFile(schemataTableFileName, "rw");
                schemataTableFile.seek(schemataTableFile.length()); // set the file pointer to the end of the file
                schemataTableFile.writeByte(dbToCreate.length());
                schemataTableFile.writeBytes(dbToCreate);
                
                // update the TABLE_ROWS column of TABLES table
                tablesTableFile = new RandomAccessFile(tablesTableFileName, "rw");
                tablesTableFile.seek(28);   // TABLE_ROWS of SCHEMATA table of information_schema has offset 1+18+1+8=28
                long schemaNum = tablesTableFile.readLong();    // the number of schemas including the information_schema stored in the database engine
                tablesTableFile.seek(28);
                tablesTableFile.writeLong(schemaNum + 1); 
                
                schemataTableFile.close();
                
                String dbFolderName = dataFolderName + "/" + dbToCreate;
                new File(dbFolderName).mkdirs();    // create the folder if not exist
                
                System.out.println("Succeed! The schema " + dbToCreate + " is created.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    /*
     * 
     * @param currColumnTokens
     * @param colInfLenProcessed
     * @return return 0 if (isNotNullable, isPrimaryKey) = (false, false), 
     *         return 10 if (isNotNullable, isPrimaryKey) = (true, false),
     *         return 11 if (isNotNullable, isPrimaryKey) = (true, true).
     */
    /*
    protected static int isNotNullIsPri(String[] currColumnTokens, int colInfLenProcessed) {
        int result = 0;
        if (currColumnTokens.length == colInfLenProcessed + 2) {
            if (currColumnTokens[colInfLenProcessed].equalsIgnoreCase("PRIMARY")) {  // primary key
                result = 11;
            } else if (currColumnTokens[colInfLenProcessed].equalsIgnoreCase("NOT")) {   // not null
                result = 10;
            }
        } else if (currColumnTokens.length == colInfLenProcessed + 4) {
            if (currColumnTokens[colInfLenProcessed].equalsIgnoreCase("PRIMARY") || currColumnTokens[colInfLenProcessed + 2].equalsIgnoreCase("PRIMARY")) {  // primary key
                result = 11;
            } else if (currColumnTokens[colInfLenProcessed].equalsIgnoreCase("NOT") || currColumnTokens[colInfLenProcessed + 2].equalsIgnoreCase("NOT")) {   // not null
                result = 10;
            }
        } 
        return result;
    }
    */
    
    /**
     * get the number of rows of the passed table
     * @param tb
     * @return return an array containing the number of rows and the file pointer to the number of rows of the passed table; return [-1,-1] if the table does not exist
     * @throws Exception 
     */
    protected static long[] getTableRowNum(String tb) throws Exception {
        long rowNum = -1;   // the row count of the passed table
        long filePointerOfTableRows = -1;
        long[] result = new long[2];
        
        RandomAccessFile tablesTableFile = new RandomAccessFile(tablesTableFileName, "rw");
        
        tablesTableFile.seek(62);   // TABLE_ROWS of TABLES table of information_schema has offset 1+18+1+8+8+1+18+1+6=62
        long tablesNum = tablesTableFile.readLong();    // the number of tables stored in the database engine
        
        tablesTableFile.seek(0);    // set the file pointer back to the beginning of the file and then scan
        for (long i = 1; i <= tablesNum; i++) { // scan the TABLES table to check if the requested table exists
            byte varcharLengthSchema = tablesTableFile.readByte();
            String currSchemaName = "";
            for(int j = 0; j < varcharLengthSchema; j++) {
                currSchemaName += (char)tablesTableFile.readByte();
            }
            if (currSchemaName.equalsIgnoreCase(dbActive)) {    // current schema is the active schema
                byte varcharLengthTable = tablesTableFile.readByte();
                String currTableName = "";
                for(int j = 0; j < varcharLengthTable; j++) {   // get the table name
                    currTableName += (char)tablesTableFile.readByte();
                }
                if (currTableName.equalsIgnoreCase(tb)) {    // the table is found
                    filePointerOfTableRows = tablesTableFile.getFilePointer();
                    rowNum = tablesTableFile.readLong();
                    break;
                } else {
                    tablesTableFile.seek(tablesTableFile.getFilePointer() + 8); // set the file pointer to the next row
                }
            } else {    // current schema is not the active schema, so continue to check the next row
                byte varcharLengthTable = tablesTableFile.readByte();
                tablesTableFile.seek(tablesTableFile.getFilePointer() + varcharLengthTable + 8);    // set the file pointer to the next row
            }
        }
        tablesTableFile.close();
        result[0] = rowNum;
        result[1] = filePointerOfTableRows;
        return result;
    }
    
    /**
     * process the request to create a new table under the currently active schema (database)
     * @param tableToCreate
     * @param ColInfTokens
     */
    protected static void createTable(String tableToCreate, String[] ColInfTokens) {
        try {
            long rowNum = getTableRowNum(tableToCreate)[0];
            if (rowNum >= 0) {  // the table to be created already exists under the currently active schema
                System.out.println("Request Rejected! The table you tried to create already exists.");
            } else {
                RandomAccessFile tablesTableFile = new RandomAccessFile(tablesTableFileName, "rw");
                RandomAccessFile columnsTableFile = new RandomAccessFile(columnsTableFileName, "rw");
                
                // update TABLES table of information_schema: insert a new row
                tablesTableFile.seek(tablesTableFile.length());
                tablesTableFile.writeByte(dbActive.length()); // TABLE_SCHEMA
                tablesTableFile.writeBytes(dbActive);
                tablesTableFile.writeByte(tableToCreate.length()); // TABLE_NAME
                tablesTableFile.writeBytes(tableToCreate);
                tablesTableFile.writeLong(0); // TABLE_ROWS
                
                // update TABLE_ROWS column of TABLES table in information_schema: table_row++
                tablesTableFile.seek(62);   // TABLE_ROWS of TABLES table of information_schema has offset 1+18+1+8+8+1+18+1+6=62
                long tablesNum = tablesTableFile.readLong();    // the number of tables stored in the database engine
                tablesTableFile.seek(62);
                tablesTableFile.writeLong(tablesNum + 1);
                
                // create empty .tbl file for this new table
                String dbFolderName = dataFolderName + "/" + dbActive;
                String tblFileName = dbFolderName + "/" + dbActive + "." + tableToCreate + ".tbl";
                new File(tblFileName).createNewFile();
                
                // update COLUMNS table of information_schema
                columnsTableFile.seek(columnsTableFile.length());
                // add information of each columns of the tableToCreate to the COLUMNS table
                for (int i = 0; i < ColInfTokens.length; i++) {
                    // parse the user command
                    String[] currColumnTokens = ColInfTokens[i].split("[ ]+");
                    String currColName = currColumnTokens[0];
                    String isNullable = "";
                    String isPrimaryKey = "";
                    int colInfLenProcessed = 2;
                    
                    String currColType = "";
                    if (currColumnTokens[1].equalsIgnoreCase("BYTE")) {
                        currColType = "byte";
                    } else if (currColumnTokens[1].equalsIgnoreCase("SHORT")) {
                        if (currColumnTokens.length > 2 && currColumnTokens[2].equalsIgnoreCase("INT")) {
                            currColType = "short int";
                            colInfLenProcessed = 3;
                        } else {
                            currColType = "short"; 
                        }
                    } else if (currColumnTokens[1].equalsIgnoreCase("INT")) {
                        currColType = "int";
                    } else if (currColumnTokens[1].equalsIgnoreCase("LONG")) {
                        if (currColumnTokens.length > 2 && currColumnTokens[2].equalsIgnoreCase("INT")) {
                            currColType = "long int";
                            colInfLenProcessed = 3;
                        } else {
                            currColType = "long";
                        }
                    } else if (currColumnTokens[1].equalsIgnoreCase("FLOAT")) {
                        currColType = "float";
                    } else if (currColumnTokens[1].equalsIgnoreCase("DOUBLE")) {
                        currColType = "double";
                    } else if (currColumnTokens[1].equalsIgnoreCase("DATETIME")) {
                        currColType = "datetime";
                    } else if (currColumnTokens[1].equalsIgnoreCase("DATE")) {
                        currColType = "date";
                    } else if (currColumnTokens[1].charAt(0) == 'C' || currColumnTokens[1].charAt(0) == 'c') {  // CHAR(n)
                        int len = Integer.parseInt(currColumnTokens[1].substring(5, currColumnTokens[1].length() - 1));
                        currColType = "char(" + len + ")";
                    } else if (currColumnTokens[1].charAt(0) == 'V' || currColumnTokens[1].charAt(0) == 'v') {  //VARCHAR(n)
                        int len = Integer.parseInt(currColumnTokens[1].substring(8, currColumnTokens[1].length() - 1));
                        currColType = "varchar(" + len + ")";
                    } else {    // unsupported data type
                        syntaxError();
                    }
                    
                    if (currColumnTokens.length == colInfLenProcessed) {
                        isNullable = "YES";
                    } else if(currColumnTokens.length == colInfLenProcessed + 2) {
                        if (currColumnTokens[colInfLenProcessed].equalsIgnoreCase("PRIMARY")) {  // primary key
                            isNullable = "NO";
                            isPrimaryKey = "PRI";
                        } else if (currColumnTokens[colInfLenProcessed].equalsIgnoreCase("NOT")) {   // not null
                            isNullable = "NO";
                        }
                    } else if (currColumnTokens.length == colInfLenProcessed + 4) {
                        if (currColumnTokens[colInfLenProcessed].equalsIgnoreCase("PRIMARY") || currColumnTokens[colInfLenProcessed + 2].equalsIgnoreCase("PRIMARY")) {  // primary key
                            isNullable = "NO";
                            isPrimaryKey = "PRI";
                        } else if (currColumnTokens[colInfLenProcessed].equalsIgnoreCase("NOT") || currColumnTokens[colInfLenProcessed + 2].equalsIgnoreCase("NOT")) {   // not null
                            isNullable = "NO";
                        }
                    } 
                    
                    // update COLUMNS table
                    columnsTableFile.writeByte(dbActive.length()); // TABLE_SCHEMA
                    columnsTableFile.writeBytes(dbActive);
                    columnsTableFile.writeByte(tableToCreate.length()); // TABLE_NAME
                    columnsTableFile.writeBytes(tableToCreate);
                    columnsTableFile.writeByte(currColName.length()); // COLUMN_NAME
                    columnsTableFile.writeBytes(currColName);
                    columnsTableFile.writeInt(i + 1); // ORDINAL_POSITION
                    columnsTableFile.writeByte(currColType.length()); // COLUMN_TYPE
                    columnsTableFile.writeBytes(currColType);               
                    columnsTableFile.writeByte(isNullable.length()); // IS_NULLABLE
                    columnsTableFile.writeBytes(isNullable);
                    columnsTableFile.writeByte(isPrimaryKey.length()); // COLUMN_KEY
                    columnsTableFile.writeBytes(isPrimaryKey);
                    
                    // update TABLE_ROWS column of COLUMNS table in information_schema: table_row++
                    tablesTableFile.seek(97);   // TABLE_ROWS of COLUMNS table of information_schema has offset 1+18+1+8+8+1+18+1+6+8+1+18+1+7=97
                    long columsNum = tablesTableFile.readLong();    // the number of total columns (row number of columns table)
                    tablesTableFile.seek(97);
                    tablesTableFile.writeLong(columsNum + 1);
                    
                    // create empty .ndx files for this new table
                    String currndxFileName = dbFolderName + "/" + dbActive + "." + tableToCreate + "." + currColName + ".ndx";
                    new File(currndxFileName).createNewFile();    
                }
                tablesTableFile.close();
                columnsTableFile.close();
                
                System.out.println("SUCCEED! The table " + tableToCreate + " is now created.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }        
    }
    
    /**
     * get columns information of the passed table from the COLUMNS table of information_schema
     * @param tb
     * @throws Exception 
     */
    protected static LinkedHashMap<Integer, ArrayList<String>> getColsInfOfTable(String tb) throws Exception {
        RandomAccessFile tablesTableFile = new RandomAccessFile(tablesTableFileName, "rw");
        RandomAccessFile columnsTableFile = new RandomAccessFile(columnsTableFileName, "rw");
        
        tablesTableFile.seek(97);   // TABLE_ROWS of COLUMNS table of information_schema has offset 1+18+1+8+8+1+18+1+6+8+1+18+1+7=97
        long columsNum = tablesTableFile.readLong();    // the number of total columns (row number of columns table)               
        
        // scan the COLUMNS table to get the column position, name, type, is_nullable, column_key information of the passed table
        LinkedHashMap<Integer, ArrayList<String>> colsInfMap = new LinkedHashMap<>();
        int position = 0;
        String currColumnName = "";
        String currColumnType = "";
        String currIsNullable = "";
        String currIsPriKey = "";
        for (long i = 1; i <= columsNum; i++) {
            byte varcharLengthSchema = columnsTableFile.readByte();
            String currSchemaName = "";
            for(int j = 0; j < varcharLengthSchema; j++) {
                currSchemaName += (char)columnsTableFile.readByte();
            }
            if (currSchemaName.equalsIgnoreCase(dbActive)) {    // this is the database currently active
                byte varcharLengthTable = columnsTableFile.readByte();
                String currTableName = "";
                for(int j = 0; j < varcharLengthTable; j++) {   // get the table name
                    currTableName += (char)columnsTableFile.readByte();
                }
                if (currTableName.equalsIgnoreCase(tb)) {    // the table is found
                    byte varcharLengthColumn = columnsTableFile.readByte();
                    currColumnName = "";
                    for(int j = 0; j < varcharLengthColumn; j++) {   // get the column name
                        currColumnName += (char)columnsTableFile.readByte();
                    }
                    position = columnsTableFile.readInt();  // get the column position
                    byte varcharLengthType = columnsTableFile.readByte();
                    currColumnType = "";
                    for(int j = 0; j < varcharLengthType; j++) {   // get the column type
                        currColumnType += (char)columnsTableFile.readByte();
                    }
                    byte varcharLengthNullable = columnsTableFile.readByte();
                    currIsNullable = "";
                    for(int j = 0; j < varcharLengthNullable; j++) {   // get the IS_NULLABLE
                        currIsNullable += (char)columnsTableFile.readByte();
                    }
                    byte varcharLengthKey = columnsTableFile.readByte();
                    currIsPriKey = "";
                    for(int j = 0; j < varcharLengthKey; j++) {   // get the IS_NULLABLE
                        currIsPriKey += (char)columnsTableFile.readByte();
                    }
                    ArrayList<String> infList = new ArrayList<>();
                    infList.add(currColumnName);
                    infList.add(currColumnType);
                    infList.add(currIsNullable);
                    infList.add(currIsPriKey);
                    colsInfMap.put(position, infList);
                } else {    // current table is not the table
                    columnsTableFile.seek(columnsTableFile.readByte() + columnsTableFile.getFilePointer() + 4);    // to the column type
                    columnsTableFile.seek(columnsTableFile.readByte() + columnsTableFile.getFilePointer());    // to is nullable
                    columnsTableFile.seek(columnsTableFile.readByte() + columnsTableFile.getFilePointer());    // to column key
                    columnsTableFile.seek(columnsTableFile.readByte() + columnsTableFile.getFilePointer());    // set the file pointer to the next row
                }                        
            } else {    // current schema is not the active schema, so continue to check the next row
                byte varcharLengthTable = columnsTableFile.readByte();
                columnsTableFile.seek(columnsTableFile.getFilePointer() + varcharLengthTable);    // to the column name
                columnsTableFile.seek(columnsTableFile.readByte() + columnsTableFile.getFilePointer() + 4);    // to the column type
                columnsTableFile.seek(columnsTableFile.readByte() + columnsTableFile.getFilePointer());    // to is nullable
                columnsTableFile.seek(columnsTableFile.readByte() + columnsTableFile.getFilePointer());    // to column key
                columnsTableFile.seek(columnsTableFile.readByte() + columnsTableFile.getFilePointer());    // set the file pointer to the next row
            }
        }
        tablesTableFile.close();
        columnsTableFile.close();
        return colsInfMap;
    }
    
    /**
     * load the passed index file to a TreeMap
     * @param currIndexFile
     * @param currColType
     * @return
     * @throws IOException
     */
    protected static TreeMap<Object, ArrayList<Integer>> loadIndexFile(RandomAccessFile currIndexFile, String currColType) throws IOException {
        TreeMap<Object, ArrayList<Integer>> currcolumnIndex = new TreeMap<>();
        long fileLength = currIndexFile.length();
        
        if (currColType.equalsIgnoreCase("BYTE")) {
            while (currIndexFile.getFilePointer() < fileLength) {   // not reach the end of the file
                byte key = currIndexFile.readByte();
                int valNum = currIndexFile.readInt();
                ArrayList<Integer> values = new ArrayList<>();
                values.add(valNum);
                for (int i = 0; i < valNum; i++) {
                    values.add(currIndexFile.readInt());
                }
                currcolumnIndex.put(key, values);
            }  
        } else if (currColType.equalsIgnoreCase("SHORT") || currColType.equalsIgnoreCase("SHORT INT")) {
            while (currIndexFile.getFilePointer() < fileLength) {   // not reach the end of the file
                short key = currIndexFile.readShort();
                int valNum = currIndexFile.readInt();
                ArrayList<Integer> values = new ArrayList<>();
                values.add(valNum);
                for (int i = 0; i < valNum; i++) {
                    values.add(currIndexFile.readInt());
                }
                currcolumnIndex.put(key, values);
            }
        } else if (currColType.equalsIgnoreCase("INT")) {
            while (currIndexFile.getFilePointer() < fileLength) {   // not reach the end of the file
                int key = currIndexFile.readInt();
                int valNum = currIndexFile.readInt();
                ArrayList<Integer> values = new ArrayList<>();
                values.add(valNum);
                for (int i = 0; i < valNum; i++) {
                    values.add(currIndexFile.readInt());
                }
                currcolumnIndex.put(key, values);
            }
        } else if (currColType.equalsIgnoreCase("LONG") || currColType.equalsIgnoreCase("LONG INT")) {
            while (currIndexFile.getFilePointer() < fileLength) {   // not reach the end of the file
                long key = currIndexFile.readLong();
                int valNum = currIndexFile.readInt();
                ArrayList<Integer> values = new ArrayList<>();
                values.add(valNum);
                for (int i = 0; i < valNum; i++) {
                    values.add(currIndexFile.readInt());
                }
                currcolumnIndex.put(key, values);
            }
        } else if (currColType.equalsIgnoreCase("FLOAT")) {
            while (currIndexFile.getFilePointer() < fileLength) {   // not reach the end of the file
                float key = currIndexFile.readFloat();
                int valNum = currIndexFile.readInt();
                ArrayList<Integer> values = new ArrayList<>();
                values.add(valNum);
                for (int i = 0; i < valNum; i++) {
                    values.add(currIndexFile.readInt());
                }
                currcolumnIndex.put(key, values);
            }
        } else if (currColType.equalsIgnoreCase("DOUBLE")) {
            while (currIndexFile.getFilePointer() < fileLength) {   // not reach the end of the file
                double key = currIndexFile.readDouble();
                int valNum = currIndexFile.readInt();
                ArrayList<Integer> values = new ArrayList<>();
                values.add(valNum);
                for (int i = 0; i < valNum; i++) {
                    values.add(currIndexFile.readInt());
                }
                currcolumnIndex.put(key, values);
            }
        } else if (currColType.equalsIgnoreCase("DATETIME")) {
            while (currIndexFile.getFilePointer() < fileLength) {   // not reach the end of the file
                long key = currIndexFile.readLong();
                int valNum = currIndexFile.readInt();
                ArrayList<Integer> values = new ArrayList<>();
                values.add(valNum);
                for (int i = 0; i < valNum; i++) {
                    values.add(currIndexFile.readInt());
                }
                currcolumnIndex.put(key, values);
            }
        } else if (currColType.equalsIgnoreCase("DATE")) {
            while (currIndexFile.getFilePointer() < fileLength) {   // not reach the end of the file
                long key = currIndexFile.readLong();
                int valNum = currIndexFile.readInt();
                ArrayList<Integer> values = new ArrayList<>();
                values.add(valNum);
                for (int i = 0; i < valNum; i++) {
                    values.add(currIndexFile.readInt());
                }
                currcolumnIndex.put(key, values);
            }
        } else if (currColType.charAt(0) == 'C' || currColType.charAt(0) == 'c') {  // CHAR(n)
            int len = Integer.parseInt(currColType.substring(5, currColType.length() - 1));
            while (currIndexFile.getFilePointer() < fileLength) {   // not reach the end of the file               
                String key = "";
                for (int i = 0; i < len; i++) {
                    key += (char)currIndexFile.readByte();  //!!!!!!!!!!! len
                }
                int valNum = currIndexFile.readInt();
                ArrayList<Integer> values = new ArrayList<>();
                values.add(valNum);
                for (int i = 0; i < valNum; i++) {
                    values.add(currIndexFile.readInt());
                }
                currcolumnIndex.put(key, values);
            }
        } else if (currColType.charAt(0) == 'V' || currColType.charAt(0) == 'v') {  //VARCHAR(n)
            // int len = Integer.parseInt(currColType.substring(8, currColType.length() - 1)); 
            while (currIndexFile.getFilePointer() < fileLength) {   // not reach the end of the file               
                int len = currIndexFile.readByte();
                String key = "";
                for (int i = 0; i < len; i++) {
                    key += (char)currIndexFile.readByte();
                }
                int valNum = currIndexFile.readInt();
                ArrayList<Integer> values = new ArrayList<>();
                values.add(valNum);
                for (int i = 0; i < valNum; i++) {
                    values.add(currIndexFile.readInt());
                }
                currcolumnIndex.put(key, values);
            }
        } else {    // unsupported data type
            
        } 
        return currcolumnIndex;
    }
    
    /**
     * overwrite the ndx file with information in TreeMap
     * @param currIndexFile
     * @param currColType
     * @param currcolumnIndex
     * @throws IOException
     */
    protected static void updateIndexFile(RandomAccessFile currIndexFile, String currColType, TreeMap<Object, ArrayList<Integer>> currcolumnIndex) throws IOException {
        
        if (currColType.equalsIgnoreCase("BYTE")) {
            for(Entry<Object, ArrayList<Integer>> entry : currcolumnIndex.entrySet()) {
                byte key = (byte)entry.getKey();    // get the index key
                currIndexFile.writeByte(key);
                ArrayList<Integer> values = entry.getValue();   // get the list of record addresses
                for(int i = 0; i < values.size(); i++) {
                    currIndexFile.writeInt(values.get(i));
                }
            }  
        } else if (currColType.equalsIgnoreCase("SHORT") || currColType.equalsIgnoreCase("SHORT INT")) {
            for(Entry<Object, ArrayList<Integer>> entry : currcolumnIndex.entrySet()) {
                short key = (short)entry.getKey();    // get the index key
                currIndexFile.writeShort(key);
                ArrayList<Integer> values = entry.getValue();   // get the list of record addresses
                for(int i = 0; i < values.size(); i++) {
                    currIndexFile.writeInt(values.get(i));
                }
            }  
        } else if (currColType.equalsIgnoreCase("INT")) {
            for(Entry<Object, ArrayList<Integer>> entry : currcolumnIndex.entrySet()) {
                int key = (int)entry.getKey();    // get the index key
                currIndexFile.writeInt(key);
                ArrayList<Integer> values = entry.getValue();   // get the list of record addresses
                for(int i = 0; i < values.size(); i++) {
                    currIndexFile.writeInt(values.get(i));
                }
            } 
        } else if (currColType.equalsIgnoreCase("LONG") || currColType.equalsIgnoreCase("LONG INT")) {
            for(Entry<Object, ArrayList<Integer>> entry : currcolumnIndex.entrySet()) {
                long key = (long)entry.getKey();    // get the index key
                currIndexFile.writeLong(key);
                ArrayList<Integer> values = entry.getValue();   // get the list of record addresses
                for(int i = 0; i < values.size(); i++) {
                    currIndexFile.writeInt(values.get(i));
                }
            }
        } else if (currColType.equalsIgnoreCase("FLOAT")) {
            for(Entry<Object, ArrayList<Integer>> entry : currcolumnIndex.entrySet()) {
                float key = (float)entry.getKey();    // get the index key
                currIndexFile.writeFloat(key);
                ArrayList<Integer> values = entry.getValue();   // get the list of record addresses
                for(int i = 0; i < values.size(); i++) {
                    currIndexFile.writeInt(values.get(i));
                }
            }
        } else if (currColType.equalsIgnoreCase("DOUBLE")) {
            for(Entry<Object, ArrayList<Integer>> entry : currcolumnIndex.entrySet()) {
                double key = (double)entry.getKey();    // get the index key
                currIndexFile.writeDouble(key);
                ArrayList<Integer> values = entry.getValue();   // get the list of record addresses
                for(int i = 0; i < values.size(); i++) {
                    currIndexFile.writeInt(values.get(i));
                }
            }
        } else if (currColType.equalsIgnoreCase("DATETIME")) {
            for(Entry<Object, ArrayList<Integer>> entry : currcolumnIndex.entrySet()) {
                long key = (long)entry.getKey();    // get the index key
                currIndexFile.writeLong(key);
                ArrayList<Integer> values = entry.getValue();   // get the list of record addresses
                for(int i = 0; i < values.size(); i++) {
                    currIndexFile.writeInt(values.get(i));
                }
            }
        } else if (currColType.equalsIgnoreCase("DATE")) {
            for(Entry<Object, ArrayList<Integer>> entry : currcolumnIndex.entrySet()) {
                long key = (long)entry.getKey();    // get the index key
                currIndexFile.writeLong(key);
                ArrayList<Integer> values = entry.getValue();   // get the list of record addresses
                for(int i = 0; i < values.size(); i++) {
                    currIndexFile.writeInt(values.get(i));
                }
            }
        } else if (currColType.charAt(0) == 'C' || currColType.charAt(0) == 'c') {  // CHAR(n)
            int len = Integer.parseInt(currColType.substring(5, currColType.length() - 1));
            for(Entry<Object, ArrayList<Integer>> entry : currcolumnIndex.entrySet()) {
                String key = (String)entry.getKey();    // get the index key
                currIndexFile.writeBytes(key);
                for (int i = 0; i < len - key.length(); i++) {
                    currIndexFile.writeByte('\0');  //!!!!!!!!!!!!!
                }
                ArrayList<Integer> values = entry.getValue();   // get the list of record addresses
                for(int i = 0; i < values.size(); i++) {
                    currIndexFile.writeInt(values.get(i));
                }
            }
        } else if (currColType.charAt(0) == 'V' || currColType.charAt(0) == 'v') {  //VARCHAR(n)
            // int len = Integer.parseInt(currColType.substring(8, currColType.length() - 1)); 
            for(Entry<Object, ArrayList<Integer>> entry : currcolumnIndex.entrySet()) {
                String key = (String)entry.getKey();    // get the index key
                currIndexFile.writeByte(key.length());
                currIndexFile.writeBytes(key);
                ArrayList<Integer> values = entry.getValue();   // get the list of record addresses
                for(int i = 0; i < values.size(); i++) {
                    currIndexFile.writeInt(values.get(i));
                }
            }
        } else {    // unsupported data type
            
        } 
    }
    
    /**
     * furthr parse the user input and update TreeMap and get values to insert
     * @param colsInfOfTable
     * @param i
     * @param valueTokens
     * @param currcolumnIndex
     * @param fileLenBeforeInsert
     * @return
     */
    protected static Object updateTreeMap(LinkedHashMap<Integer, ArrayList<String>> colsInfOfTable, int i, String[] valueTokens, TreeMap<Object, ArrayList<Integer>> currcolumnIndex, long fileLenBeforeInsert) {
        Object valToInsert = null;
        boolean errorOccured = false;
        String currColName = colsInfOfTable.get(i + 1).get(0);
        String currColType = colsInfOfTable.get(i + 1).get(1);
        String currIsNullable = colsInfOfTable.get(i + 1).get(2);
        String currIsPriKey = colsInfOfTable.get(i + 1).get(3);
        if (valueTokens[i].equalsIgnoreCase("NULL")) {  // the value to insert is null
            if (currIsNullable.equalsIgnoreCase("NO")) {
                System.out.println("Request Rejected! The column " + currColName + " is not nullable.");
                errorOccured = true;
            } else {    // in input null is allowed
                if (currColType.equalsIgnoreCase("BYTE")) {
                    valToInsert = Byte.MIN_VALUE;   // ***use the min value to represent null
                } else if (currColType.equalsIgnoreCase("SHORT") || currColType.equalsIgnoreCase("SHORT INT")) {
                    valToInsert = Short.MIN_VALUE;   // ***use the min value to represent null
                } else if (currColType.equalsIgnoreCase("INT")) {
                    valToInsert = Integer.MIN_VALUE;   // ***use the min value to represent null
                } else if (currColType.equalsIgnoreCase("LONG") || currColType.equalsIgnoreCase("LONG INT")) {
                    valToInsert = Long.MIN_VALUE;   // ***use the min value to represent null
                } else if (currColType.equalsIgnoreCase("FLOAT")) {
                    valToInsert = Float.MIN_VALUE;   // ***use the min value to represent null
                } else if (currColType.equalsIgnoreCase("DOUBLE")) {
                    valToInsert = Double.MIN_VALUE;   // ***use the min value to represent null
                } else if (currColType.equalsIgnoreCase("DATETIME")) {
                    valToInsert = Long.MIN_VALUE;   // ***use the min value to represent null
                } else if (currColType.equalsIgnoreCase("DATE")) {
                    valToInsert = Long.MIN_VALUE;   // ***use the min value to represent null
                } else if (currColType.charAt(0) == 'C' || currColType.charAt(0) == 'c') {  // CHAR(n)
                    int len = Integer.parseInt(currColType.substring(5, currColType.length() - 1));
                    String x = "";
                    for (int j = 0; j < len; j++) {
                        x += '\0';
                    }
                    valToInsert = x;   // ***use fixed length '\0' to represent null
                } else if (currColType.charAt(0) == 'V' || currColType.charAt(0) == 'v') {  //VARCHAR(n)
                    // int len = Integer.parseInt(currColType.substring(8, currColType.length() - 1));
                    valToInsert = '\0';   // ***use '\0' to represent null
                } else {    // unsupported data type
                    
                }
                
                if (currcolumnIndex.containsKey(valToInsert)) {
                    ArrayList<Integer> addrsOri = currcolumnIndex.get(valToInsert);
                    ArrayList<Integer> addrs = new ArrayList<>();
                    addrs.add(addrsOri.get(0) + 1);
                    for (int j = 1; j < addrsOri.size(); j++) {
                        addrs.add(addrsOri.get(j));
                    }
                    currcolumnIndex.put(valToInsert, addrs);    
                } else {
                    ArrayList<Integer> addrs = new ArrayList<>();
                    addrs.add(1);
                    addrs.add((int)fileLenBeforeInsert);
                    currcolumnIndex.put(valToInsert, addrs);
                }
            }
        } else {    // the value to insert is not null
            try {
                if (currColType.equalsIgnoreCase("BYTE")) {
                    valToInsert = Byte.parseByte(valueTokens[i]);
                } else if (currColType.equalsIgnoreCase("SHORT") || currColType.equalsIgnoreCase("SHORT INT")) {
                    valToInsert = Short.parseShort(valueTokens[i]);
                } else if (currColType.equalsIgnoreCase("INT")) {
                    valToInsert = Integer.parseInt(valueTokens[i]);
                } else if (currColType.equalsIgnoreCase("LONG") || currColType.equalsIgnoreCase("LONG INT")) {
                    valToInsert = Long.parseLong(valueTokens[i]);
                } else if (currColType.equalsIgnoreCase("FLOAT")) {
                    valToInsert = Float.parseFloat(valueTokens[i]);
                } else if (currColType.equalsIgnoreCase("DOUBLE")) {
                    valToInsert = Double.parseDouble(valueTokens[i]);
                } else if (currColType.equalsIgnoreCase("DATETIME")) {
                    String x = valueTokens[i].substring(1, valueTokens[i].length() - 1); // eliminate the '' symbol
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                    valToInsert = dateFormat.parse(x).getTime(); 
                } else if (currColType.equalsIgnoreCase("DATE")) {
                    String x = valueTokens[i].substring(1, valueTokens[i].length() - 1); // eliminate the '' symbol
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    valToInsert = dateFormat.parse(x).getTime(); 
                } else if (currColType.charAt(0) == 'C' || currColType.charAt(0) == 'c') {  // CHAR(n)
                    int len = Integer.parseInt(currColType.substring(5, currColType.length() - 1));
                    String x = valueTokens[i].substring(1, valueTokens[i].length() - 1); // eliminate the '' symbol   
                    valToInsert = x;
                    if (x.length() != len) {
                        System.out.println("Request Rejected! The value input has wrong format.");
                        errorOccured = true;
                    }
                } else if (currColType.charAt(0) == 'V' || currColType.charAt(0) == 'v') {  //VARCHAR(n)
                    int len = Integer.parseInt(currColType.substring(8, currColType.length() - 1));
                    String x = valueTokens[i].substring(1, valueTokens[i].length() - 1); // eliminate the '' symbol   
                    valToInsert = x;
                    if (x.length() > len) {
                        System.out.println("Request Rejected! The value input has wrong format.");
                        errorOccured = true;
                    }
                } else {    // unsupported data type
                    
                }
                
            } catch (NumberFormatException e1) {
                System.out.println("Request Rejected! The value input has wrong format.");
                errorOccured = true;
            } catch (ParseException e) {
                System.out.println("Request Rejected! The value input has wrong format.");
                errorOccured = true;
            }
            
            if (currIsPriKey.equalsIgnoreCase("PRI")) { // primary key
                if (currcolumnIndex.containsKey(valToInsert)) {
                    System.out.println("Request Rejected! There exists a row in the table with primary key " + valToInsert + ".");
                    errorOccured = true;
                } else {
                    ArrayList<Integer> addrs = new ArrayList<>();
                    addrs.add(1);
                    addrs.add((int)fileLenBeforeInsert);
                    currcolumnIndex.put(valToInsert, addrs);
                }
            } else {    // not primary key
                if (currcolumnIndex.containsKey(valToInsert)) {
                    ArrayList<Integer> addrsOri = currcolumnIndex.get(valToInsert);
                    ArrayList<Integer> addrs = new ArrayList<>();
                    addrs.add(addrsOri.get(0) + 1);
                    for (int j = 1; j < addrsOri.size(); j++) {
                        addrs.add(addrsOri.get(j));
                    }
                    currcolumnIndex.put(valToInsert, addrs);    
                } else {
                    ArrayList<Integer> addrs = new ArrayList<>();
                    addrs.add(1);
                    addrs.add((int)fileLenBeforeInsert);
                    currcolumnIndex.put(valToInsert, addrs);
                }
            }
        }
        if (errorOccured) {
            return null;
        } else {
            return valToInsert;
        }
    }
    
    protected static void writeToTable(RandomAccessFile tableFile, String currColType, Object currValue) throws IOException {
        
        if (currColType.equalsIgnoreCase("BYTE")) {
            byte value = Byte.parseByte(currValue.toString());
            tableFile.writeByte(value);
        } else if (currColType.equalsIgnoreCase("SHORT") || currColType.equalsIgnoreCase("SHORT INT")) {
            short value = Short.parseShort(currValue.toString());
            tableFile.writeShort(value);
        } else if (currColType.equalsIgnoreCase("INT")) {
            int value = Integer.parseInt(currValue.toString());
            tableFile.writeInt(value);
        } else if (currColType.equalsIgnoreCase("LONG") || currColType.equalsIgnoreCase("LONG INT")) {
            long value = Long.parseLong(currValue.toString());
            tableFile.writeLong(value);
        } else if (currColType.equalsIgnoreCase("FLOAT")) {
            float value = Float.parseFloat(currValue.toString());
            tableFile.writeFloat(value);
        } else if (currColType.equalsIgnoreCase("DOUBLE")) {
            double value = Double.parseDouble(currValue.toString());
            tableFile.writeDouble(value);
        } else if (currColType.equalsIgnoreCase("DATETIME")) {
            long value = Long.parseLong(currValue.toString());
            tableFile.writeLong(value);
        } else if (currColType.equalsIgnoreCase("DATE")) {
            long value = Long.parseLong(currValue.toString());
            tableFile.writeLong(value);
        } else if (currColType.charAt(0) == 'C' || currColType.charAt(0) == 'c') {  // CHAR(n)
            //int len = Integer.parseInt(currColType.substring(5, currColType.length() - 1));
            String value = currValue.toString();
            tableFile.writeBytes(value);
        } else if (currColType.charAt(0) == 'V' || currColType.charAt(0) == 'v') {  //VARCHAR(n)
            // int len = Integer.parseInt(currColType.substring(8, currColType.length() - 1));
            String value = currValue.toString();
            tableFile.writeByte(value.length());
            tableFile.writeBytes(value);
        } else {    // unsupported data type
            
        }
        
    }
    
    
    /**
     * process the request to insert values to the passed table under the currently active schema (database)
     * @param tableToInsert
     * @param valueTokens
     */
    protected static void insertValues(String tableToInsert, String[] valueTokens) {
        try {
            long[] tableInf = getTableRowNum(tableToInsert);
            if (tableInf[0] < 0) {   // the table doesn't exist
                System.out.println("Request Rejected! The table " + tableToInsert + " does not exist in the schema " + dbActive + ".");
            } else {
                RandomAccessFile tablesTableFile = new RandomAccessFile(tablesTableFileName, "rw");
                RandomAccessFile columnsTableFile = new RandomAccessFile(columnsTableFileName, "rw");
                
                // get columns information of this table
                LinkedHashMap<Integer, ArrayList<String>> colsInfOfTable = getColsInfOfTable(tableToInsert);
                int colsNum = colsInfOfTable.size();
                
                // load ndx files of this table to TreeMap
                String dbFolderName = dataFolderName + "/" + dbActive;
                ArrayList<TreeMap<Object, ArrayList<Integer>>> columnIndexMaps = new ArrayList<>();
                for (int i = 0; i < colsNum; i++) {
                    TreeMap<Object, ArrayList<Integer>> currcolumnIndex = null;  //!!!
                    String currColName = colsInfOfTable.get(i + 1).get(0);
                    
                    String currColType = colsInfOfTable.get(i + 1).get(1);
                    String currIndexFileName = dbFolderName + "/" + dbActive + "." + tableToInsert + "." + currColName + ".ndx";                    
                    RandomAccessFile currIndexFile = new RandomAccessFile(currIndexFileName, "rw");
                    
                    currcolumnIndex = loadIndexFile(currIndexFile, currColType);
                    columnIndexMaps.add(currcolumnIndex);
                    
                    currIndexFile.close();
                }
                
               
                String tableLocation = dbFolderName + "/" + dbActive + "." + tableToInsert + ".tbl";
                RandomAccessFile tableFile = new RandomAccessFile(tableLocation, "rw");
                long fileLenBeforeInsert = tableFile.length();
                
                ArrayList<Object> valuesFinal = new ArrayList<>();  //!!!!!!!!!!!! store all the parsed values to insert
                boolean errorOccured = false;
                if (valueTokens.length > colsNum) { // more attribute values than needed is entered
                    syntaxError();
                    errorOccured = true;
                } else {
                    // input values are not enough for all columns, so append null as extra input
                    if (valueTokens.length < colsNum) {
                        String[] newvalueTokens = new String[colsNum];
                        int i;
                        for (i = 0 ; i < valueTokens.length; i++) {
                            newvalueTokens[i] = valueTokens[i];
                        }
                        for (; i < colsNum; i++) {
                            newvalueTokens[i] = "null";
                        }
                        valueTokens = newvalueTokens;
                    }
                    
                    // update TreeMap  !!!!!!!!!!!!!!!!!
                    for (int i = 0 ; i < colsNum; i++) {
                        /*
                        String currColName = colsInfOfTable.get(i + 1).get(0);
                        String currColType = colsInfOfTable.get(i + 1).get(1);
                        String currIsNullable = colsInfOfTable.get(i + 1).get(2);
                        String currIsPriKey = colsInfOfTable.get(i + 1).get(3);
                        */
                        TreeMap<Object, ArrayList<Integer>> currcolumnIndex = columnIndexMaps.get(i);
                        
                        Object valToInsert = null;  //!!!!!!!!!!!!
                        //!!!!!!!!!!!!!!!!!
                        valToInsert = updateTreeMap(colsInfOfTable, i, valueTokens, currcolumnIndex, fileLenBeforeInsert);
                        
                        if (valToInsert == null) {
                            errorOccured = true;
                        }
                        
                        valuesFinal.add(valToInsert);   
                    }
                }
                
                if (!errorOccured) {
                    // append a new row to the tbl file of this table  !!!!!!!!!!!!!!!
                    tableFile.seek(fileLenBeforeInsert);
                    for (int i = 0 ; i < colsNum; i++) {
                        String currColType = colsInfOfTable.get(i + 1).get(1);
                        Object currValue = valuesFinal.get(i);
                        writeToTable(tableFile, currColType, currValue);
                    }
                    
                    // write TreeMap back to ndx files (overwrite)
                    for (int i = 0; i < colsNum; i++) {
                        TreeMap<Object, ArrayList<Integer>> currcolumnIndex = null; //!!!
                        String currColName = colsInfOfTable.get(i + 1).get(0);
                        String currColType = colsInfOfTable.get(i + 1).get(1);
                        String currIndexFileName = dbFolderName + "/" + dbActive + "." + tableToInsert + "." + currColName + ".ndx";                    
                        RandomAccessFile currIndexFile = new RandomAccessFile(currIndexFileName, "rw");
                        
                        currcolumnIndex = columnIndexMaps.get(i);
                        updateIndexFile(currIndexFile, currColType, currcolumnIndex);
                        
                        currIndexFile.close();
                    }
                    
                    // update TABLE_ROWS in the information_schema: TABLE_ROWS++
                    tablesTableFile.seek(tableInf[1]);
                    tablesTableFile.writeLong(tableInf[0] + 1);
                    
                    System.out.println("SUCCEED! The new row is successfully inserted.");
                }
                
                tableFile.close();
                tablesTableFile.close();
                columnsTableFile.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * read content of passed currColType type from the passed tableFile file, starting from the current file pointer position
     * @param tableFile
     * @param currColType
     * @return
     * @throws IOException
     */
    protected static String readColumnValue(RandomAccessFile tableFile, String currColType) throws IOException {
        String columnValue = "";
        if (currColType.equalsIgnoreCase("BYTE")) {
            columnValue += tableFile.readByte();
        } else if (currColType.equalsIgnoreCase("SHORT") || currColType.equalsIgnoreCase("SHORT INT")) {
            columnValue += tableFile.readShort();
        } else if (currColType.equalsIgnoreCase("INT")) {
            columnValue += tableFile.readInt();
        } else if (currColType.equalsIgnoreCase("LONG") || currColType.equalsIgnoreCase("LONG INT")) {
            columnValue += tableFile.readLong();
        } else if (currColType.equalsIgnoreCase("FLOAT")) {
            columnValue += tableFile.readFloat();
        } else if (currColType.equalsIgnoreCase("DOUBLE")) {
            columnValue += tableFile.readDouble();
        } else if (currColType.equalsIgnoreCase("DATETIME")) {
            Date time = new Date(tableFile.readLong());
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
            columnValue = dateFormat.format(time);
        } else if (currColType.equalsIgnoreCase("DATE")) {
            Date time = new Date(tableFile.readLong());
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            columnValue = dateFormat.format(time);
        } else if (currColType.charAt(0) == 'C' || currColType.charAt(0) == 'c') {  // CHAR(n)
            int len = Integer.parseInt(currColType.substring(5, currColType.length() - 1));
            for (int i = 0; i < len; i++) {
                columnValue += (char)tableFile.readByte();  //!!!!!!!!!!!
            }
        } else if (currColType.charAt(0) == 'V' || currColType.charAt(0) == 'v') {  //VARCHAR(n)
            // int len = Integer.parseInt(currColType.substring(8, currColType.length() - 1));
            int len = tableFile.readByte();
            for (int i = 0; i < len; i++) {
                columnValue += (char)tableFile.readByte();
            } 
        } else {    // unsupported data type
            
        }
        return columnValue;
    }
       
    /**
     * process the request to select all rows of the passed table
     * @param tableToSelectFrom
     */
    protected static void selectAllFromTable(String tableToSelectFrom) {
        try {
            long rowNum = getTableRowNum(tableToSelectFrom)[0];   // the row count of the passed table
            
            if (rowNum < 0) {   // the table doesn't exist
                System.out.println("Request Rejected! The table " + tableToSelectFrom + " you queried does not exist.");
            } else {
                LinkedHashMap<Integer, ArrayList<String>> colsInfOfTable = getColsInfOfTable(tableToSelectFrom);
                int colsNum = colsInfOfTable.size();
                
                // print the table titles
                System.out.println(tbSperateLine(66));
                for (int j = 1; j <= colsNum; j++) {
                    System.out.print("| " + colsInfOfTable.get(j).get(0) + "\t"); 
                }
                System.out.println("| ");       
                System.out.println(tbSperateLine(66));
                
                // print the table content
                if (rowNum > 0) {  // the table contains at least one row
                    String tableLocation = dataFolderName + "/" + dbActive + "/" + dbActive + "." + tableToSelectFrom + ".tbl";
                    RandomAccessFile tableFile = new RandomAccessFile(tableLocation, "rw");
                    
                    for (int j = 1; j <= rowNum; j++) { // print each row
                        for (int k = 1; k <= colsNum; k++) {   // within each row, print each attribute value
                            String currColType = colsInfOfTable.get(k).get(1);
                            System.out.print("| " + readColumnValue(tableFile, currColType) + "\t");
                        }
                        System.out.println("| ");
                    }
                    
                    tableFile.close();
                }
                System.out.println(tbSperateLine(66));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * get an arraylist of table row addresses according to the where selection condition
     * @param currIndexFile
     * @param currColType
     * @param whereColValue
     * @param operator
     * @return
     * @throws IOException 
     */
    protected static ArrayList<Integer> getAddrListOfSelection(RandomAccessFile currIndexFile, String currColType, String whereColValue, String operator) throws IOException {
        ArrayList<Integer> addrList = new ArrayList<>();
        long fileLength = currIndexFile.length();
        boolean errorOccured = false;
        
        try {
            if (currColType.equalsIgnoreCase("BYTE")) {
                byte whereValue = Byte.parseByte(whereColValue);
                while (currIndexFile.getFilePointer() < fileLength) {   // not reach the end of the file
                    byte key = currIndexFile.readByte();
                    if (operator.equals(">=")) {
                        if (key >= whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<=")) {
                        if (key <= whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("=")) {
                        if (key == whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<>")) {
                        if (key != whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals(">")) {
                        if (key > whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<")) {
                        if (key < whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addresses to the list
                            }
                        }
                    }   
                }  
            } else if (currColType.equalsIgnoreCase("SHORT") || currColType.equalsIgnoreCase("SHORT INT")) {
                short whereValue = Short.parseShort(whereColValue);
                while (currIndexFile.getFilePointer() < fileLength) {   // not reach the end of the file
                    short key = currIndexFile.readShort();
                    if (operator.equals(">=")) {
                        if (key >= whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<=")) {
                        if (key <= whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("=")) {
                        if (key == whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<>")) {
                        if (key != whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals(">")) {
                        if (key > whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<")) {
                        if (key < whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addresses to the list
                            }
                        }
                    }   
                    
                }
            } else if (currColType.equalsIgnoreCase("INT")) {
                int whereValue = Integer.parseInt(whereColValue);
                while (currIndexFile.getFilePointer() < fileLength) {   // not reach the end of the file
                    int key = currIndexFile.readInt();
                    if (operator.equals(">=")) {
                        if (key >= whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<=")) {
                        if (key <= whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("=")) {
                        if (key == whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<>")) {
                        if (key != whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals(">")) {
                        if (key > whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<")) {
                        if (key < whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addresses to the list
                            }
                        }
                    }   
                }
            } else if (currColType.equalsIgnoreCase("LONG") || currColType.equalsIgnoreCase("LONG INT")) {
                long whereValue = Long.parseLong(whereColValue);
                while (currIndexFile.getFilePointer() < fileLength) {   // not reach the end of the file
                    long key = currIndexFile.readLong();
                    if (operator.equals(">=")) {
                        if (key >= whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<=")) {
                        if (key <= whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("=")) {
                        if (key == whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<>")) {
                        if (key != whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals(">")) {
                        if (key > whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<")) {
                        if (key < whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addresses to the list
                            }
                        }
                    }   
                }
            } else if (currColType.equalsIgnoreCase("FLOAT")) {
                float whereValue = Float.parseFloat(whereColValue);
                while (currIndexFile.getFilePointer() < fileLength) {   // not reach the end of the file
                    float key = currIndexFile.readFloat();
                    if (operator.equals(">=")) {
                        if (key >= whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<=")) {
                        if (key <= whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("=")) {
                        if (key == whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<>")) {
                        if (key != whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals(">")) {
                        if (key > whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<")) {
                        if (key < whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addresses to the list
                            }
                        }
                    }   
                }
            } else if (currColType.equalsIgnoreCase("DOUBLE")) {
                double whereValue = Double.parseDouble(whereColValue);
                while (currIndexFile.getFilePointer() < fileLength) {   // not reach the end of the file
                    double key = currIndexFile.readDouble();
                    if (operator.equals(">=")) {
                        if (key >= whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<=")) {
                        if (key <= whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("=")) {
                        if (key == whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<>")) {
                        if (key != whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals(">")) {
                        if (key > whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<")) {
                        if (key < whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addresses to the list
                            }
                        }
                    }   
                }
            } else if (currColType.equalsIgnoreCase("DATETIME")) {
                String x = whereColValue.substring(1, whereColValue.length() - 1); // eliminate the '' symbol
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                long whereValue = dateFormat.parse(x).getTime();
                while (currIndexFile.getFilePointer() < fileLength) {   // not reach the end of the file
                    long key = currIndexFile.readLong();
                    if (operator.equals(">=")) {
                        if (key >= whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<=")) {
                        if (key <= whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("=")) {
                        if (key == whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<>")) {
                        if (key != whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals(">")) {
                        if (key > whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<")) {
                        if (key < whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addresses to the list
                            }
                        }
                    }   
                }
            } else if (currColType.equalsIgnoreCase("DATE")) {
                String x = whereColValue.substring(1, whereColValue.length() - 1); // eliminate the '' symbol
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                long whereValue = dateFormat.parse(x).getTime(); 
                while (currIndexFile.getFilePointer() < fileLength) {   // not reach the end of the file
                    long key = currIndexFile.readLong();
                    if (operator.equals(">=")) {
                        if (key >= whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<=")) {
                        if (key <= whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("=")) {
                        if (key == whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<>")) {
                        if (key != whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals(">")) {
                        if (key > whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<")) {
                        if (key < whereValue) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addresses to the list
                            }
                        }
                    }   
                }
            } else if (currColType.charAt(0) == 'C' || currColType.charAt(0) == 'c') {  // CHAR(n)
                int len = Integer.parseInt(currColType.substring(5, currColType.length() - 1));
                String x = whereColValue.substring(1, whereColValue.length() - 1); // eliminate the '' symbol   
                String whereValue = x;
                while (currIndexFile.getFilePointer() < fileLength) {   // not reach the end of the file               
                    String key = "";
                    for (int i = 0; i < len; i++) {
                        key += (char)currIndexFile.readByte();  //!!!!!!!!!!! len
                    }
                    int cmp = key.compareTo(whereValue);
                    if (operator.equals(">=")) {
                        if (cmp >= 0) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<=")) {
                        if (cmp <= 0) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("=")) {
                        if (cmp == 0) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<>")) {
                        if (cmp != 0) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals(">")) {
                        if (cmp > 0) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<")) {
                        if (cmp < 0) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addresses to the list
                            }
                        }
                    }                      
                }
            } else if (currColType.charAt(0) == 'V' || currColType.charAt(0) == 'v') {  //VARCHAR(n)
                // int len = Integer.parseInt(currColType.substring(8, currColType.length() - 1)); 
                String x = whereColValue.substring(1, whereColValue.length() - 1); // eliminate the '' symbol   
                String whereValue = x;
                
                while (currIndexFile.getFilePointer() < fileLength) {   // not reach the end of the file               
                    int len = currIndexFile.readByte();
                    String key = "";
                    for (int i = 0; i < len; i++) {
                        key += (char)currIndexFile.readByte();
                    }
                    int cmp = key.compareTo(whereValue);
                    if (operator.equals(">=")) {
                        if (cmp >= 0) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<=")) {
                        if (cmp <= 0) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("=")) {
                        if (cmp == 0) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<>")) {
                        if (cmp != 0) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals(">")) {
                        if (cmp > 0) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addr to the list
                            }
                        }
                    } else if (operator.equals("<")) {
                        if (cmp < 0) {    // condition satisfied, add addresses to the list
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                addrList.add(currIndexFile.readInt());
                            }
                        } else {    // condition not satisfied, go to check next key
                            int valNum = currIndexFile.readInt();
                            for (int i = 0; i < valNum; i++) {
                                currIndexFile.readInt();    // read the data but not include addresses to the list
                            }
                        }
                    }   
                }   
            } else {    // unsupported data type
                
            } 
        } catch (NumberFormatException e1) {
            System.out.println("Request Rejected! The value in where clause has wrong format.");
            errorOccured = true;
        } catch (ParseException e) {
            System.out.println("Request Rejected! The value in where clause has wrong format.");
            errorOccured = true;
        }       
        if (errorOccured) {
            return null;
        } else {
            return addrList;
        }
    }
    
    /**
     * process the request to select specific rows of the passed table according to the where condition
     * @param tableToSelectFrom
     * @param whereCondition
     */
    protected static void selectFromTable(String tableToSelectFrom, String whereCondition) {
        try {
            long rowNum = getTableRowNum(tableToSelectFrom)[0];   // the row count of the passed table
            
            if (rowNum < 0) {   // the table doesn't exist
                System.out.println("Request Rejected! The table " + tableToSelectFrom + " you queried does not exist.");
            } else {
                LinkedHashMap<Integer, ArrayList<String>> colsInfOfTable = getColsInfOfTable(tableToSelectFrom);
                int colsNum = colsInfOfTable.size();
                
                // process where condition
                String operator = null;
                String whereColName = null;
                String whereColValue = null;
                boolean errorOccurred = false;
                if (whereCondition != null) {   // there is a where condition
                    // parse where condition
                    String[] whereTokens = whereCondition.split("[<>=]");
                    if (whereCondition.contains(">=")) { // >=
                        operator = ">=";
                        if (whereTokens.length != 3) {
                            syntaxError();
                            errorOccurred = true;
                        } else {
                            whereColName = whereTokens[0];
                            whereColValue = whereTokens[2];
                        }
                    } else if (whereCondition.contains("<=")) { // <=
                        operator = "<=";
                        if (whereTokens.length != 3) {
                            syntaxError();
                            errorOccurred = true;
                        } else {
                            whereColName = whereTokens[0];
                            whereColValue = whereTokens[2];
                        }
                    } else if (whereCondition.contains("=")) {  // =
                        operator = "=";
                        if (whereTokens.length != 2) {
                            syntaxError();
                            errorOccurred = true;
                        } else {
                            whereColName = whereTokens[0];
                            whereColValue = whereTokens[1];
                        }
                    } else if (whereCondition.contains("<>")) { // !=
                        operator = "<>";
                        if (whereTokens.length != 3) {
                            syntaxError();
                            errorOccurred = true;
                        } else {
                            whereColName = whereTokens[0];
                            whereColValue = whereTokens[2];
                        }
                    } else if (whereCondition.contains(">")) {  // >
                        operator = ">";
                        if (whereTokens.length != 2) {
                            syntaxError();
                            errorOccurred = true;
                        } else {
                            whereColName = whereTokens[0];
                            whereColValue = whereTokens[1];
                        }
                    } else if (whereCondition.contains("<")) {  // <
                        operator = "<";
                        if (whereTokens.length != 2) {
                            syntaxError();
                            errorOccurred = true;
                        } else {
                            whereColName = whereTokens[0];
                            whereColValue = whereTokens[1];
                        }
                    } else {
                        syntaxError();
                        errorOccurred = true;
                    }
                    
                    if (!errorOccurred) {
                        String currColName = null;     
                        String currColType = null;
                        boolean colNameFound = false;
                        for (int i = 0; i < colsNum; i++) {
                            currColName = colsInfOfTable.get(i + 1).get(0);     
                            currColType = colsInfOfTable.get(i + 1).get(1);
                            if (whereColName.equalsIgnoreCase(currColName)) {
                                colNameFound = true;
                                break;
                            } 
                        }
                        if (colNameFound) { // if the column name in where condition exists
                            String dbFolderName = dataFolderName + "/" + dbActive;
                            String currIndexFileName = dbFolderName + "/" + dbActive + "." + tableToSelectFrom + "." + currColName + ".ndx";                    
                            RandomAccessFile currIndexFile = new RandomAccessFile(currIndexFileName, "rw");
                            
                            ArrayList<Integer> addrList = getAddrListOfSelection(currIndexFile, currColType, whereColValue, operator);
                            if (addrList != null) {
                                // print the table titles
                                System.out.println(tbSperateLine(66));
                                for (int j = 1; j <= colsNum; j++) {
                                    System.out.print("| " + colsInfOfTable.get(j).get(0) + "\t"); 
                                }
                                System.out.println("| ");       
                                System.out.println(tbSperateLine(66));
                                
                                // print the table content
                                if (addrList.size() > 0) {  // the filtered table contains at least one row
                                    String tableLocation = dataFolderName + "/" + dbActive + "/" + dbActive + "." + tableToSelectFrom + ".tbl";
                                    RandomAccessFile tableFile = new RandomAccessFile(tableLocation, "rw");
                                    
                                    for (Integer addr : addrList) {
                                        tableFile.seek(addr);
                                        for (int k = 1; k <= colsNum; k++) {   // within each row, print each attribute value
                                            currColType = colsInfOfTable.get(k).get(1);
                                            System.out.print("| " + readColumnValue(tableFile, currColType) + "\t");
                                        }
                                        System.out.println("| ");
                                    }                                   
                                    
                                    tableFile.close();
                                }
                                System.out.println(tbSperateLine(66));
                            }
                            
                            currIndexFile.close();
                        } else {
                            syntaxError();
                        }  
                    }
                    
                } else {    // there is no where condition
                    // print the table titles
                    System.out.println(tbSperateLine(66));
                    for (int j = 1; j <= colsNum; j++) {
                        System.out.print("| " + colsInfOfTable.get(j).get(0) + "\t"); 
                    }
                    System.out.println("| ");       
                    System.out.println(tbSperateLine(66));
                    
                    // print the table content
                    if (rowNum > 0) {  // the table contains at least one row
                        String tableLocation = dataFolderName + "/" + dbActive + "/" + dbActive + "." + tableToSelectFrom + ".tbl";
                        RandomAccessFile tableFile = new RandomAccessFile(tableLocation, "rw");
                        
                        for (int j = 1; j <= rowNum; j++) { // print each row
                            for (int k = 1; k <= colsNum; k++) {   // within each row, print each attribute value
                                String currColType = colsInfOfTable.get(k).get(1);
                                System.out.print("| " + readColumnValue(tableFile, currColType) + "\t");
                            }
                            System.out.println("| ");
                        }
                        
                        tableFile.close();
                    }
                    System.out.println(tbSperateLine(66));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
    // **************************************************************************
    //  MAIN METHOD
    // **************************************************************************
    public static void main(String[] args) {
        
        if (!infoSchemaExists()) {  // initialize information_schema schema if it does not exist
            InitializeInformationSchema.Initialize(infoSchemaFolderName, schemataTableFileName, tablesTableFileName, columnsTableFileName);
            System.out.println();
            System.out.println("Hello! information_schema has been initialized for you.");
        }
        
        splashScreen(); // Display the welcome splash screen
        Scanner scanner = new Scanner(System.in);
        scanner.useDelimiter(";");
        String userCommand; // Variable to collect user input from the prompt
        String cmdDelimiter = "[ ]+";
        String[] cmdTokens = null;
        
        // collect and process user commands
        do {
            System.out.print(prompt);
            userCommand = scanner.next().trim();    // get the current user command
            
            // parse the command
            cmdTokens = userCommand.split(cmdDelimiter);
            if (cmdTokens[0].equalsIgnoreCase("HELP")) {
                // finished************************
                help();
            } else if (cmdTokens[0].equalsIgnoreCase("SHOW")) {
                if (cmdTokens.length != 2) {
                    syntaxError();
                } else if (cmdTokens[1].equalsIgnoreCase("SCHEMAS")) {  // show schemas
                    // finished************************
                    showSchemas();
                } else if (cmdTokens[1].equalsIgnoreCase("TABLES")) {   // show tables
                    // finished************************
                    showTables();
                } else {
                    syntaxError();
                }     
            } else if (cmdTokens[0].equalsIgnoreCase("USE")) {
                if (cmdTokens.length != 2) {
                    syntaxError();
                } else {
                    String dbToUse = cmdTokens[1];  // use a specific database
                    // finished************************
                    useSchema(dbToUse); 
                }
            } else if (cmdTokens[0].equalsIgnoreCase("CREATE")) {
                if (cmdTokens.length == 3 && cmdTokens[1].equalsIgnoreCase("SCHEMA")) {   // create a new schema
                    String dbToCreate = cmdTokens[2];
                    // finished************************
                    createSchema(dbToCreate);
                } else if (cmdTokens.length > 3 && cmdTokens[1].equalsIgnoreCase("TABLE")) {   // create a new table
                    // further parse the userCommand
                    String cmd1 = userCommand.replaceFirst("[(]", " (((");  // whitespace is added in case that there's no whitespace between table name and the '(' symbol
                    cmd1 += "))";
                    // get name of the table to be created
                    String[] getTableNameTokens = cmd1.split("[ ]+");
                    String tableToCreate = getTableNameTokens[2];
                    // get the columns information of the table to be created
                    String[] getColInfTokens = cmd1.split("[()]{3}");
                    String[] ColInfTokens = getColInfTokens[1].split("[,]");
                    for (int i = 0; i < ColInfTokens.length; i++) {
                        ColInfTokens[i] = ColInfTokens[i].trim();   // information of each the column
                    }
                    // table under the currently active database
                    // finished************************
                    createTable(tableToCreate, ColInfTokens);
                } else {
                    syntaxError();
                }
            } else if (cmdTokens[0].equalsIgnoreCase("INSERT")) {
                if (cmdTokens.length < 4) {
                    syntaxError();
                } else if (cmdTokens[1].equalsIgnoreCase("INTO") && cmdTokens[3].substring(0,6).equalsIgnoreCase("VALUES")) {
                    // further parse the userCommand
                    String tableToInsert = cmdTokens[2];
                    
                    String cmd1 = userCommand.replaceFirst("[(]", " (((");
                    cmd1 += "))";
                    String[] getValuesToInsertTokens = cmd1.split("[()]{3}");
                    String[] valueTokens = getValuesToInsertTokens[1].split("[,]");
                    
                    for (int i = 0; i < valueTokens.length; i++) {
                        valueTokens[i] = valueTokens[i].trim(); 
                    }
                    // table under the currently active database
                    // finished************************
                    insertValues(tableToInsert, valueTokens);
                } else {
                    syntaxError();
                }
            } else if (cmdTokens[0].equalsIgnoreCase("DROP")) {
                if (cmdTokens.length == 3 && cmdTokens[1].equalsIgnoreCase("TABLE")) {
                    // String tableToDrop = cmdTokens[2];
                    // table under the currently active database
                    // this command is not required to be implemented************************
                } else {
                    syntaxError();
                }
            } else if (cmdTokens[0].equalsIgnoreCase("SELECT")) {
                if (cmdTokens.length < 4) {
                    syntaxError();
                } else if (cmdTokens[1].equalsIgnoreCase("*") && cmdTokens[2].equalsIgnoreCase("FROM")) {
                    String tableToSelectFrom = cmdTokens[3];
                    if (cmdTokens.length == 4) {    // select all rows from a specific table
                        // table under the currently active database
                        // finished************************
                        selectAllFromTable(tableToSelectFrom);
                    } else if (cmdTokens.length >= 6 && cmdTokens[4].equalsIgnoreCase("WHERE")) {    // select part of the rows from a specific table
                        String cmd1 = userCommand.replaceFirst("[W][H][E][R][E]", "WHERE(((");
                        String cmd2 = cmd1.replaceFirst("[w][h][e][r][e]", "WHERE(((");
                        String[] getTokens = cmd2.split("[(]{3}");
                        
                        String whereCondition = getTokens[1].trim();
                        // table under the currently active database
                        // finished************************
                        selectFromTable(tableToSelectFrom, whereCondition);
                    } else {
                        syntaxError();
                    }   
                } else {
                    syntaxError();
                }
            } else if (cmdTokens[0].equalsIgnoreCase("EXIT")) {
                if (cmdTokens.length == 1) {
                    // finished************************
                    break;
                } else {
                    syntaxError();
                }
            } else {
                syntaxError();
            }
        } while (true);
        scanner.close();
        System.out.println("Bye...");   
    }   // End main() method
    
}
