package rabena_yostor;

import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

public class DBApp {
	
	
	public void init() {
		
		//creating directories
		String currentDirectory = System.getProperty("user.dir");
		
		String dataPath = currentDirectory + "\\data";
		File dataFolder = new File(dataPath);
		if (!dataFolder.exists()) {
			if (!dataFolder.mkdir()) {
				//throw new DBAppException("An error occurred while creating data directory.");
			}
		}
		
		String tablesPath = currentDirectory + "\\data\\tables";
		File tablesFolder = new File(tablesPath);
		if (!tablesFolder.exists()) {
			if (!tablesFolder.mkdir()) {
				//throw new DBAppException("An error occurred while creating tables directory.");
			}
		}
		
		String configPath = currentDirectory + "\\config";
		File configFolder = new File(configPath);
		if (!configFolder.exists()) {
			if (!configFolder.mkdir()) {
				//throw new DBAppException("An error occurred while creating config directory.");
			}
		}
		
		
		//creating metadata file
		File metadata = new File(dataPath + "\\metadata.csv");
		boolean f = false;
		if (!metadata.exists()) {
			try {
				metadata.createNewFile();
				f = true;
			}
			catch (IOException i) {
				//throw new DBAppException("An error occurred while creating the metadata file.");
				i.printStackTrace();
			}
		}
		
		
		
		//initializing metadata file
		if (f) {
			try {
				FileWriter myWriter = new FileWriter(metadata);
				myWriter.write("Table Name, Column Name, Column Type, ClusteringKey, Indexed\n");
				myWriter.close();
			}
			catch (IOException i) {
				//throw new DBAppException("An error occurred while initializing to the metadata file.");
				i.printStackTrace();
			}
		}
		
		
		//creating and saving properties file
		Properties prop = new Properties();
		prop.setProperty("MaximumRowsCountinPage", "200");
		prop.setProperty("NodeSize", "15");
	
		File configFile = new File(currentDirectory + "\\config\\DBApp.properties");
		if (!configFile.exists()) {
			try {
				FileOutputStream out = new FileOutputStream(currentDirectory + "\\config\\DBApp.properties");
				prop.store(out, "");
			}
			catch (IOException i) {
				//throw new DBAppException("An error occurred while creating to the configuration file.");
				i.printStackTrace();
			}
		}
		
	}
	
	
	public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType) throws DBAppException {
		
		//check if table exists
		if (tableExists(strTableName)) {
			throw new DBAppException("Table already exists.");
		}
		
		
		//check that hashtable is not empty
		if (htblColNameType.size() == 0) {
			throw new DBAppException("No columns entered.");
		}
		
		
		//check if the column types are valid
		if (!checkValidColumnTypes(htblColNameType)) {
			throw new DBAppException("Invalid column type entered.");
		}
		
		
		//check that the clustering key column is valid
		Set<String> keys = htblColNameType.keySet();
		String type = htblColNameType.get(strClusteringKeyColumn);
		if (!keys.contains(strClusteringKeyColumn)) {
			throw new DBAppException("The entered clustering key does not match any of the entered columns.");
		}
		if (type.equals("java.lang.Boolean")) {
			throw new DBAppException("Cannot select a boolean column as a clustering key.");
		}
		
		
		//create table object and directories
		createTableDirectories(strTableName);
		
		
		//add table info to metadata file
		addTableToMetadata(strTableName, strClusteringKeyColumn, htblColNameType, keys);
		
	}
		
	
	public void createBTreeIndex(String strTableName, String strColName) throws DBAppException {
		
		String currentDirectory = System.getProperty("user.dir");
		
		//get table info
		String tablePath = currentDirectory + "\\data\\tables\\" + strTableName;
		Vector<Object> info = getTableInfo(strTableName);
		Vector<String> columnNames = (Vector<String>) info.get(0);
		Vector<String> columnTypes = (Vector<String>) info.get(1);
		Vector<String> indices = (Vector<String>) info.get(3);
		
		
		//check if table exists
		if (columnNames.size()==0)
			throw new DBAppException("Table does not exist.");
		
		
		//check if column exists
		if (!columnNames.contains(strColName))
			throw new DBAppException("Column does not exist.");
		
		
		//check if type is not polygon
		int columnIndex = columnNames.indexOf(strColName);
		if ( columnTypes.get(columnIndex).equals("java.awt.Polygon") )
			throw new DBAppException("Cannot create a B+Tree index on a column of type Polygon.");
		
		
		//check if index already exists
		if (indices.contains(strColName))
			throw new DBAppException("Index already exists.");
		
		
		//create B+tree object and directories
		String treePath = tablePath + "\\indices\\"+ strColName;
		createTreeDirectories(strTableName, strColName, treePath, "bptree");
		
		
		//modify metadata file
		editIndexInMetadata(strTableName,strColName);

		
		//insert all records in the table in the b+tree
		
		//get table object
		Table table = (Table) readObject(tablePath + "\\" + strTableName + ".ser");
		
		//loop on the table pages and insert all records in the b+tree
		for (int i=0 ; i<table.getPages().size() ; i++) {
			TablePage currentPage = (TablePage) readObject(table.getPages().get(i));
			
			//loop on the records in the page
			for (int j=0 ; j<currentPage.getRecords().size() ; j++) {
				BPTree tree = (BPTree) readObject(treePath + "\\bptree.ser");
				Comparable key = (Comparable) currentPage.getRecords().get(j).getAttributes().get(columnIndex);
				String pointer = currentPage.getPath();
				tree.insert(key, pointer);
			}
		}
		
	}
	
	
	public void createRTreeIndex(String strTableName, String strColName) throws DBAppException {

		String currentDirectory = System.getProperty("user.dir");
		
		//get table info
		String tablePath = currentDirectory + "\\data\\tables\\" + strTableName;
		Vector<Object> info = getTableInfo(strTableName);
		Vector<String> columnNames = (Vector<String>) info.get(0);
		Vector<String> columnTypes = (Vector<String>) info.get(1);
		Vector<String> indices = (Vector<String>) info.get(3);
		
		
		//check if table exists
		if (columnNames.size()==0)
			throw new DBAppException("Table does not exist.");
		
		
		//check if column exists
		if (!columnNames.contains(strColName))
			throw new DBAppException("Column does not exist.");
		
		
		//check if type is polygon
		int columnIndex = columnNames.indexOf(strColName);
		if ( !columnTypes.get(columnIndex).equals("java.awt.Polygon") )
			throw new DBAppException("Cannot create an RTree index on a column not of type Polygon.");
		
		
		//check if index already exists
		if (indices.contains(strColName))
			throw new DBAppException("Index already exists.");
		
		
		//create Rtree object and directories
		String treePath = tablePath + "\\indices\\"+ strColName;
		createTreeDirectories(strTableName, strColName, treePath, "rtree");
		
		
		//modify metadata file
		editIndexInMetadata(strTableName,strColName);
		
		
		//insert all records in the table in the b+tree
		
		//get table object
		Table table = (Table) readObject(tablePath + "\\" + strTableName + ".ser");
		
		//loop on the table pages and insert all records in the b+tree
		for (int i=0 ; i<table.getPages().size() ; i++) {
			TablePage currentPage = (TablePage) readObject(table.getPages().get(i));
			
			//loop on the records in the page
			for (int j=0 ; j<currentPage.getRecords().size() ; j++) {
				RTree tree = (RTree) readObject(treePath + "\\rtree.ser");
				Comparable key = (Comparable) currentPage.getRecords().get(j).getAttributes().get(columnIndex);
				String pointer = currentPage.getPath();
				tree.insert(key, pointer);
			}
		}
		
	}
	
	
	public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException {
		
		String currentDirectory = System.getProperty("user.dir");
		
		//get table info from metadata file
		Vector<Object> info = getTableInfo(strTableName);
		Vector<String> columnNames = (Vector<String>) info.get(0);
		Vector<String> columnTypes = (Vector<String>) info.get(1);
		String clusteringKey = (String) info.get(2);
		Vector<String> indices = (Vector<String>) info.get(3);
		
		
		//check metadata if table exists
		if (columnNames.size()==0)
			throw new DBAppException("Table does not exist.");
		
		
		//get table object
		String tablePath = currentDirectory + "\\data\\tables\\" + strTableName;
		Table t = (Table) readObject(tablePath + "\\" + strTableName + ".ser");
		
		
		//get data that user entered and check its validity
		
		//check that all the columns are entered
		if (htblColNameValue.size()!=columnNames.size())
			throw new DBAppException("Some fields are missing.");
		
		//get data from hashtable and check the column names and data types
		Vector<Object> columnValues = new Vector<Object>();
		for (int i=0 ; i<columnNames.size() ; i++) {
			Object o = htblColNameValue.get(columnNames.get(i));
			if (o==null)
				throw new DBAppException("Wrong column name(s) entered.");
			
			String type = o.getClass().getCanonicalName();
			if ( !type.equals(columnTypes.get(i)) )
				throw new DBAppException("Invalid data type entered.");
			
			columnValues.add(o);
		}
		
		
		//create the new record
		Record newRecord = new Record();
		for (int i=0; i<columnValues.size() ; i++) {
			if (columnValues.get(i) instanceof Polygon) {
				MyPolygon p = new MyPolygon((Polygon) columnValues.get(i));
				newRecord.getAttributes().add(p);
			}
			else
				newRecord.getAttributes().add(columnValues.get(i));
		}
		newRecord.getAttributes().add(new Date());
		
		
		//inserting the record in the table
		t.insert(newRecord, clusteringKey, columnNames, columnTypes, indices);
		
	}
	
	
	public void updateTable(String strTableName, String strClusteringKey, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		
		String currentDirectory = System.getProperty("user.dir");
		
		//get table info from metadata file
		Vector<Object> info = getTableInfo(strTableName);
		Vector<String> columnNames = (Vector<String>) info.get(0);
		Vector<String> columnTypes = (Vector<String>) info.get(1);
		String clusteringKey = (String) info.get(2);
		Vector<String> indices = (Vector<String>) info.get(3);
		
		
		//check metadata if table exists
		if (columnNames.size()==0)
			throw new DBAppException("Table does not exist.");
		
		
		//get table object
		String tablePath = currentDirectory + "\\data\\tables\\" + strTableName;
		Table t = (Table) readObject(tablePath + "\\" + strTableName + ".ser");
		
		
		//get data that user entered and check its validity
		Vector<Object> columnValues = checkDataEntered(htblColNameValue, columnNames, columnTypes);
		
		
		//parsing the key from the entered string and checking its format
		int keyIndex = columnNames.indexOf(clusteringKey);
		String keyType = columnTypes.get(keyIndex);
		Comparable key = checkStringOfKey(strClusteringKey, keyType);
		
		
		//updating the records of the table
		boolean rowsUpdated = t.update(key, columnValues, clusteringKey, columnNames, columnTypes, indices);
		
		
		//check if any row(s) were updated
		if (!rowsUpdated)
			throw new DBAppException("No matching record found.");
		
	}
	
	
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		
		String currentDirectory = System.getProperty("user.dir");
		
		//get table info from metadata file
		Vector<Object> info = getTableInfo(strTableName);
		Vector<String> columnNames = (Vector<String>) info.get(0);
		Vector<String> columnTypes = (Vector<String>) info.get(1);
		String clusteringKey = (String) info.get(2);
		Vector<String> indices = (Vector<String>) info.get(3);
		
		
		//check metadata if table exists
		if (columnNames.size()==0)
			throw new DBAppException("Table does not exist.");
		
		
		//get table object
		String tablePath = currentDirectory + "\\data\\tables\\" + strTableName;
		Table t = (Table) readObject(tablePath + "\\" + strTableName + ".ser");
		
		
		//get data that user entered and check its validity
		Vector<Object> columnValues = checkDataEntered(htblColNameValue, columnNames, columnTypes);
		
		
		//deleting the records from table
		boolean rowsDeleted = t.delete(columnValues, clusteringKey, columnNames, columnTypes, indices);
		
		
		//check if any row(s) were deleted
		if (!rowsDeleted)
			throw new DBAppException("No matching record found.");
		
	}
	
	
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		
		String currentDirectory = System.getProperty("user.dir");
		String strTableName= arrSQLTerms[0]._strTableName;
		
		//check if table exists
		if(!tableExists(strTableName))
			throw new DBAppException("Table does not exist.");
		
		//get table object
		String tablePath = currentDirectory + "\\data\\tables\\" + strTableName;
		Table t = (Table) readObject(tablePath+"\\"+strTableName+".ser");
		Vector<String> pages = t.getPages();
		
		//get table info from metadata file
		Vector<Object> info = getTableInfo(strTableName);
		Vector<String> columnNames = (Vector<String>) info.get(0);
		Vector<String> columnTypes = (Vector<String>) info.get(1);
		String clusteringKey = (String) info.get(2);
		Vector<String> indexedColumns = (Vector<String>) info.get(3);
		
		//check if user entered correct data
		for(int i=0;i<arrSQLTerms.length;i++) {
			String columnName = arrSQLTerms[i]._strColumnName;
			Object value = arrSQLTerms[i]._objValue;
			String operator = arrSQLTerms[i]._strOperator;
			String tableName=arrSQLTerms[i]._strTableName;
			if(!tableName.equals(strTableName))
				throw new DBAppException("Joins are not supported, please enter the same table name for all queries");
			if(!(operator.equals("=")||operator.equals("!=")||operator.equals(">")||operator.equals("<")||
					operator.equals("<=")||operator.equals(">=")))
				throw new DBAppException("Please enter a correct operator");
			if(!columnNames.contains(columnName))
				throw new DBAppException(columnName + " doesn't exist");
			else {
				int index = columnNames.indexOf(columnName);
				String valueType = value.getClass().getCanonicalName();
				if(!valueType.equals(columnTypes.get(index)))
					throw new DBAppException("Column is of the type "+columnTypes.get(index));
			}
		}
				
		//check if any of the columns we are searching for is indexed
		boolean indexed = indexedQuery(arrSQLTerms, strarrOperators, indexedColumns);
		Vector<Record> result = new Vector<Record>();
		
		if(indexed) {
			System.out.println("Accessing index...");
			Vector<String> pagesToScan = allQueryPages(arrSQLTerms, strarrOperators, indexedColumns);
			for(int i=0;i<pagesToScan.size();i++) {
				TablePage p = (TablePage)readObject(pagesToScan.get(i));
				result.addAll(p.linearSelect(arrSQLTerms, strarrOperators));
			}
		}
		if(!indexed) {
			for (int i = 0; i < pages.size(); i++) {
				TablePage p = (TablePage) readObject(pages.get(i));
				Vector<Record> r = p.linearSelect(arrSQLTerms, strarrOperators);
				result.addAll(r);
			}
		}
		
		return result.iterator();
	}
	
	
	
	
	
	public static Object readObject(String path) throws DBAppException {
		if(path == null)
			return null;
		
		Object o = null;
		try {
			FileInputStream fileIn = new FileInputStream(path);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			o = in.readObject();
			in.close();
			fileIn.close();
		}
		catch (IOException i) {
			throw new DBAppException("An error occurred while retrieving the object from disk.");
		}
		catch (ClassNotFoundException c) {
			throw new DBAppException("Object class not found.");
		}
		return o;
	}
	
	
	public static void writeObject(Object object, String objectPath) throws DBAppException {
		try {
			FileOutputStream fileOut = new FileOutputStream(objectPath);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(object);
			out.close();
			fileOut.close();
		}
		catch (IOException i) {
			throw new DBAppException("An error occurred while saving the object to the disk.");
		}
	}
	
	
	public static int getProperty(String property) throws DBAppException {
		
		String currentDirectory = System.getProperty("user.dir");
		Properties prop = new Properties();
		try {
			FileInputStream in = new FileInputStream(currentDirectory + "\\config\\DBApp.properties");
			prop.load(in);
		}
		catch (IOException i) {
			throw new DBAppException("An error occurred while retrieving the properties file.");
		}
		return Integer.parseInt(prop.getProperty(property));
	}
	
	
	public boolean tableExists(String tableName) throws DBAppException {
		
		String currentDirectory = System.getProperty("user.dir");
		try {
			BufferedReader br = new BufferedReader(new FileReader(currentDirectory + "\\data\\metadata.csv"));
			String line = br.readLine();
			while (line != null) {
				String[] info = line.split(",");
				if(info[0].equals(tableName)) {
					br.close();
					return true;
				}
				line=br.readLine();
			}
			br.close();
			return false;
		}
		catch (IOException i) {
			throw new DBAppException("An error occurred while retrieving the metadata file.");
		}
	}
	
	
	public void addTableToMetadata(String tableName, String clusteringKeyColumn, Hashtable<String,String> htblColNameType, Set<String> keys) throws DBAppException {
		
		String currentDirectory = System.getProperty("user.dir");
		String metadataPath = currentDirectory + "\\data\\metadata.csv";
		
		String s = "";
		for (String k : keys) {
			String v = htblColNameType.get(k);
			s += tableName + "," + k + "," + v + ",";
			if (k.equals(clusteringKeyColumn))
				s += "True,False\n";
			else
				s += "False,False\n";
		}

		try {
			FileWriter writer = new FileWriter(metadataPath, true);
			writer.append(s);
			writer.close();
		}
		catch (IOException e) {
			throw new DBAppException("An error occurred while writing to the metadata file.");
		}
		
	}
	
	
	public void createTableDirectories(String tableName) throws DBAppException {
		
		String currentDirectory = System.getProperty("user.dir");
		
		//create directories
		String tablePath = currentDirectory + "\\data\\tables\\" + tableName;
		File tableFolder = new File(tablePath);
		if (!tableFolder.mkdir())
			throw new DBAppException("An error occurred while creating table directory.");
		
		String pagesPath = tablePath + "\\pages";
		File pagesFolder = new File(pagesPath);
		if (!pagesFolder.mkdir())
			throw new DBAppException("An error occurred while creating pages directory.");
		
		String indicesPath = tablePath + "\\indices";
		File indicesFolder = new File(indicesPath);
		if (!indicesFolder.mkdir())
			throw new DBAppException("An error occurred while creating indices directory.");
		
		
		//create and save the table to disk
		Table newTable = new Table(tableName);
		writeObject(newTable, tablePath + "\\" + tableName + ".ser");
				
	}
	
	
	public void createTreeDirectories(String tableName, String columnName, String treePath, String treeType) throws DBAppException {
		
		//create directories
		File treeDirectory = new File(treePath);
		if (!treeDirectory.mkdir())
			throw new DBAppException("An error occurred while creating index directory.");
		
		File nodesDirectory = new File(treePath + "\\nodes");
		if (!nodesDirectory.mkdir())
			throw new DBAppException("An error occurred while creating nodes directory.");
		
		File leafPagesDirectory = new File(treePath + "\\leafPages");
		if (!leafPagesDirectory.mkdir())
			throw new DBAppException("An error occurred while creating leafPages directory.");
		
		
		//create and save the tree to disk
		Tree tree;
		if (treeType.equals("bptree"))
			tree = new BPTree(tableName,columnName,treePath);
		else
			tree = new RTree(tableName,columnName,treePath);
		
		writeObject(tree,treePath + "\\" + treeType + ".ser");
	}
	
	
	public void editIndexInMetadata(String strTableName, String strColName) throws DBAppException{
		
		String currentDirectory = System.getProperty("user.dir");
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(currentDirectory + "\\data\\metadata.csv"));
			String s = "";
			String line = br.readLine();
			while (line != null) {
				String[] data = line.split(",");
				if (data[0].equals(strTableName) && data[1].equals(strColName))
					s = s + data[0] + "," + data[1] + "," + data[2] + "," + data[3] + "," + "True" + "\n";
				else
					s = s + line + "\n";
				line = br.readLine();
			}
			br.close();
			try {
				FileWriter writer = new FileWriter(currentDirectory + "\\data\\metadata.csv", false);
				writer.append(s);
				writer.close();
			}
			catch (IOException i) {
				throw new DBAppException("An error occurred while writing to the metadata file.");
			}
		}
		catch (IOException i) {
			throw new DBAppException("An error occurred while reading the metadata file.");
		}
	
	}

	
	public boolean checkValidColumnTypes(Hashtable<String,String> htblColNameType) {
		
		Set<String> keys = htblColNameType.keySet();
		for (String k : keys) {
			String value = htblColNameType.get(k);
			if (!value.equals("java.lang.Integer") && !value.equals("java.lang.Double")
					&& !value.equals("java.lang.String") && !value.equals("java.lang.Boolean")
					&& !value.equals("java.util.Date") && !value.equals("java.awt.Polygon")) {
				return false;
			}
		}
		return true;
	}
	
	
	public Comparable checkStringOfKey(String strClusteringKey, String keyType) throws DBAppException {
		
		if (keyType.equals("java.lang.String")) {
			return strClusteringKey;
		}
		else if (keyType.equals("java.lang.Integer")) {
			try {
				return Integer.parseInt(strClusteringKey);
			}
			catch (NumberFormatException n) {
				throw new DBAppException("Invalid data entered.");
			}
		}
		else if (keyType.equals("java.lang.Double")) {
			try {
				return Double.parseDouble(strClusteringKey);
			}
			catch (NumberFormatException n) {
				throw new DBAppException("Invalid data entered.");
			}
		}
		else if (keyType.equals("java.util.Date")) {
			try {
				return new SimpleDateFormat("yyyy-MM-dd").parse(strClusteringKey);
			}
			catch (ParseException p) {
				throw new DBAppException("Invalid data entered.");
			}
		}
		else if (keyType.equals("java.awt.Polygon")) {
			// convert to our polygon class
			Polygon p = new Polygon();
			String[] str = strClusteringKey.split(",");
			for (int j = 0; j < str.length; j++) {
				str[j] = str[j].replace(")", "");
				str[j] = str[j].replace("(", "");
			}
			for (int j = 0; j < str.length - 1; j++) {
				int x = Integer.parseInt(str[j]);
				int y = Integer.parseInt(str[j + 1]);
				p.addPoint(x, y);
				j++;
			}
			return new MyPolygon(p);
		}
		
		return null;
	}
	
	
	public Vector<Object> checkDataEntered(Hashtable<String, Object> htblColNameValue, Vector<String> columnNames, Vector<String> columnTypes) throws DBAppException {
		
		//check hashtable is not empty
		if (htblColNameValue.size() == 0)
			throw new DBAppException("No data entered.");
		
		
		//check hashtable has valid column names
		Set<String> keys = htblColNameValue.keySet();
		for (String k : keys) {
			if (!columnNames.contains(k)) {
				throw new DBAppException("Wrong column name(s) entered.");
			}
		}
		
		
		//get data from hashtable
		Vector<Object> columnValues = new Vector<Object>();
		for (int i=0 ; i<columnNames.size() ; i++) {
			Object o = htblColNameValue.get(columnNames.get(i));
			if (o instanceof Polygon)
				columnValues.add(i, new MyPolygon((Polygon) o));
			else
				columnValues.add(i, o);
		}
		
		
		//check the types of the entered data
		for (int i=0 ; i<columnNames.size() ; i++) {
			Object o = htblColNameValue.get(columnNames.get(i));
			if (o==null)
				continue;
			String type = o.getClass().getCanonicalName();
			if (!type.equals(columnTypes.get(i)))
				throw new DBAppException("Invalid data type entered.");
		}
		
		return columnValues;
	}
	
	
	public static Vector<Object> getTableInfo(String tableName) throws DBAppException {
		
		//returns a vector containing the info of the table from the metadata file
		
		String currentDirectory = System.getProperty("user.dir");
		
		Vector<String> columnNames = new Vector<String>();
		Vector<String> columnTypes = new Vector<String>();
		String clusteringKey = "";
		Vector<String> indices = new Vector<String>();
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(currentDirectory+"\\data\\metadata.csv"));
			String line = br.readLine();
			while (line != null) {
				String[] info = line.split(",");
				if(info[0].equals(tableName)) {
					columnNames.add(info[1]);
					columnTypes.add(info[2]);
					if (info[3].equals("True"))
						clusteringKey = info[1];
					if (info[4].equals("True"))
						indices.add(info[1]);
				}
				line=br.readLine();
			}
			br.close();
		}
		catch (IOException i) {
			throw new DBAppException("An error occurred while retrieving the metadata file.");
		}
		
		Vector<Object> info = new Vector<Object>();
		info.add(columnNames);
		info.add(columnTypes);
		info.add(clusteringKey);
		info.add(indices);
		
		return info;
	}
	
	
	public static boolean indexedQuery(SQLTerm[] arrSQLTerms, String[] strarrOperators, Vector<String> indexedColumns) {
		
		SQLTerm last = arrSQLTerms[arrSQLTerms.length-1];
		String lastColumn = last._strColumnName;
		boolean indexed = false;
		if(indexedColumns.contains(lastColumn))
			indexed=true;
		
		if(strarrOperators.length==0)
			return indexed;

		SQLTerm[] arrSQLTermsnew = new SQLTerm[arrSQLTerms.length-1];
		String[] strarrOperatorsnew  = new String[strarrOperators.length-1];
		for(int i =0;i<arrSQLTermsnew.length;i++) {
			arrSQLTermsnew[i]=arrSQLTerms[i];
		}
		for(int i =0;i<strarrOperatorsnew.length;i++) {
			strarrOperatorsnew[i]=strarrOperators[i];
		}
		
		if(strarrOperators[strarrOperators.length-1].equals("AND"))
			return indexedQuery(arrSQLTermsnew, strarrOperatorsnew, indexedColumns) | indexed;
		else
			return indexedQuery(arrSQLTermsnew, strarrOperatorsnew, indexedColumns) & indexed;
		
	}
	
	
	public static boolean clusteredQuery(SQLTerm[] arrSQLTerms, String[] strarrOperators, String clusteringKey) {
		
		SQLTerm last = arrSQLTerms[arrSQLTerms.length-1];
		String lastColumn = last._strColumnName;
		boolean clustered = false;
		if(clusteringKey.equals(lastColumn) && !(last._strOperator.equals("!=")))
			clustered=true;
		
		if(strarrOperators.length==0)
			return clustered;

		SQLTerm[] arrSQLTermsnew = new SQLTerm[arrSQLTerms.length-1];
		String[] strarrOperatorsnew  = new String[strarrOperators.length-1];
		for(int i =0;i<arrSQLTermsnew.length;i++) {
			arrSQLTermsnew[i]=arrSQLTerms[i];
		}
		for(int i =0;i<strarrOperatorsnew.length;i++) {
			strarrOperatorsnew[i]=strarrOperators[i];
		}
		
		if(strarrOperators[strarrOperators.length-1].equals("AND"))
			return clusteredQuery(arrSQLTermsnew, strarrOperatorsnew, clusteringKey) | clustered;
		else
			return clusteredQuery(arrSQLTermsnew, strarrOperatorsnew, clusteringKey) & clustered;
		
	}
	
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Vector<String> queryPages(SQLTerm query) throws DBAppException{
		
		String strTableName= query._strTableName;
		String currentDirectory = System.getProperty("user.dir");
		String tablePath = currentDirectory + "\\data\\tables\\" + strTableName;
		
		Vector<String> indexedColumns= (Vector<String>) getTableInfo(strTableName).get(3);
		if(!indexedColumns.contains(query._strColumnName))
			return ((Table)readObject(tablePath +"\\"+strTableName+".ser")).getPages();
		
		String columnName = query._strColumnName;
		String operator = query._strOperator;
		Comparable value;
		String treePath="";
		if(!(query._objValue instanceof Polygon)) {
			value = (Comparable)query._objValue;
			treePath = tablePath + "\\indices\\"+ columnName + "\\bptree.ser";
		}
		else {
			value = new MyPolygon ((Polygon)query._objValue);
			treePath = tablePath + "\\indices\\"+ columnName + "\\rtree.ser";
		}
		Tree tree = (Tree) readObject(treePath);
		LeafNode leaf = tree.searchForLeaf(value);
		Vector<String> resultPages = new Vector<String>();
		if (operator.equals("=")) {
			for (int j = 0; j < leaf.getKeys().size(); j++) {
				if (value.compareTo(leaf.getKeys().get(j)) == 0) {
					LeafPage page = (LeafPage) readObject(leaf.getPointers().get(j));
					resultPages.addAll(page.getPointers());
					while (page.getNextPage() != null) {
						page = (LeafPage) readObject(page.getNextPage());
						resultPages.addAll(page.getPointers());
					}	
				}
			} 
		}
		if(operator.equals("!=")) {
			while(leaf.getPrevLeaf()!=null) {
				leaf = (LeafNode)readObject(leaf.getPrevLeaf());
			}
			while(leaf!=null) {
				for(int i =0;i<leaf.getKeys().size();i++) {
					if(leaf.getKeys().get(i).compareTo(value)!=0 || value instanceof MyPolygon) {
						LeafPage page = (LeafPage) readObject(leaf.getPointers().get(i));
						resultPages.addAll(page.getPointers());
						while (page.getNextPage() != null) {
							page = (LeafPage) readObject(page.getNextPage());
							resultPages.addAll(page.getPointers());
						}	
					}
				}
				leaf = (LeafNode)readObject(leaf.getNextLeaf());
			}
		}
		if(operator.equals("<")) {	
			while(leaf!=null) {
				for (int i =0;i<leaf.getKeys().size();i++) {
					if (leaf.getKeys().get(i).compareTo(value)<0) {
						LeafPage page = (LeafPage) readObject(leaf.getPointers().get(i));
						resultPages.addAll(page.getPointers());
						while (page.getNextPage() != null) {
							page = (LeafPage) readObject(page.getNextPage());
							resultPages.addAll(page.getPointers());
						} 
					} 
				}
				leaf = (LeafNode)readObject(leaf.getPrevLeaf());
			}
		}
		if(operator.equals(">")) {	
			while(leaf!=null) {
				for (int i =0;i<leaf.getKeys().size();i++) {
					if (leaf.getKeys().get(i).compareTo(value)>0) {
						LeafPage page = (LeafPage) readObject(leaf.getPointers().get(i));
						resultPages.addAll(page.getPointers());
						while (page.getNextPage() != null) {
							page = (LeafPage) readObject(page.getNextPage());
							resultPages.addAll(page.getPointers());
						} 
					} 
				}
				leaf = (LeafNode)readObject(leaf.getNextLeaf());
			}
		}
		if(operator.equals("<=")) {	
			while(leaf!=null) {
				for (int i =0;i<leaf.getKeys().size();i++) {
					if (leaf.getKeys().get(i).compareTo(value)<=0) {
						LeafPage page = (LeafPage) readObject(leaf.getPointers().get(i));
						resultPages.addAll(page.getPointers());
						while (page.getNextPage() != null) {
							page = (LeafPage) readObject(page.getNextPage());
							resultPages.addAll(page.getPointers());
						} 
					} 
				}
				leaf = (LeafNode)readObject(leaf.getPrevLeaf());
			}
		}
		if(operator.equals(">=")) {	
			while(leaf!=null) {
				for (int i =0;i<leaf.getKeys().size();i++) {
					if (leaf.getKeys().get(i).compareTo(value)>=0) {
						LeafPage page = (LeafPage) readObject(leaf.getPointers().get(i));
						resultPages.addAll(page.getPointers());
						while (page.getNextPage() != null) {
							page = (LeafPage) readObject(page.getNextPage());
							resultPages.addAll(page.getPointers());
						} 
					} 
				}
				leaf = (LeafNode)readObject(leaf.getNextLeaf());
			}
		}
		Vector<String> resultPagesUnique = new Vector<String>();
		for(int i=0;i<resultPages.size();i++) {
			if(!resultPagesUnique.contains(resultPages.get(i)))
				resultPagesUnique.add(resultPages.get(i));
		}
		resultPagesUnique.sort(null);
		return resultPagesUnique;
	}
	
	
	public static Vector<String> allQueryPages(SQLTerm[] arrSQLTerms, String[] strarrOperators, Vector<String> indexedColumns) throws DBAppException {
		
		SQLTerm last = arrSQLTerms[arrSQLTerms.length-1];
		String lastColumn = last._strColumnName;
		boolean indexed=false;
		if(indexedColumns.contains(lastColumn))
			indexed=true;
		
		if(strarrOperators.length==0) {
				return queryPages(last);
		}
		SQLTerm[] arrSQLTermsnew = new SQLTerm[arrSQLTerms.length-1];
		String[] strarrOperatorsnew  = new String[strarrOperators.length-1];
		for(int i =0;i<arrSQLTermsnew.length;i++) {
			arrSQLTermsnew[i]=arrSQLTerms[i];
		}
		for(int i =0;i<strarrOperatorsnew.length;i++) {
			strarrOperatorsnew[i]=strarrOperators[i];
		}
		
		if(strarrOperators[strarrOperators.length-1].equals("AND"))
			return intersect(allQueryPages(arrSQLTermsnew, strarrOperatorsnew, indexedColumns),queryPages(last));
		else
			return union(allQueryPages(arrSQLTermsnew, strarrOperatorsnew, indexedColumns),queryPages(last));
			
	}
	
	
	public static Vector<String> intersect(Vector<String> vector1, Vector<String> vector2){
		
		Vector<String> result=new Vector<String>();
		for(int i=0;i<vector1.size();i++) {
			if(vector2.contains(vector1.get(i)))
				result.add(vector1.get(i));
		}
		return result;
	}
	
	
	public static Vector<String> union(Vector<String> vector1, Vector<String> vector2){
		
		Vector<String> result=new Vector<String>();
		result.addAll(vector1);
		for(int i=0;i<vector2.size();i++) {
			if(!result.contains(vector2.get(i)))
				result.add(vector2.get(i));
		}
		result.sort(null);
		return result;
		
	}
	
	
}
