package rabena_yostor;

import java.io.Serializable;
import java.util.Vector;

public class Table implements Serializable {
	
	private static final long serialVersionUID = 8452807159493691524L;
	private String tableName;
	private long numOfPages;
	private Vector<String> pages;
	

	public Table(String tableName) {
		this.tableName = tableName;
		pages = new Vector<String>();
		numOfPages = 0;
	}


	public String getTableName() {
		return tableName;
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public long getNumOfPages() {
		return numOfPages;
	}

	public void setNumOfPages(long numOfPages) {
		this.numOfPages = numOfPages;
	}

	public Vector<String> getPages() {
		return pages;
	}
	
	public void setPages(Vector<String> pages) {
		this.pages = pages;
	}
	
	
	
	public void insert(Record newRecord, String clusteringKey,  Vector<String> columnNames, Vector<String> columnTypes, Vector<String> indices) throws DBAppException {
		
		//paths
		String currentDirectory = System.getProperty("user.dir");
		String tablePath = currentDirectory + "\\data\\tables\\" + tableName;
		
		//get the index of the clustering key
		int keyIndex = columnNames.indexOf(clusteringKey);
		
		
		//if it is the first insertion
		if (pages.size()==0) {
			//get maximum number of rows per page
			int maxRows = DBApp.getProperty("MaximumRowsCountinPage");
			
			//create a new page and insert the record in it
			TablePage newPage = new TablePage(maxRows, tableName, tablePath+"\\pages\\"+"page"+numOfPages+".ser");
			pages.add(newPage.getPath());
			numOfPages++;
			newPage.insert(newRecord, keyIndex);
			
			//update indices if they exist for the inserted record
			insertIntoIndices(newRecord, newPage.getPath(), columnNames, columnTypes, indices);
		}
		
		//if it is not the first insertion
		
		//if there is an index on the clustering key
		else if (indices.contains(clusteringKey)) {
			
			//load the index
			String treePath;
			if (!columnTypes.get(keyIndex).equals("java.awt.Polygon"))
				treePath = tablePath + "\\indices\\"+ clusteringKey + "\\bptree.ser";
			else
				treePath = tablePath + "\\indices\\"+ clusteringKey + "\\rtree.ser";
			Tree tree = (Tree) DBApp.readObject(treePath);
			
			
			//search for the appropriate leaf
			Comparable keyValue = (Comparable) newRecord.getAttributes().get(keyIndex);
			LeafNode leaf = tree.searchForLeaf(keyValue);
			
			
			//declare variables
			String pagePath;
			TablePage page;
			Record last = null;
			
			//if the leaf contains the key
			int indexOfKeyInLeaf = leaf.indexOf(keyValue);
			if (indexOfKeyInLeaf != -1) {
				//find the page to insert the record into
				LeafPage leafPage = (LeafPage) DBApp.readObject(leaf.getPointers().get(indexOfKeyInLeaf));
				pagePath = leafPage.getPointers().firstElement();
				page = (TablePage) DBApp.readObject(pagePath);
				last = page.insert(newRecord, keyIndex);
			}
			
			//if the leaf doesn't contain the key
			else {
				
				int indexOfGreatestLesser = leaf.indexOfGreatestLesser(keyValue);
				
				//if the key is the smallest key
				if (indexOfGreatestLesser == -1) {
					//insert the record in the first page
					pagePath = pages.get(0);
					page = (TablePage) DBApp.readObject(pagePath);
					last = page.insert(newRecord, keyIndex);
				}
				
				//if the key is not the smallest key in the leaf
				else {
					
					//get the last page of the smaller key
					LeafPage leafPage = (LeafPage) DBApp.readObject(leaf.getPointers().get(indexOfGreatestLesser));
					while (leafPage.getNextPage()!=null)
						leafPage = (LeafPage) DBApp.readObject(leafPage.getNextPage());
					pagePath = leafPage.getPointers().lastElement();
					page = (TablePage) DBApp.readObject(pagePath);
					last = page.insert(newRecord, keyIndex);
				}
				
			}
									
			
			//update indices if they exist for the inserted record
			insertIntoIndices(newRecord, pagePath, columnNames, columnTypes, indices);
			
			
			//shifting the last record if the page is full
			if (last != null) {
				int indexOfPage = pages.indexOf(pagePath) + 1;
				shiftRecords(last, indexOfPage, keyIndex, columnNames, columnTypes, indices, clusteringKey);
			}
			
		}
		
		//if there is NO index on the clustering key 
		else {
			
			//initialize variables
			Comparable keyValue = (Comparable) newRecord.getAttributes().get(keyIndex);
			String pagePath = "";
			TablePage page;
			Record last = null;
			int i;
			
			//check for the place to insert and insert the record
			for (i=0 ; i<pages.size() ; i++) {
				pagePath = pages.get(i);
				page = (TablePage) DBApp.readObject(pagePath);
				Comparable c = ((Comparable) page.getRecords().lastElement().getAttributes().get(keyIndex));
				if (c.compareTo(keyValue)>=0) {
					last = page.insert(newRecord, keyIndex);
					break;
				}
				if (i==pages.size()-1) {
					last = page.insert(newRecord, keyIndex);
					break;
				}
			}


			//update indices if they exist for the inserted record
			insertIntoIndices(newRecord, pagePath, columnNames, columnTypes, indices);
			
			
			//shifting the last record if the page is full
			if (last != null)
				shiftRecords(last, i+1, keyIndex, columnNames, columnTypes, indices, clusteringKey);
			
		}
		
		
		//save modified table
		DBApp.writeObject(this, tablePath + "\\" + tableName + ".ser");

	}
	
	
	public boolean update(Comparable key, Vector<Object> columnValues, String clusteringKey,  Vector<String> columnNames, Vector<String> columnTypes, Vector<String> indices) throws DBAppException {
		
		//check if the table is empty
		if (pages.size()==0)
			throw new DBAppException("Table is empty.");
		
		//paths
		String currentDirectory = System.getProperty("user.dir");
		String tablePath = currentDirectory + "\\data\\tables\\" + tableName;
		
		//boolean return value
		//the method returns false if no matching record was found, true otherwise
		boolean rowsUpdated = false;
		
		//get the index of the clustering key
		int clusteringKeyIndex = columnNames.indexOf(clusteringKey);
		
		
		//if there is an index on the clustering key
		if (indices.contains(clusteringKey)) {
			
			//load the index
			String treePath;
			if (!columnTypes.get(clusteringKeyIndex).equals("java.awt.Polygon"))
				treePath = tablePath + "\\indices\\"+ clusteringKey + "\\bptree.ser";
			else
				treePath = tablePath + "\\indices\\"+ clusteringKey + "\\rtree.ser";
			Tree tree = (Tree) DBApp.readObject(treePath);
			
			
			//search for the appropriate leaf
			LeafNode leaf = tree.searchForLeaf(key);
			
			
			//get the position of the key in the leaf
			int indexOfKeyInLeaf = leaf.indexOf(key);
			
			
			//check if the key exists
			if (indexOfKeyInLeaf == -1)
				return false;
			
			
			//if the keys exists load its leaf page
			LeafPage currentLeafPage = (LeafPage) DBApp.readObject(leaf.getPointers().get(indexOfKeyInLeaf));
			
			
			//loop on the leaf pages to get all table pages that contain the key
			do {
				//loop on the table pages pointers in the current leaf page 
				for (int i=0 ; i<currentLeafPage.getPointers().size() ; i++) {
					String currentTablePagePath = currentLeafPage.getPointers().get(i);
					TablePage currentTablePage = (TablePage) DBApp.readObject(currentTablePagePath);
					if (currentTablePage.update(key, columnValues, clusteringKeyIndex, columnNames, columnTypes, indices))
						rowsUpdated = true;
				}
				
				//go to next leaf page if it exists
				if (currentLeafPage.getNextPage() != null)
					currentLeafPage = (LeafPage) DBApp.readObject((currentLeafPage.getNextPage()));
				else
					currentLeafPage = null;
				
			} while (currentLeafPage != null);
			
		}
		
		//if there is NO index on the clustering key 
		else {
			
			//read each page and find out if page contains record(s) with clustering key
			for (int i=0 ; i<pages.size() ; i++) {
				String pagePath = pages.get(i);
				TablePage currentPage = (TablePage) DBApp.readObject(pagePath);
				Comparable c = ((Comparable) currentPage.getRecords().lastElement().getAttributes().get(clusteringKeyIndex));
				if (c.compareTo(key) > 0) {
					if (currentPage.update(key, columnValues, clusteringKeyIndex, columnNames, columnTypes, indices))
						rowsUpdated = true;
					break;
				} else if (c.compareTo(key) == 0) {
					if (currentPage.update(key, columnValues, clusteringKeyIndex, columnNames, columnTypes, indices))
						rowsUpdated = true;
				}
			}
			
		}
		
		return rowsUpdated;
	}
	
	
	public boolean delete(Vector<Object> columnValues, String clusteringKey,  Vector<String> columnNames, Vector<String> columnTypes, Vector<String> indices) throws DBAppException {
		
		//check if the table is empty
		if (pages.size()==0)
			throw new DBAppException("Table is empty.");
		
		//boolean return value
		//the method returns false if no matching record was found, true otherwise
		boolean rowsDeleted = false;
		
		//get the index of the clustering key
		int clusteringKeyIndex = columnNames.indexOf(clusteringKey);
		
		
		//if there are indices created on the table
		if (indices.size() != 0) {
			
			int count=0;
			for(Object o: columnValues) {
				if(o!=null)
					count++;
			}
			SQLTerm[] arrSQLTerms = new SQLTerm[count];
			int i=0;
			for(int j=0 ; j<columnValues.size() ; j++) {
				if(columnValues.get(j)!=null) {
					arrSQLTerms[i]= new SQLTerm();
					arrSQLTerms[i]._strTableName = tableName;
					arrSQLTerms[i]._strColumnName = columnNames.get(j);
					if(columnValues.get(j) instanceof MyPolygon)
						arrSQLTerms[i]._objValue=((MyPolygon)columnValues.get(j)).getPolygon();
					else
						arrSQLTerms[i]._objValue=columnValues.get(j);
					arrSQLTerms[i]._strOperator="=";
					i++;
				}		
			}
			String[] strarrOperators = new String[count-1];
			for(int j=0;j<strarrOperators.length;j++) {
				strarrOperators[j]="AND";
			}
			Vector<String> pages = DBApp.allQueryPages(arrSQLTerms, strarrOperators, indices);
			for (int j=0 ; j<pages.size() ; j++) {
				String pagePath = pages.get(j);
				TablePage currentPage = (TablePage) DBApp.readObject(pagePath);
				if (currentPage.delete(columnValues, clusteringKeyIndex, columnNames, columnTypes, indices))
					rowsDeleted = true;
			}
			
		}
		
		//if there no indices created on the table
		//if user entered clustering key
		else if (columnValues.get(clusteringKeyIndex) != null) {
			
			//read each page and find out if page contains record(s) with clustering key
			Comparable key = (Comparable) columnValues.get(clusteringKeyIndex);
			for (int i=0 ; i<pages.size() ; i++) {
				String pagePath = pages.get(i);
				TablePage currentPage = (TablePage) DBApp.readObject(pagePath);
				Comparable c = ((Comparable) currentPage.getRecords().lastElement().getAttributes().get(clusteringKeyIndex));
				if (c.compareTo(key) > 0) {
					if (currentPage.delete(columnValues, clusteringKeyIndex, columnNames, columnTypes, indices))
						rowsDeleted = true;
					break;
				}
				else if (c.compareTo(key) == 0) {
					if (currentPage.delete(columnValues, clusteringKeyIndex, columnNames, columnTypes, indices))
						rowsDeleted = true;
				}
			}

		}
		
		//if user did NOT enter clustering key
		else {
			
			//must loop linearly on all the entries in all the pages
			for (int i = 0; i < pages.size(); i++) {
				String path = pages.get(i);
				TablePage currentPage = (TablePage) DBApp.readObject(path);
				if (currentPage.delete(columnValues, clusteringKeyIndex, columnNames, columnTypes, indices))
					rowsDeleted = true;
			}
			
		}
		
		return rowsDeleted;
	}
	
	
	public void shiftRecords(Record last, int indexOfPage, int keyIndex, Vector<String> columnNames, Vector<String> columnTypes, Vector<String> indices, String clusteringKey) throws DBAppException {
		
		//if all the pages were full
		if (indexOfPage==pages.size()) {
			//paths
			String currentDirectory = System.getProperty("user.dir");
			String tablePath = currentDirectory + "\\data\\tables\\" + tableName;
			
			//get maximum number of rows per page
			int maxRows = DBApp.getProperty("MaximumRowsCountinPage");
			
			//create a new page and insert the last record in it
			TablePage newPage = new TablePage(maxRows, tableName, tablePath+"\\pages\\"+"page"+numOfPages+".ser");
			pages.add(newPage.getPath());
			numOfPages++;
			newPage.insert(last, keyIndex);
			
			//updating indices if they exist for every time a record is shifted
			updateIndices(last, pages.get(indexOfPage-1), newPage.getPath(), columnNames, columnTypes, indices, clusteringKey);
			return;
		}
		
		//shift the record
		String pagePath = pages.get(indexOfPage);
		TablePage page = (TablePage) DBApp.readObject(pagePath);
		Record tmp = last;
		last = page.insert(last, keyIndex);
		
		
		//updating indices if they exist for every time a record is shifted
		updateIndices(tmp, pages.get(indexOfPage-1), pagePath, columnNames, columnTypes, indices, clusteringKey);
		
		
		//if the page was full continue shifting
		if (last != null)
				shiftRecords(last, ++indexOfPage, keyIndex, columnNames, columnTypes, indices, clusteringKey);
		
	}
	
	
	public void insertIntoIndices(Record newRecord, String pointer, Vector<String> columnNames, Vector<String> columnTypes, Vector<String> indices) throws DBAppException {
		
		//paths
		String currentDirectory = System.getProperty("user.dir");
		String tablePath = currentDirectory + "\\data\\tables\\" + tableName;
		
		for (int i=0 ; i<indices.size() ; i++) {
			String indexColumnName = indices.get(i);
			int columnIndex = columnNames.indexOf(indexColumnName);
			Comparable key = (Comparable) newRecord.getAttributes().get(columnIndex);
			if (!columnTypes.get(columnIndex).equals("java.awt.Polygon")) {
				String treePath = tablePath + "\\indices\\"+ indexColumnName + "\\bptree.ser";
				BPTree tree = (BPTree) DBApp.readObject(treePath);
				tree.insert(key,pointer);
			}
			else {
				String treePath = tablePath + "\\indices\\"+ indexColumnName + "\\rtree.ser";
				RTree tree = (RTree) DBApp.readObject(treePath);
				tree.insert(key,pointer);
			}
		}
	}
	
	
	public void updateIndices(Record shiftedRecord, String oldPointer, String newPointer, Vector<String> columnNames, Vector<String> columnTypes, Vector<String> indices, String clusteringKey) throws DBAppException {
		
		//paths
		String currentDirectory = System.getProperty("user.dir");
		String tablePath = currentDirectory + "\\data\\tables\\" + tableName;
		
		for (int i=0 ; i<indices.size() ; i++) {
			String indexColumnName = indices.get(i);
			int columnIndex = columnNames.indexOf(indexColumnName);
			boolean isClusteringKey = indexColumnName.equals(clusteringKey);
			Comparable key = (Comparable) shiftedRecord.getAttributes().get(columnIndex);
			
			//check the type of the index
			if (!columnTypes.get(columnIndex).equals("java.awt.Polygon")) {
				String treePath = tablePath + "\\indices\\"+ indexColumnName + "\\bptree.ser";
				BPTree tree = (BPTree) DBApp.readObject(treePath);
				tree.insert(key, newPointer);
				tree.delete(key, oldPointer, columnIndex, isClusteringKey);
			}
			else {
				String treePath = tablePath + "\\indices\\"+ indexColumnName + "\\rtree.ser";
				RTree tree = (RTree) DBApp.readObject(treePath);
				tree.insert(key, newPointer);
				tree.delete(key, oldPointer, columnIndex, isClusteringKey);
			}
		}
	}

	
	public void viewTable() throws DBAppException {
		System.out.println(tableName + ":");
		for (int i=0 ; i<pages.size() ; i++) {
			System.out.println("Page " + i + ":");
			TablePage currentPage = (TablePage) DBApp.readObject(pages.get(i));
			currentPage.viewPage();
		}
		System.out.println("End of the table.");
	}
	
	
}
