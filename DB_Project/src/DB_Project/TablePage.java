package rabena_yostor;

import java.awt.Polygon;
import java.io.File;
import java.util.Date;
import java.util.Vector;

public class TablePage extends Page {
	
	private static final long serialVersionUID = 1675077537702115401L;
	private Vector<Record> records;
	private String tableName;
	
	
	public TablePage(int maxNumOfEntries, String tableName, String path) {
		super(maxNumOfEntries, path);
		this.tableName = tableName;
		records = new Vector<Record>();
	}
	
	
	public Vector<Record> getRecords() {
		return records;
	}
	
	public void setRecords(Vector<Record> records) {
		this.records = records;
	}
	
	
	
	public Record insert(Record r, int keyIndex) throws DBAppException {
		
		Record last = null;
		Comparable key = (Comparable) r.getAttributes().get(keyIndex);
		
		//find the position to insert the record at
		int position = indexOfGreatestLesser(key, keyIndex) + 1;
		
		//insert the record
		records.insertElementAt(r, position);
		
		//check if the maximum number of entries is exceeded remove the last element and return it
		if (records.size() > getMaxNumOfEntries())
			last = records.remove(records.size()-1);
		
		// save page in hard disk
		DBApp.writeObject(this, this.getPath());
		
		return last;
	}
	
	
	public boolean update(Comparable key, Vector<Object> columnValues, int clusteringKeyIndex,  Vector<String> columnNames, Vector<String> columnTypes, Vector<String> indices) throws DBAppException {
		
		//search the page for the key
		int indexOfRecord = indexOfFirstOccurrence(key, clusteringKeyIndex, true);
		
		
		//if no matching record was found return false
		if (indexOfRecord==-1)
			return false;
		boolean result = false;
		
		//update matching record(s)
		for (int i=indexOfRecord ; i<records.size() ; i++) {
			
			Comparable c = (Comparable) records.get(i).getAttributes().get(clusteringKeyIndex);
			
			//if the record have the same value of the key
			if (c.equals(key)) {
				
				//return true as there were row(s) that were updated
				result = true;
				
				//clone the old attributes of the record 
				Record oldRecord = new Record();
				Vector<Object> oldAttributes = (Vector<Object>) records.get(i).getAttributes().clone();
				oldRecord.setAttributes(oldAttributes);
				
				//update the attributes of the record 
				Vector<Object> attributes = records.get(i).getAttributes();
				for (int j = 0; j < columnValues.size(); j++) {
					if (columnValues.get(j) != null) {
						attributes.remove(j);
						attributes.add(j, columnValues.get(j));
					}
				}
				attributes.remove(attributes.size()-1);
				attributes.add(new Date());
				
				// save page in hard disk
				DBApp.writeObject(this, this.getPath());
				
				//update the indices if they exist for the record
				updateIndices(oldRecord, records.get(i), clusteringKeyIndex, columnNames, columnTypes, indices);
				
			}
			else if (c.compareTo(key)>0)
				break;
		}
		
		return result;
	}
	

	public boolean delete(Vector<Object> columnValues, int clusteringKeyIndex,  Vector<String> columnNames, Vector<String> columnTypes, Vector<String> indices) throws DBAppException {
		
		//paths
		String currentDirectory = System.getProperty("user.dir");
		String tablePath = currentDirectory + "\\data\\tables\\" + tableName + "\\" + tableName + ".ser";
		boolean result = false;
		
		
		//if user entered clustering key
		if (columnValues.get(clusteringKeyIndex) != null) {
			
			//search the page for the key
			Comparable key = (Comparable) columnValues.get(clusteringKeyIndex);
			int indexOfRecord = indexOfFirstOccurrence(key, clusteringKeyIndex, true);
			
			
			//if no matching record was found return false
			if (indexOfRecord==-1)
				return false;
			
			
			//delete matching record(s)
			for (int i=indexOfRecord ; i<records.size() ; i++) {
				
				Comparable c = (Comparable) records.get(i).getAttributes().get(clusteringKeyIndex);
				
				//if the record have the same value of the clustering key
				if (c.equals(key)) {
					
					//check the values of the other columns
					boolean deleteCurrentRecord = records.get(i).checkDeleteRecord(columnValues);
					
					//if the values match delete the current record
					if (deleteCurrentRecord) {
						//delete the record
						Record deletedRecord = records.remove(i);
						i--;
						
						// save page in hard disk
						DBApp.writeObject(this, this.getPath());
						
						//update the indices if they exist for the record
						removeFromIndices(deletedRecord, clusteringKeyIndex, columnNames, columnTypes, indices);
						
						//return true as there were row(s) that were updated
						result = true;
					}
				}
				else if (c.compareTo(key)>0)
					break;
			}
		}
		
		//if user did NOT enter clustering key
		else {
			
			for (int i=0 ; i<records.size() ; i++) {
				
				//check the values of the records
				boolean deleteCurrentRecord = records.get(i).checkDeleteRecord(columnValues);
				
				//if the values match delete the current record
				if (deleteCurrentRecord) {
					//delete the record
					Record deletedRecord = records.remove(i);
					i--;
					
					// save page in hard disk
					DBApp.writeObject(this, this.getPath());
					
					//update the indices if they exist for the record
					removeFromIndices(deletedRecord, clusteringKeyIndex, columnNames, columnTypes, indices);
					
					//return true as there were row(s) that were updated
					result = true;
				}
			}
			
		}
		
		
		//if the page became empty
		if (records.size() == 0) {
			// delete page
			deletePage();
			Table t = (Table) DBApp.readObject(tablePath); 
			t.getPages().remove(getPath());
			DBApp.writeObject(t, tablePath);
		}
		
		
		return result;
	}
	
	
	public void updateIndices(Record oldRecord, Record newRecord, int clusteringKeyIndex, Vector<String> columnNames, Vector<String> columnTypes, Vector<String> indices) throws DBAppException {
		
		//paths
		String currentDirectory = System.getProperty("user.dir");
		String tablePath = currentDirectory + "\\data\\tables\\" + tableName;
		
		//get the name of the clustering key
		String clusteringKey = columnNames.get(clusteringKeyIndex); 
		
		for (int i=0 ; i<indices.size() ; i++) {
			String indexColumnName = indices.get(i);
			int columnIndex = columnNames.indexOf(indexColumnName);
			boolean isClusteringKey = indexColumnName.equals(clusteringKey);
			Comparable oldKey = (Comparable) oldRecord.getAttributes().get(columnIndex);
			Comparable newKey = (Comparable) newRecord.getAttributes().get(columnIndex);
			
			//if this attribute's value didn't change skip current iteration
			if (oldKey.equals(newKey)) {
				continue;
			}
			
			//if this attribute's value changed update its index
			//check the type of the index
			if (!columnTypes.get(columnIndex).equals("java.awt.Polygon")) {
				String treePath = tablePath + "\\indices\\"+ indexColumnName + "\\bptree.ser";
				BPTree tree = (BPTree) DBApp.readObject(treePath);
				tree.delete(oldKey, this.getPath(), columnIndex, isClusteringKey);
				tree.insert(newKey, this.getPath());
			}
			else {
				String treePath = tablePath + "\\indices\\"+ indexColumnName + "\\rtree.ser";
				RTree tree = (RTree) DBApp.readObject(treePath);
				tree.delete(oldKey, this.getPath(), columnIndex, isClusteringKey);
				tree.insert(newKey, this.getPath());
			}
		}
	}
	
	
	public void removeFromIndices(Record deletedRecord, int clusteringKeyIndex, Vector<String> columnNames, Vector<String> columnTypes, Vector<String> indices) throws DBAppException {
		
		//paths
		String currentDirectory = System.getProperty("user.dir");
		String tablePath = currentDirectory + "\\data\\tables\\" + tableName;
		
		//get the name of the clustering key
		String clusteringKey = columnNames.get(clusteringKeyIndex); 
		
		for (int i=0 ; i<indices.size() ; i++) {
			String indexColumnName = indices.get(i);
			int columnIndex = columnNames.indexOf(indexColumnName);
			boolean isClusteringKey = indexColumnName.equals(clusteringKey);
			Comparable key = (Comparable) deletedRecord.getAttributes().get(columnIndex);
			
			//check the type of the index
			if (!columnTypes.get(columnIndex).equals("java.awt.Polygon")) {
				String treePath = tablePath + "\\indices\\"+ indexColumnName + "\\bptree.ser";
				BPTree tree = (BPTree) DBApp.readObject(treePath);
				tree.delete(key, this.getPath(), columnIndex, isClusteringKey);
			}
			else {
				String treePath = tablePath + "\\indices\\"+ indexColumnName + "\\rtree.ser";
				RTree tree = (RTree) DBApp.readObject(treePath);
				tree.delete(key, this.getPath(), columnIndex, isClusteringKey);
			}
		}
	}
	
	
	public int indexOfFirstOccurrence(Comparable key, int keyIndex, boolean binary) {
		
		//search binary or linearly for the index of the first occurrence of the key
		//return -1 if the key doesn't exist
		
		//binary search for the index of the first occurrence of the key
		if (binary) {
			
			int l = 0;
			int r = records.size() - 1;
			int m = l + (r-l+1)/2;
			int result = -1;
			
			while (l <= r) {
				m = l + (r-l+1)/2;
				if (key.compareTo(records.get(m).getAttributes().get(keyIndex)) == 0) {
					result = m;
					r = m-1;
				}
				else if (key.compareTo(records.get(m).getAttributes().get(keyIndex)) < 0) {
					r = m-1;
				}
				else if (key.compareTo(records.get(m).getAttributes().get(keyIndex)) > 0) {
					l = m+1;
				}
			}
			
			return result;
		}
		//linear search for the index of the first occurrence of the key
		else {
			for (int i=0 ; i<records.size() ; i++)
				if (key.compareTo(records.get(i).getAttributes().get(keyIndex))==0)
					return i;
			return -1;
		}
		
	}
	
	
	public int indexOfGreatestLesser(Comparable key, int keyIndex) {
		
		//binary search for the index of the last occurrence of greatest entry less than the key
		//return -1 if the key is the less than or equal than all the entries
		
		int l = 0;
		int r = records.size() - 1;
		int m = l + (r-l+1)/2;
		int result = -1;

		while (l <= r) {
			m = l + (r-l+1)/2;
			if (key.compareTo(records.get(m).getAttributes().get(keyIndex))>0) {
				result = m;
				l = m+1;
			}
			else if (key.compareTo(records.get(m).getAttributes().get(keyIndex))<0) {
				r = m-1;
			}
			else if (key.compareTo(records.get(m).getAttributes().get(keyIndex))==0) {
				r = m-1;
			}
		}

		return result;
	}
	
	
	public void viewPage() {
		for (int i = 0; i < records.size(); i++)
			System.out.println(records.get(i).getAttributes());
	}
	
	
	public void deletePage() throws DBAppException {
		File pageFile = new File(getPath());
		if (!pageFile.delete())
			throw new DBAppException("An error occured while deleting the table page.");
	}
	
	
	public Vector<Record> linearSelect(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException{
		
		Vector<String> columnNames = null;
		String clusteringKey="";
		try {
			columnNames = (Vector<String>) DBApp.getTableInfo(tableName).get(0);
			clusteringKey = (String) DBApp.getTableInfo(tableName).get(2);
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Vector<Record> result = new Vector<Record>();
		int start = 0,end=0;
		if (!DBApp.clusteredQuery(arrSQLTerms, strarrOperators, clusteringKey)) {
			end=records.size()-1;
			System.out.println("Linearly scanning "+this.getPath()+"...");
		}
		else {
			System.out.println("Binary-searching " + this.getPath()+"...");
			start = allQueriesStartIndex(arrSQLTerms, strarrOperators);
			end = allQueriesEndIndex(arrSQLTerms, strarrOperators);
		}
		for (int j=start; j<=end; j++) {
			Record r = getRecords().get(j);
			boolean match = false;
			for (int k = 0; k < arrSQLTerms.length; k++) {
				String columnName = arrSQLTerms[k]._strColumnName;
				int columnIndex = columnNames.indexOf(columnName);
				String operator = arrSQLTerms[k]._strOperator;
				Object value = arrSQLTerms[k]._objValue;
				if (value instanceof Polygon) {
					value = new MyPolygon((Polygon) value);
				}
				if (operator.equals("=")) {
					if (r.getAttributes().get(columnIndex).equals(value)) {
						if (k == 0 || strarrOperators[k - 1].equals("OR")) {
							match = true;
						}
						if (k != 0 && strarrOperators[k - 1].equals("XOR")) {
							match = !match;
						}
					} else {
						if (k != 0 && strarrOperators[k - 1].equals("AND")) {
							match = false;
						}
					}
				}
				if (operator.equals("!=")) {
					if (!r.getAttributes().get(columnIndex).equals(value)) {
						if (k == 0 || strarrOperators[k - 1].equals("OR")) {
							match = true;
						}
						if (k != 0 && strarrOperators[k - 1].equals("XOR")) {
							match = !match;
						}
					} else {
						if (k != 0 && strarrOperators[k - 1].equals("AND")) {
							match = false;
						}
					}
				}
				if (operator.equals("<")) {
					if (((Comparable) r.getAttributes().get(columnIndex)).compareTo(value) < 0) {
						if (k == 0 || strarrOperators[k - 1].equals("OR")) {
							match = true;
						}
						if (k != 0 && strarrOperators[k - 1].equals("XOR")) {
							match = !match;
						}
					} else {
						if (k != 0 && strarrOperators[k - 1].equals("AND")) {
							match = false;
						}
					}
				}
				if (operator.equals(">")) {
					if (((Comparable) r.getAttributes().get(columnIndex)).compareTo(value) > 0) {
						if (k == 0 || strarrOperators[k - 1].equals("OR")) {
							match = true;
						}
						if (k != 0 && strarrOperators[k - 1].equals("XOR")) {
							match = !match;
						}
					} else {
						if (k != 0 && strarrOperators[k - 1].equals("AND")) {
							match = false;
						}
					}
				}
				if (operator.equals(">=")) {
					if (((Comparable) r.getAttributes().get(columnIndex)).compareTo(value) >= 0) {
						if (k == 0 || strarrOperators[k - 1].equals("OR")) {
							match = true;
						}
						if (k != 0 && strarrOperators[k - 1].equals("XOR")) {
							match = !match;
						}
					} else {
						if (k != 0 && strarrOperators[k - 1].equals("AND")) {
							match = false;
						}
					}
				}
				if (operator.equals("<=")) {
					if (((Comparable) r.getAttributes().get(columnIndex)).compareTo(value) <= 0) {
						if (k == 0 || strarrOperators[k - 1].equals("OR")) {
							match = true;
						}
						if (k != 0 && strarrOperators[k - 1].equals("XOR")) {
							match = !match;
						}
					} else {
						if (k != 0 && strarrOperators[k - 1].equals("AND")) {
							match = false;
						}
					}
				}
			}
			if (match)
				result.add(r);
		} 
		return result;
		
	}
	
	
	@SuppressWarnings("unchecked")
	public int queryStartIndex(SQLTerm query) throws DBAppException {
		
		String clusteringKey = (String) DBApp.getTableInfo(tableName).get(2);
		Vector<String> columnNames= (Vector<String>) DBApp.getTableInfo(tableName).get(0);
		String columnName=query._strColumnName;
		int index=columnNames.indexOf(columnName);
		String operator=query._strOperator;
		Comparable value;
		if(query._objValue instanceof Polygon)
			value = new MyPolygon((Polygon) query._objValue);
		else
			 value = (Comparable) query._objValue;
		if(clusteringKey.equals(columnName)) {
			if(operator.equals("=")) {
				return indexOfFirstOccurrence(value, index, true);
			}
			if(operator.equals("!="))
				return 0;
			if(operator.equals("<")) {
				if(((Comparable) records.get(0).getAttributes().get(index)).compareTo(value)>=0)
					return -1;
				else 
					return 0;
			}
			if(operator.equals(">")) {
				if(((Comparable) records.lastElement().getAttributes().get(index)).compareTo(value)<=0)
					return -1;
				else
					return 0;
			}
			if(operator.equals("<=")) {
				if(((Comparable) records.get(0).getAttributes().get(index)).compareTo(value)>0)
					return -1;
				else 
					return 0;
			}
			if(operator.equals(">=")) {
				if(((Comparable) records.lastElement().getAttributes().get(index)).compareTo(value)<0)
					return -1;
				else
					return 0;
			}
		}
		return 0;
	}
	
	
	public int queryEndIndex(SQLTerm query) throws DBAppException {
		
		String clusteringKey = (String) DBApp.getTableInfo(tableName).get(2);
		Vector<String> columnNames= (Vector<String>) DBApp.getTableInfo(tableName).get(0);
		String columnName=query._strColumnName;
		int index=columnNames.indexOf(columnName);
		String operator=query._strOperator;
		Comparable value;
		if(query._objValue instanceof Polygon)
			value = new MyPolygon((Polygon) query._objValue);
		else
			value = (Comparable) query._objValue;
		if(clusteringKey.equals(columnName)) {
			if(operator.equals("=")) {
				if(indexOfFirstOccurrence(value, index, true)==-1)
					return -1;
				else
					return records.size()-1;
						
			}
			if(operator.equals("!="))
				return records.size()-1;
			if(operator.equals("<")) {
				if(((Comparable) records.get(0).getAttributes().get(index)).compareTo(value)>=0)
					return -1;
				else 
					return indexOfGreatestLesser(value, index);
			}
			if(operator.equals(">")) {
				if(((Comparable) records.lastElement().getAttributes().get(index)).compareTo(value)<=0)
					return -1;
				else
					return records.size()-1;
			}
			if(operator.equals("<=")) {
				if(((Comparable) records.get(0).getAttributes().get(index)).compareTo(value)>0)
					return -1;
				else 
					return records.size()-1;
			}
			if(operator.equals(">=")) {
				if(((Comparable) records.lastElement().getAttributes().get(index)).compareTo(value)<0)
					return -1;
				else
					return records.size()-1;
			}
		}
		return records.size()-1;
	}
	
	
	public int allQueriesStartIndex(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		
		SQLTerm last = arrSQLTerms[arrSQLTerms.length-1];
		String lastColumn = last._strColumnName;
		if(strarrOperators.length==0) {
				return queryStartIndex(last);
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
			return Integer.max(allQueriesStartIndex(arrSQLTermsnew, strarrOperatorsnew), queryStartIndex(last));
		else
			return Integer.min(allQueriesStartIndex(arrSQLTermsnew, strarrOperatorsnew), queryStartIndex(last));
	}
	
	
	public int allQueriesEndIndex(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		
		SQLTerm last = arrSQLTerms[arrSQLTerms.length-1];
		String lastColumn = last._strColumnName;
		if(strarrOperators.length==0) {
				return queryEndIndex(last);
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
			return Integer.min(allQueriesEndIndex(arrSQLTermsnew, strarrOperatorsnew), queryEndIndex(last));
		else
			return Integer.max(allQueriesEndIndex(arrSQLTermsnew, strarrOperatorsnew), queryEndIndex(last));
	}
	
	
}
