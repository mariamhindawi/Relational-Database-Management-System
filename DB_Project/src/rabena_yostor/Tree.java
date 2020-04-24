package rabena_yostor;

import java.io.Serializable;
import java.util.Vector;

public abstract class Tree implements Serializable {
	
	private static final long serialVersionUID = 2588420888488229365L;
	private String root;
	private long numOfNodes;
	private String tableName;
	private String columnName;
	private String path;
	private String type;
	
	
	public Tree(String tableName, String columnName, String path) {
		this.tableName = tableName;
		this.columnName = columnName;
		this.path = path;
		numOfNodes = 0;
		if (this instanceof BPTree)
			type = "bptree";
		else if (this instanceof RTree)
			type = "rtree";
	}
	
	
	public String getRoot() {
		return root;
	}
	
	public void setRoot(String root) {
		this.root = root;
	}
	
	public long getNumOfNodes() {
		return numOfNodes;
	}
	
	public void setNumOfNodes(long numOfNodes) {
		this.numOfNodes = numOfNodes;
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public String getColumnName() {
		return columnName;
	}
	
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	
	public String getPath() {
		return path;
	}
	
	public void setPath(String path) {
		this.path = path;
	}
	
	public String getType() {
		return type;
	}
	
	public void setType(String type) {
		this.type = type;
	}
	
	
	
	public void insert(Comparable key, String pagePointer) throws DBAppException {
		
		//get maximum number of keys per node
		int maxNumOfKeys = DBApp.getProperty("NodeSize");
		
		//if the tree is empty
		if (root == null) {
			String nodePath = path + "\\nodes\\node" + numOfNodes + ".ser";
			numOfNodes++;
			root = nodePath;
			Node root = new LeafNode(maxNumOfKeys,nodePath,null,true,path,type);
			root.insert(key, pagePointer, numOfNodes);
			DBApp.writeObject(this, path + "\\" + type +".ser");
		}
		//if the tree is not empty
		else {
			//search for the leaf and insert in it
			LeafNode leaf = searchForLeaf(key);
			numOfNodes = leaf.insert(key, pagePointer, numOfNodes);
		}
		
		//save the tree object
		Tree tree = (Tree) DBApp.readObject(path + "\\" + type + ".ser");
		tree.setNumOfNodes(this.numOfNodes);
		DBApp.writeObject(tree, tree.getPath() + "\\" + tree.getType() + ".ser");
	}
	
	
	public void delete(Comparable key, String pagePointer, int keyIndex, boolean isClusteringKey) throws DBAppException {
		
		//load the table page from pagePointer to check if it contains the key
		TablePage tablePage = (TablePage) DBApp.readObject(pagePointer);
		
		
		//if the table page contains the key return
		if (tablePage.indexOfFirstOccurrence(key, keyIndex, isClusteringKey) != -1)
			return;
		
		
		//if the page doesn't contain the key delete its pointer from the leaf pages
		
		//go to the leaf node that contains key
		LeafNode leafNode = searchForLeaf(key);
		
		//search for the position of the key in the leaf node and get its leaf page
		int indexOfKeyInLeaf = leafNode.indexOf(key);
		LeafPage leafPage = (LeafPage) DBApp.readObject(leafNode.getPointers().get(indexOfKeyInLeaf));
		
		//delete the pointer from the leaf pages
		leafPage.deletePointer(pagePointer, isClusteringKey);
		
		//reload the leaf page
		leafPage = (LeafPage) DBApp.readObject(leafNode.getPointers().get(indexOfKeyInLeaf));
		
		//check if the last occurrence of the key is removed
		if (leafPage.getPointers().size()==0) {
			// delete the leaf page
			leafPage.deletePage();
			
			//delete the key from the node
			leafNode.delete(key, indexOfKeyInLeaf);
			
			//check if the root became empty delete it, update the root path, and reset the numOfNodes counter
			Node rootNode = (Node) DBApp.readObject(root);
			if (rootNode.getKeys().size() == 0) {
				if (rootNode instanceof LeafNode) {
					root = null;
					numOfNodes = 0;
				}
				else {
					root = rootNode.getPointers().firstElement();
					Node newRoot = (Node) DBApp.readObject(root);
					newRoot.setRoot(true);
					newRoot.setParent(null);
					DBApp.writeObject(newRoot, newRoot.getPath());
				}
				rootNode.deleteNode();
			}
					
			DBApp.writeObject(this, path + "\\" + type +".ser");	
		}
		
	}
	
	
	public LeafNode searchForLeaf(Comparable key) throws DBAppException {
		return searchForLeafHelper(key,root);
	}
	
	
	public LeafNode searchForLeafHelper(Comparable key, String path) throws DBAppException {
		Node node = (Node) DBApp.readObject(path);
		
		//if this is a LeafNode return it
		if (node instanceof LeafNode) {
			return ((LeafNode) node);
		}
		
		//if this is an InternalNode check which pointer to go to
		int indexOfPointer = node.indexOf(key);
		//if the node doesn't contain the key
		if (indexOfPointer==-1)
			indexOfPointer = node.indexOfGreatestLesser(key) + 1;
		//if the node contains the key
		else
			indexOfPointer++;
			
		
		//go to the next level 
		return searchForLeafHelper(key, node.getPointers().get(indexOfPointer));
	}
	
	
	public void view() throws DBAppException {
		if (root==null)
			throw new DBAppException("Tree is empty.");
		
		Node rootNode  = (Node) DBApp.readObject(root);
		Vector<String> currentLevel = new Vector<String>();
		currentLevel.add(rootNode.getPath());
		do {
			currentLevel = viewLevel(currentLevel);
			System.out.println();
		} while(currentLevel.size()!=0);
		
	}
	
	
	public static Vector<String> viewLevel(Vector<String> currentLevel) throws DBAppException {
		Vector<String> nextLevel = new Vector<String>();
		for (int i=0 ; i<currentLevel.size() ; i++) {
			if (currentLevel.get(i).equals(""))
				System.out.print("&& | ");
			else {
				Node currentNode = (Node) DBApp.readObject(currentLevel.get(i));
				currentNode.viewNode();
				if(i!=currentLevel.size()-1)
					System.out.print(" | ");
				if(!(currentNode instanceof LeafNode)) {
					for (int j=0 ; j<currentNode.getPointers().size() ; j++) {
						nextLevel.add(currentNode.getPointers().get(j));
					}
					if (i!=currentLevel.size()-1)
						nextLevel.add("");
				}
			}
			
		}
		
		return nextLevel;
	}
	
	
	public void viewPointers() throws DBAppException {
		if (root==null)
			throw new DBAppException("Tree is empty.");
		
		Node currentNode  = (Node) DBApp.readObject(root);
		while (currentNode instanceof InternalNode) {
			currentNode =  (Node) DBApp.readObject( currentNode.getPointers().firstElement() );
		}
		
		LeafNode currentLeaf = (LeafNode) currentNode;
		int leafNum = 0;
		do {
			System.out.println("Leaf " + leafNum++ + ":");
			for (int i=0 ; i<currentLeaf.getKeys().size() ; i++) {
				System.out.println("Key " + currentLeaf.getKeys().get(i) + ":");
				LeafPage currentKeyPage = (LeafPage) DBApp.readObject(currentLeaf.getPointers().get(i));
				currentKeyPage.view();
			}
			System.out.println();
			if (currentLeaf.getNextLeaf()!=null)
				currentLeaf = (LeafNode) DBApp.readObject(currentLeaf.getNextLeaf());
			else {
				currentLeaf = null;
			}
		} while (currentLeaf!= null);
	}
	
	
}