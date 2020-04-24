package rabena_yostor;

import java.io.File;
import java.io.Serializable;
import java.util.Vector;

public abstract class Node implements Serializable {
	
	private static final long serialVersionUID = 1646828268641618009L;
	private String path;
	private String parent;
	private int minNumOfKeys;
	private int maxNumOfKeys;
	private boolean isRoot;
	private String treePath;
	private String treeType;
	private Vector<Comparable> keys;
	private Vector<String> pointers;
	
	
	public Node(int maxNumOfKeys, String path, String parent, boolean isRoot, String treePath, String type) {
		this.maxNumOfKeys = maxNumOfKeys;
		this.path = path;
		this.parent = parent;
		this.isRoot = isRoot;
		this.treePath = treePath;
		this.treeType = type;
		this.keys = new Vector<Comparable>();
		this.pointers = new Vector<String>();
	}
	
	
	public String getParent() {
		return parent;
	}
	
	public void setParent(String parent) {
		this.parent = parent;
	}
	
	public Vector<String> getPointers() {
		return pointers;
	}
	
	public void setPointers(Vector<String> pointers) {
		this.pointers = pointers;
	}
	
	public String getPath() {
		return path;
	}
	
	public void setPath(String path) {
		this.path = path;
	}
	
	public int getMinNumOfKeys() {
		return minNumOfKeys;
	}
	
	public void setMinNumOfKeys(int minNumOfKeys) {
		this.minNumOfKeys = minNumOfKeys;
	}
	
	public int getMaxNumOfKeys() {
		return maxNumOfKeys;
	}
	
	public void setMaxNumOfKeys(int maxNumOfKeys) {
		this.maxNumOfKeys = maxNumOfKeys;
	}
	
	public Vector<Comparable> getKeys() {
		return keys;
	}
	
	public void setKeys(Vector<Comparable> keys) {
		this.keys = keys;
	}
	
	public boolean isRoot() {
		return isRoot;
	}
	
	public void setRoot(boolean isRoot) {
		this.isRoot = isRoot;
	}
	
	public String getTreePath() {
		return treePath;
	}
	
	public void setTreePath(String treePath) {
		this.treePath = treePath;
	}
	
	public String getTreeType() {
		return treeType;
	}
	
	public void setTreeType(String type) {
		this.treeType = type;
	}
	
	
	
	public abstract long insert(Comparable key, String pointer, long numOfNodes) throws DBAppException;
	
	
	public void viewNode() {
		for (int i=0 ; i<getKeys().size() ; i++) {
			System.out.print(getKeys().get(i).toString());
			if(i!=getKeys().size()-1)
				System.out.print(",");
		}
	}
	
	
	public boolean isFull() {
		return keys.size()>maxNumOfKeys;
	}
	
	
	public int indexOf(Comparable key) {
		
		//binary search for the index of the key
		//return -1 if the key doesn't exist
		
		int l = 0;
		int r = keys.size() - 1;
		int m = l + (r-l+1)/2;
		int result = -1;

		while (l <= r) {
			m = l + (r-l+1)/2;
			if (key.compareTo(keys.get(m)) == 0) {
				result = m;
				break;
			}
			else if (key.compareTo(keys.get(m)) < 0) {
				r = m-1;
			}
			else if (key.compareTo(keys.get(m)) > 0) {
				l = m+1;
			}
		}

		return result;
	}
	
	
	public int indexOfGreatestLesser(Comparable key) {
		
		//binary search for the index of the last occurrence of greatest entry less than the key
		//return -1 if the key is the less than or equal than all the entries
		
		int l = 0;
		int r = keys.size() - 1;
		int m = l + (r-l+1)/2;
		int result = -1;

		while (l <= r) {
			m = l + (r-l+1)/2;
			if (key.compareTo(keys.get(m))>0) {
				result = m;
				l = m+1;
			}
			else if (key.compareTo(keys.get(m))<0) {
				r = m-1;
			}
			else if (key.compareTo(keys.get(m))==0) {
				r = m-1;
			}
		}

		return result;
	}
	
	
	public void deleteNode() throws DBAppException {
		File nodeFile = new File(path);
		if (!nodeFile.delete())
			throw new DBAppException("An error occured while deleting the node.");
	}
	
	
	public String getLeftSiblingPath(Comparable key) throws DBAppException {
		//returns the path of the left sibling if there is one
		
		InternalNode parentNode = (InternalNode) DBApp.readObject(parent);
		
		int indexOfLeftSibling = parentNode.indexOf(key);
		if (indexOfLeftSibling != -1 )
			return parentNode.getPointers().get(indexOfLeftSibling);
		
		indexOfLeftSibling = parentNode.indexOfGreatestLesser(key);
		if (indexOfLeftSibling != -1 )
			return parentNode.getPointers().get(indexOfLeftSibling);
		
		return null;
	}
	
	
	public String getRightSiblingPath(Comparable key) throws DBAppException {
		//returns the path of the right sibling if there is one
		
		InternalNode parentNode = (InternalNode) DBApp.readObject(parent);
		
		int indexOfRightSibling = parentNode.indexOf(key) + 2;
		if (indexOfRightSibling < parentNode.getPointers().size() )
			return parentNode.getPointers().get(indexOfRightSibling);
		
		indexOfRightSibling = parentNode.indexOfGreatestLesser(key) + 2;
		if (indexOfRightSibling < parentNode.getPointers().size() )
			return parentNode.getPointers().get(indexOfRightSibling);
		
		return null;
	}
	
	
	public boolean canBorrowFromSibling(Node sibling) {
		//returns true if the sibling has enough keys to lend the node
		
		if (sibling.getKeys().size() > sibling.getMinNumOfKeys())
			return true;
		
		return false;
	}
	
	
	public Comparable getMinimumOfSubTree() throws DBAppException {
		
		if (this instanceof LeafNode)
			return keys.firstElement();
		
		Node leftMost = (Node) DBApp.readObject(pointers.firstElement());
		return leftMost.getMinimumOfSubTree();
		
		
	}
	
	
}