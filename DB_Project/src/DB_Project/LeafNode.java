package rabena_yostor;

public class LeafNode extends Node {
	
	private static final long serialVersionUID = 1646828268641618009L;
	private String nextLeaf;
	private String prevLeaf;
	
	
	public LeafNode(int maxNumOfKeys, String path, String parent, boolean isRoot, String treePath, String type) {
		super(maxNumOfKeys,path,parent,isRoot,treePath,type);
		if (isRoot)
			this.setMinNumOfKeys(1);
		else {
			this.setMinNumOfKeys((this.getMaxNumOfKeys() + 1) / 2);
		}
	}
	
	
	public void setRoot(boolean isRoot) {
		super.setRoot(isRoot);
		if (isRoot)
			this.setMinNumOfKeys(1);
		else {
			this.setMinNumOfKeys((this.getMaxNumOfKeys() + 1) / 2);
		}
	}
	
	public String getNextLeaf() {
		return nextLeaf;
	}
	
	public void setNextLeaf(String nextLeaf) {
		this.nextLeaf = nextLeaf;
	}
	
	public String getPrevLeaf() {
		return prevLeaf;
	}
	
	public void setPrevLeaf(String prevLeaf) {
		this.prevLeaf = prevLeaf;
	}
	
	
	
	public long insert(Comparable key, String pagePointer, long numOfNodes) throws DBAppException {
		
		//get maximum number of keys per node
		int maxNumOfKeys = DBApp.getProperty("NodeSize");
			
		//if the node contains the key
		int indexOfKey = indexOf(key);
		if (indexOfKey!=-1) {
			//insert the pointer in the leaf page
			LeafPage keyPage = (LeafPage) DBApp.readObject(getPointers().get(indexOfKey));
			keyPage.insert(pagePointer,getTreePath(),key);
		}
		//if the node doesn't contain the key
		else {
			//get the index of the greatest element less than the key
			int indexOfGreatestLesser = indexOfGreatestLesser(key);
			
			//create new leaf page and insert the pointer in it
			String keyString = key.toString(); 
			if (keyString.contains(":")) {
				keyString = keyString.replace(":", "");
			}
			LeafPage newPage = new LeafPage(maxNumOfKeys, getTreePath() + "\\leafPages\\" + keyString + "_0.ser");
			newPage.insert(pagePointer, getTreePath(), key);
			
			//insert the new key and path of the leaf pointer
			getKeys().insertElementAt(key, indexOfGreatestLesser+1);
			getPointers().insertElementAt(newPage.getPath(), indexOfGreatestLesser+1);
			DBApp.writeObject(this, getPath());
		}
		
		//if the maximum number of keys is exceeded
		if (isFull()) {
			
			//check if this is the root
			String nodePath;
			InternalNode newRoot = null;
			if (isRoot()) {
				//create new root
				nodePath = getTreePath() + "\\nodes\\node" + numOfNodes + ".ser";
				numOfNodes++;
				newRoot = new InternalNode(maxNumOfKeys, nodePath, null, true, getTreePath(), getTreeType());
				
				//updating current node and tree
				setRoot(false);
				setParent(newRoot.getPath());
				Tree tree = (Tree) DBApp.readObject(getTreePath() + "\\" + getTreeType() + ".ser");
				tree.setRoot(newRoot.getPath());
				DBApp.writeObject(tree, getTreePath() + "\\" + getTreeType() + ".ser");
			}
			
			
			//create new LeafNode and update pointers
			nodePath = getTreePath() + "\\nodes\\node" + numOfNodes + ".ser";
			numOfNodes++;
			LeafNode newNode = new LeafNode(maxNumOfKeys,nodePath,getParent(),false,getTreePath(),getTreeType());
			if (this.nextLeaf != null) {
				LeafNode nextNode = (LeafNode) DBApp.readObject(nextLeaf);
				nextNode.prevLeaf = newNode.getPath();
				DBApp.writeObject(nextNode,nextNode.getPath());
			}
			newNode.prevLeaf = this.getPath();
			newNode.nextLeaf = this.nextLeaf;
			this.nextLeaf = newNode.getPath();
			
			
			//split the entries
			int middleIndex = getKeys().size()/2;
			Comparable middleKey = getKeys().get(middleIndex);
			for (int i=middleIndex ; i<getKeys().size() ; i++) {
				newNode.getKeys().add( getKeys().remove(i) );
				newNode.getPointers().add( getPointers().remove(i) );
				i--;
			}

			
			//if the node was the root update the new root
			if (newRoot!=null) {
				newRoot.getPointers().add(getPath());
				DBApp.writeObject(newRoot, newRoot.getPath());
			}
			
			
			//save nodes
			DBApp.writeObject(this,this.getPath());
			DBApp.writeObject(newNode,newNode.getPath());
			
			
			//insert into parent
			InternalNode parent = (InternalNode) DBApp.readObject(getParent());
			numOfNodes = parent.insert(middleKey, newNode.getPath(), numOfNodes);
		}
		
		return numOfNodes;
		
	}
	
	
	public void delete(Comparable key, int indexOfKeyInLeaf) throws DBAppException {
		
		//remove the key pointer pair and save the leaf
		getKeys().remove(indexOfKeyInLeaf);
		getPointers().remove(indexOfKeyInLeaf);
		DBApp.writeObject(this, getPath());
		
		
		//if leaf contains less than the minimum number of keys after removing the key
		if (getKeys().size()<getMinNumOfKeys() && !isRoot()) {
			
			String leftSiblingPath = getLeftSiblingPath(key);
			LeafNode leftSibling = (LeafNode) DBApp.readObject(leftSiblingPath);
			String rightSiblingPath = getRightSiblingPath(key);
			LeafNode rightSibling = (LeafNode) DBApp.readObject(rightSiblingPath);
			
			//if leaf can borrow from left sibling
			if (leftSibling != null && canBorrowFromSibling(leftSibling)) {
				borrowFromLeftSibling(leftSibling);
			}
			//if leaf can borrow from right sibling
			else if (rightSibling != null && canBorrowFromSibling(rightSibling)) {
				borrowFromRightSibling(rightSibling);
			}
			//if leaf cannot borrow from either sibling
			else {
				//merge the leaf with one of its siblings and remove the sibling's pointer from the parent
				if (leftSibling != null) {
					mergeWithLeftSibling(leftSibling);
					
					//get the parent and delete the left sibling's pointer and its key
					InternalNode parent = (InternalNode) DBApp.readObject(getParent());
					parent.delete(leftSiblingPath, true);
				}
				else {
					mergeWithRightSibling(rightSibling);

					//get the parent and delete the right sibling's pointer and its key
					InternalNode parent = (InternalNode) DBApp.readObject(getParent());
					parent.delete(rightSiblingPath, false);
				}
			}
		}
		
		
		if (!isRoot()) {
			//check if the key exists in the internal nodes of the tree and modify it
			InternalNode parent = (InternalNode) DBApp.readObject(getParent());
			parent.checkKeyInInternalNodes(key);
		}
		
	}
	
	
	public void borrowFromLeftSibling(LeafNode leftSibling) throws DBAppException {
		
		//remove maximum key from the left sibling and its pointer
		Comparable borrowedKey = leftSibling.getKeys().remove(leftSibling.getKeys().size()-1);
		String borrowedPointer = leftSibling.getPointers().remove(leftSibling.getPointers().size()-1);
		
		//add borrowed key and its pointer to leaf node
		getKeys().add(0, borrowedKey);
		getPointers().add(0, borrowedPointer);
		
		//update parent pointer to be the new minimum value in the current node
		InternalNode parent = (InternalNode) DBApp.readObject(getParent());
		int indexOfKeyInParent = parent.indexOfGreatestLesser(borrowedKey)+1;
		parent.getKeys().set(indexOfKeyInParent, borrowedKey);
		
		//save the nodes
		DBApp.writeObject(this, this.getPath());
		DBApp.writeObject(leftSibling, leftSibling.getPath());
		DBApp.writeObject(parent, parent.getPath());
	}
	
	
	public void borrowFromRightSibling(LeafNode rightSibling) throws DBAppException {
		
		//remove minimum key from the right sibling and its pointer
		Comparable borrowedKey = rightSibling.getKeys().remove(0);
		String borrowedPointer = rightSibling.getPointers().remove(0);
		
		//add borrowed key and its pointer to leaf node
		getKeys().add(borrowedKey);
		getPointers().add(borrowedPointer);
		
		//update parent pointer to be the new minimum value in the right child
		InternalNode parent = (InternalNode) DBApp.readObject(getParent());
		int indexOfKeyInParent = parent.indexOf(borrowedKey);
		parent.getKeys().set(indexOfKeyInParent, rightSibling.getKeys().firstElement());
		
		//save the nodes
		DBApp.writeObject(this, this.getPath());
		DBApp.writeObject(rightSibling, rightSibling.getPath());
		DBApp.writeObject(parent, parent.getPath());
	}
	
	
	public void mergeWithLeftSibling(LeafNode leftSibling) throws DBAppException {
		
		//move all keys and pointers in left sibling to the node
		for(int i=leftSibling.getKeys().size()-1 ; i>=0 ; i--) {
			getKeys().add(0, leftSibling.getKeys().get(i));
			getPointers().add(0, leftSibling.getPointers().get(i));
		}
		
		//update the previous and next leaf pointers
		prevLeaf = leftSibling.prevLeaf;
		LeafNode tmp = (LeafNode) DBApp.readObject(leftSibling.prevLeaf);
		if (tmp != null) {
			tmp.nextLeaf = getPath();
			DBApp.writeObject(tmp, tmp.getPath());
		}
		
		//delete the left sibling node
		leftSibling.deleteNode();
		
		//save the node
		DBApp.writeObject(this, this.getPath());
	}
	
	
	public void mergeWithRightSibling(LeafNode rightSibling) throws DBAppException {
		
		//move all keys and pointers in right sibling to the node
		for(int i=0 ; i<rightSibling.getKeys().size() ; i++) {
			getKeys().add(rightSibling.getKeys().get(i));
			getPointers().add(rightSibling.getPointers().get(i));
		}
		
		//update the previous and next leaf pointers
		nextLeaf = rightSibling.nextLeaf;
		LeafNode tmp = (LeafNode) DBApp.readObject(rightSibling.nextLeaf);
		if (tmp != null) {
			tmp.prevLeaf = getPath();
			DBApp.writeObject(tmp, tmp.getPath());
		}
		
		//delete the right sibling node
		rightSibling.deleteNode();
		
		//save the node
		DBApp.writeObject(this, this.getPath());
	}
	
	
}