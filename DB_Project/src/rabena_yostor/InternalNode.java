package rabena_yostor;

public class InternalNode extends Node {
	
	private static final long serialVersionUID = 1646828268641618009L;
	
	
	public InternalNode(int maxNumOfKeys, String path, String parent, boolean isRoot, String treePath, String type) {
		super(maxNumOfKeys,path,parent,isRoot,treePath,type);
		if (isRoot)
			this.setMinNumOfKeys(1);
		else
			this.setMinNumOfKeys((int) Math.ceil((this.getMaxNumOfKeys() + 1) / 2.0) - 1);
	}
	
	
	public void setRoot(boolean isRoot) {
		super.setRoot(isRoot);
		if (isRoot)
			this.setMinNumOfKeys(1);
		else {
			this.setMinNumOfKeys((int) Math.ceil((this.getMaxNumOfKeys() + 1) / 2.0) - 1);
		}
	}
	
	
	
	public long insert(Comparable key, String nodePointer, long numOfNodes) throws DBAppException {
		
		//get maximum number of keys per node
		int maxNumOfKeys = DBApp.getProperty("NodeSize");
		
		
		//insert the key and pointer
		int indexOfGreatestLesser = indexOfGreatestLesser(key);
		getKeys().insertElementAt(key, indexOfGreatestLesser+1);
		getPointers().insertElementAt(nodePointer, indexOfGreatestLesser+2);
		DBApp.writeObject(this, getPath());
		
		
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
			
			
			//create new InternalNode
			nodePath = getTreePath() + "\\nodes\\node" + numOfNodes + ".ser";
			numOfNodes++;
			InternalNode newNode = new InternalNode(maxNumOfKeys, nodePath, getParent(), false, getTreePath(), getTreeType());
			
			
			//split the entries
			int middleIndex = getKeys().size()/2;
			Comparable middleKey = getKeys().remove(middleIndex);
			for (int i=middleIndex ; i<getKeys().size() ; i++) {
				newNode.getKeys().add( getKeys().remove(i) );
				newNode.getPointers().add( getPointers().remove(i+1) );
				i--;
			}
			newNode.getPointers().add( getPointers().remove(getPointers().size()-1) );
			
			
			//update the parents pointers of the nodes
			for (int i=0 ; i<newNode.getPointers().size() ; i++) {
				Node currentNode = (Node) DBApp.readObject(newNode.getPointers().get(i));
				currentNode.setParent(newNode.getPath());
				DBApp.writeObject(currentNode, currentNode.getPath());
			}
			
			
			//if the node was the root update the new root's first pointer
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

	
	public void delete(String pointer, boolean isLeft) throws DBAppException {
		
		//remove the key pointer pair and save the node
		int indexOfPointer = getPointers().indexOf(pointer);
		Comparable key;
		if (isLeft)
			key = getKeys().remove(indexOfPointer);
		else
			key = getKeys().remove(indexOfPointer-1);
		getPointers().remove(indexOfPointer);
		DBApp.writeObject(this, getPath());
		
		
		//if node contains less than the minimum number of keys after removing the key
		if (getKeys().size()<getMinNumOfKeys() && !isRoot()) {
			
			String leftSiblingPath = getLeftSiblingPath(key);
			InternalNode leftSibling = (InternalNode) DBApp.readObject(leftSiblingPath);
			String rightSiblingPath = getRightSiblingPath(key);
			InternalNode rightSibling = (InternalNode) DBApp.readObject(rightSiblingPath);
			
			//if node can borrow from left sibling
			if (leftSibling != null && canBorrowFromSibling(leftSibling)) {
				borrowFromLeftSibling(leftSibling);
			}
			//if node can borrow from right sibling
			else if (rightSibling != null && canBorrowFromSibling(rightSibling)) {
				borrowFromRightSibling(rightSibling);
			}
			//if cannot borrow from either sibling
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
		
	}
	
	
	public void borrowFromLeftSibling(InternalNode leftSibling) throws DBAppException {
		
		//remove maximum key from the left sibling and its pointer
		Comparable borrowedKey = leftSibling.getKeys().remove(leftSibling.getKeys().size()-1);
		String borrowedPointer = leftSibling.getPointers().remove(leftSibling.getPointers().size()-1);
		
		//add borrowed key and its pointer to the node
		getKeys().add(0, borrowedKey);
		getPointers().add(0, borrowedPointer);
		
		//replace the borrowed key with the minimum value of its right subtree
		Node rightChild = (Node) DBApp.readObject(getPointers().get(1));
		getKeys().set(0, rightChild.getMinimumOfSubTree());
		
		//update parent pointer to be the new minimum value of its right subtree
		InternalNode parent = (InternalNode) DBApp.readObject(getParent());
		int indexOfKeyInParent = parent.indexOfGreatestLesser(borrowedKey)+1;
		parent.getKeys().set(indexOfKeyInParent, this.getMinimumOfSubTree());
		
		//save the nodes
		DBApp.writeObject(this, this.getPath());
		DBApp.writeObject(leftSibling, leftSibling.getPath());
		DBApp.writeObject(parent, parent.getPath());
	}
	
	
	public void borrowFromRightSibling(InternalNode rightSibling) throws DBAppException {
		
		//remove minimum key from the right sibling and its pointer
		Comparable borrowedKey = rightSibling.getKeys().remove(0);
		String borrowedPointer = rightSibling.getPointers().remove(0);
		
		//add borrowed key and its pointer to leaf node
		getKeys().add(borrowedKey);
		getPointers().add(borrowedPointer);
		
		//replace the borrowed key with the minimum value of its right subtree
		Node rightChild = (Node) DBApp.readObject(getPointers().lastElement());
		getKeys().set(getKeys().size()-1, rightChild.getMinimumOfSubTree());
		
		//update parent pointer to be the new minimum value of its right subtree
		InternalNode parent = (InternalNode) DBApp.readObject(getParent());
		int indexOfKeyInParent = parent.indexOfGreatestLesser(borrowedKey);
		parent.getKeys().set(indexOfKeyInParent, rightSibling.getMinimumOfSubTree());
		
		//save the nodes
		DBApp.writeObject(this, this.getPath());
		DBApp.writeObject(rightSibling, rightSibling.getPath());
		DBApp.writeObject(parent, parent.getPath());
	}
	
	
	public void mergeWithLeftSibling(InternalNode leftSibling) throws DBAppException {
		
		//get the minimum of the first pointer's subtree and add it as the first key
		Node firstChild = (Node) DBApp.readObject(getPointers().firstElement());
		getKeys().add(0, firstChild.getMinimumOfSubTree());
		
		//move all keys and pointers in left sibling to the node
		
		//move and update the parent of the last node of left the sibling
		getPointers().add(0, leftSibling.getPointers().lastElement());
		Node current = (Node) DBApp.readObject(leftSibling.getPointers().lastElement());
		current.setParent(getPath());
		DBApp.writeObject(current, current.getPath());
		
		//move and update the parents of the rest of nodes
		for(int i=leftSibling.getKeys().size()-1 ; i>=0 ; i--) {
			getKeys().add(0, leftSibling.getKeys().get(i));
			getPointers().add(0, leftSibling.getPointers().get(i));
			
			//update the parents of the moved nodes
			current = (Node) DBApp.readObject(leftSibling.getPointers().get(i));
			current.setParent(getPath());
			DBApp.writeObject(current, current.getPath());
		}
		
		//delete the left sibling node
		leftSibling.deleteNode();
		
		//save the node
		DBApp.writeObject(this, this.getPath());
	}
	
	
	public void mergeWithRightSibling(InternalNode rightSibling) throws DBAppException {

		//get the minimum of the first pointer's subtree and add it as the first key
		Node firstChildOfRightSibling = (Node) DBApp.readObject(rightSibling.getPointers().firstElement());
		getKeys().add(firstChildOfRightSibling.getMinimumOfSubTree());
		
		//move all keys and pointers in right sibling to the node
		
		//move and update the parent of the first node of the right sibling
		Node current = (Node) DBApp.readObject(rightSibling.getPointers().firstElement());
		current.setParent(getPath());
		DBApp.writeObject(current, current.getPath());
		getPointers().add(rightSibling.getPointers().remove(0));
		
		//move and update the parents of the rest of nodes
		for(int i=0 ; i<rightSibling.getKeys().size() ; i++) {
			getKeys().add(rightSibling.getKeys().get(i));
			getPointers().add(rightSibling.getPointers().get(i));
			
			//update the parents of the moved nodes 
			current = (Node) DBApp.readObject(rightSibling.getPointers().get(i));
			current.setParent(getPath());
			DBApp.writeObject(current, current.getPath());
		}
		
		//delete the right sibling node
		rightSibling.deleteNode();
		
		//save the node
		DBApp.writeObject(this, this.getPath());
	}
	
	
	public void checkKeyInInternalNodes(Comparable key) throws DBAppException {
		
		//check if the key exists in this node
		int indexOfKey = indexOf(key);
		if (indexOfKey != -1) {
			//replace it with the minimum value of the right subtree
			Node rightChild = (Node) DBApp.readObject(getPointers().get(indexOfKey+1));
			getKeys().set(indexOfKey, rightChild.getMinimumOfSubTree());
			
			//save the node
			DBApp.writeObject(this, getPath());
			return;
		}
		
		//check in the parent
		if (getParent() != null) {
			InternalNode parent = (InternalNode)DBApp.readObject(getParent());
			parent.checkKeyInInternalNodes(key);
		}
		
	}
	
	
}