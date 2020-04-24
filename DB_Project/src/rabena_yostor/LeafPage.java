package rabena_yostor;

import java.io.File;
import java.util.Vector;

public class LeafPage extends Page {
	
	
	private static final long serialVersionUID = 1675077537702115401L;
	private Vector<String> pointers;
	private String nextPage;
	
	
	public LeafPage(int maxNumOfEntries, String path) {
		super(maxNumOfEntries,path);
		pointers = new Vector<String>();
		nextPage = null;
	}
	
	
	public Vector<String> getPointers() {
		return pointers;
	}
	
	public void setPointers(Vector<String> pointers) {
		this.pointers = pointers;
	}
	
	public String getNextPage() {
		return nextPage;
	}
	
	public void setNextPage(String nextPage) {
		this.nextPage = nextPage;
	}
	
	
	
	public void insert(String pagePath, String treePath, Comparable key) throws DBAppException {
		insertHelper(pagePath,treePath,key,0);
	}
	
	
	public void insertHelper(String pagePath, String treePath, Comparable key, int i) throws DBAppException {
		if (pointers.contains(pagePath)) {
			return;
		}
		if (pointers.size()<getMaxNumOfEntries()) {
			pointers.add(pagePath);
			DBApp.writeObject(this, getPath());
			return;
		}
		//if page is full
		if (nextPage != null) {
			LeafPage next = (LeafPage) DBApp.readObject(nextPage);
			next.insertHelper(pagePath,treePath,key,++i);
			return;
		}
		//if all pages are full
		String keyString = key.toString(); 
		if (keyString.contains(":")) {
			keyString = keyString.replace(":", "");
		}
		LeafPage newPage = new LeafPage(getMaxNumOfEntries(), treePath + "\\leafPages\\" + keyString + "_" + ++i + ".ser");
		newPage.pointers.add(pagePath);
		nextPage = newPage.getPath();
		DBApp.writeObject(this, getPath());
		DBApp.writeObject(newPage, newPage.getPath());
	}
	
	
	public void view() throws DBAppException {
		int pageNum = 0;
		LeafPage currentPage = this;
		while (currentPage!=null) {
			System.out.println("Page " + pageNum++ + ":");
			for (int i=0 ; i<currentPage.pointers.size() ; i++)
				System.out.println(currentPage.pointers.get(i));
			if (currentPage.nextPage!=null)
				currentPage = (LeafPage) DBApp.readObject(currentPage.nextPage);
			else
				currentPage = null;
				
		}
		
	}
	
	
	public void deletePointer(String pagePointer, boolean isClusteringKey) throws DBAppException {
		
		//search for the pointer in the current leaf page 
		int indexOfPointerInLeafPage = indexOf(pagePointer, isClusteringKey);
		
		
		//if the current page contains the pointer
		if (indexOfPointerInLeafPage != -1) {
			pointers.remove(indexOfPointerInLeafPage);
			shiftBack();
			return;
		}
		
		
		//if the current page doesn't contain the pointer
		LeafPage next = (LeafPage) DBApp.readObject(this.nextPage);
		
		//check if the next page is the last page and contains only 1 pointer
		if (next.pointers.size()==1 && next.nextPage==null) {
			//if the last pointer is the pagePointer
			if (next.pointers.get(0).equals(pagePointer)) {
				//delete the page
				next.deletePage();
				//update the current page's next pointer and save the current page
				this.nextPage = null;
				DBApp.writeObject(this, getPath());
			}
			return;
		}
		
		//else go to the next page
		next.deletePointer(pagePointer, isClusteringKey);
		
	}
	
	
	public void shiftBack() throws DBAppException {
		
		//if this is the last page return
		if (getNextPage()==null) {
			DBApp.writeObject(this, getPath());
			return;
		}
		
			
		//if this is not the last page insert in it the first element of the next page and remove the element from the next page
		LeafPage next = (LeafPage) DBApp.readObject(getNextPage());
		this.pointers.add(next.pointers.remove(0));
		DBApp.writeObject(this, getPath());
		
		//if next page is empty delete it
		if (next.pointers.size()==0 && next.nextPage==null) {
			//delete the page
			next.deletePage();
			//update the current page's next pointer and save the current page			
			this.nextPage = null;
			DBApp.writeObject(this, getPath());
			return;
		}
		//shift the rest of the pages
		else
			next.shiftBack();
		
	}
	
	
	public int indexOf(String key, boolean binary) {
		
		//search binary or linearly for the index of the key
		//return -1 if the key doesn't exist
				
		//binary search for the index of the key
		if (binary) {

			int l = 0;
			int r = pointers.size() - 1;
			int m = l + (r-l+1)/2;
			int result = -1;

			while (l <= r) {
				m = l + (r-l+1)/2;
				if (key.compareTo(pointers.get(m)) == 0) {
					result = m;
					break;
				}
				else if (key.compareTo(pointers.get(m)) < 0) {
					r = m-1;
				}
				else if (key.compareTo(pointers.get(m)) > 0) {
					l = m+1;
				}
			}

			return result;
		}
		//linear search for the index of the key
		else {
			for (int i=0 ; i<pointers.size() ; i++)
				if ( key.equals(pointers.get(i)) )
					return i;
			return -1;
		}
	}
	
	
	public void deletePage() throws DBAppException {
		File pageFile = new File(getPath());
		if (!pageFile.delete())
			throw new DBAppException("An error occured while deleting the leaf page.");
	}
	
	
}
