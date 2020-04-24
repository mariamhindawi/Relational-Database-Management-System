package rabena_yostor;

import java.io.Serializable;

public abstract class Page implements Serializable {
	
	
	private static final long serialVersionUID = 1675077537702115401L;
	private int maxNumOfEntries;
	private String path;
	
	
	public Page(int maxNumOfEntries, String path) {
		this.maxNumOfEntries = maxNumOfEntries;
		this.path = path;
	}

	
	public int getMaxNumOfEntries() {
		return maxNumOfEntries;
	}

	public void setMaxNumOfEntries(int maxNumOfEntries) {
		this.maxNumOfEntries = maxNumOfEntries;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}
	
	
	
	public abstract void deletePage() throws DBAppException;
	
	
}
