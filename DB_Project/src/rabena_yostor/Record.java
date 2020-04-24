package rabena_yostor;

import java.io.Serializable;
import java.util.Vector;

public class Record implements Serializable {
	
	private static final long serialVersionUID = 4659753799213468841L;
	private Vector<Object> attributes;
	
	
	public Record() {
		attributes = new Vector<Object>();
	}
	
	
	public Vector<Object> getAttributes() {
		return attributes;
	}

	public void setAttributes(Vector<Object> attributes) {
		this.attributes = attributes;
	}
	
	
	public boolean equals(Object obj) {
		Record r = (Record ) obj;
		for(int i =0;i<attributes.size();i++)
			if(!attributes.get(i).equals(r.getAttributes().get(i)))
				return false;
		return true;
	}
	
	
	public String toString() {
		String s = "";
		for (Object o: attributes) {
			s += o.toString() + ", ";
		}
		s = s.substring(0, s.length()-2);
		return s;
	}
	
	
	public boolean checkDeleteRecord(Vector<Object> columnValues) {
		
		for (int j=0 ; j<attributes.size()-1 ; j++)
			if ( (columnValues.get(j) != null) && !(attributes.get(j).equals(columnValues.get(j))) )
				return false;
		
		return true;
	}
	
	
}
