package rabena_yostor;

import java.awt.Dimension;
import java.awt.Polygon;
import java.io.Serializable;
import java.util.Arrays;

public class MyPolygon implements Comparable, Serializable {
	
	private static final long serialVersionUID = 8734644135354853421L;
	private Polygon polygon;
	
	
	public MyPolygon(Polygon p) {
		polygon = p;
	}
	
	
	public Polygon getPolygon() {
		return polygon;
	}
	
	public void setPolygon(Polygon polygon) {
		this.polygon = polygon;
	}
	
	
	
	public int compareTo(Object o) {
		MyPolygon p = (MyPolygon) o;
		Dimension dim1 = this.polygon.getBounds().getSize();
		Dimension dim2 = p.polygon.getBounds().getSize();
		int area1 = dim1.width * dim1.height;
		int area2 = dim2.width * dim2.height;
		
		if (area1==area2)
			return 0;
		else if (area1>area2)
			return 1;
		else
			return -1;
	}
	
	
	public boolean equals(Object obj) {
		MyPolygon myp = (MyPolygon) obj;
		return Arrays.equals(this.polygon.xpoints, myp.polygon.xpoints) && Arrays.equals(this.polygon.ypoints, myp.polygon.ypoints);
	}
	
	
	public String toString() {
		return "" + polygon.getBounds().height * polygon.getBounds().width + " (metres squared)";
	}
	
	
}