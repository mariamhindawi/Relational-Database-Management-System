package rabena_yostor;

import java.awt.Polygon;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Hashtable;

public class DBAppTest {
	
	
	public static void main(String[] args) throws ParseException {
		
		//initializing the DBApp
//		DBApp app = new DBApp();
//		app.init();
		
		
		
		//creating a table
//		try {
//			Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
//			htblColNameType.put("id", "java.lang.Integer");
//			htblColNameType.put("name", "java.lang.String");
//			htblColNameType.put("gpa", "java.lang.Double");
//			htblColNameType.put("male", "java.lang.Boolean");
//			htblColNameType.put("birthdate", "java.util.Date");
//			htblColNameType.put("polygon", "java.awt.Polygon");
//			app.createTable("student", "id", htblColNameType);
//		}
//		catch (DBAppException e) {
//			System.out.println(e.getMessage());
//			e.printStackTrace();
//		}
		
		
		
		//creating an index
//		try {
//			app.createBTreeIndex("student", "id");
//			//app.createRTreeIndex("student", "polygon");
//		}
//		catch (DBAppException e) {
//			System.out.println(e.getMessage());
//			e.printStackTrace();
//		}
		
		
		
		//inserting into table
//		try {
//			Polygon p = new Polygon();
//			p.addPoint(0, 0);
//			p.addPoint(3, 3);
//			
//			System.out.println("Insert:");
//			System.out.println();
//			Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
//			htblColNameValue.put("id", 6);
//			htblColNameValue.put("name", "Saad");
//			htblColNameValue.put("gpa", 4.6);
//			htblColNameValue.put("male", true);
//			htblColNameValue.put("birthdate", new SimpleDateFormat("yyyy-MM-dd").parse("2003-12-20"));
//			htblColNameValue.put("polygon", p);
//			app.insertIntoTable("student", htblColNameValue);
//			
//			//load and view table
//			String currentDirectory = System.getProperty("user.dir");
//			Table t = (Table) DBApp.readObject(currentDirectory + "\\data\\tables\\student\\student.ser");
//			t.viewTable();
//			System.out.println();
//			
//			//load and view index
//			BPTree tree = (BPTree) DBApp.readObject(currentDirectory + "\\data\\tables\\student\\indices\\id\\bptree.ser");
//			tree.view();
//			System.out.println();
//			tree.viewPointers();
//			
//			BPTree tree1 = (BPTree) DBApp.readObject(currentDirectory + "\\data\\tables\\student\\indices\\gpa\\bptree.ser");
//			tree1.view();
//			System.out.println();
//			tree1.viewPointers();
//			
//			BPTree tree2 = (BPTree) DBApp.readObject(currentDirectory + "\\data\\tables\\student\\indices\\name\\bptree.ser");
//			tree2.view();
//			System.out.println();
//			tree2.viewPointers();
//			
//			BPTree tree3 = (BPTree) DBApp.readObject(currentDirectory + "\\data\\tables\\student\\indices\\birthdate\\bptree.ser");
//			tree3.view();
//			System.out.println();
//			tree3.viewPointers();
//			
//			BPTree tree4 = (BPTree) DBApp.readObject(currentDirectory + "\\data\\tables\\student\\indices\\male\\bptree.ser");
//			tree4.view();
//			System.out.println();
//			tree4.viewPointers();
//			
//			RTree tree5 = (RTree) DBApp.readObject(currentDirectory + "\\data\\tables\\student\\indices\\polygon\\rtree.ser");
//			tree5.view();
//			System.out.println();
//			tree5.viewPointers();
//		}
//		catch (DBAppException e) {
//			System.out.println(e.getMessage());
//			e.printStackTrace();
//		}
		
		
		
		//deleting from table
//		try {
//			Polygon p = new Polygon();
//			p.addPoint(0, 0);
//			p.addPoint(5, 5);
//			
//			System.out.println("Delete:");
//			System.out.println();
//			Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
////			htblColNameValue.put("id", 40);
////			htblColNameValue.put("name", "Joe");
////			htblColNameValue.put("gpa", 0.7);
////			htblColNameValue.put("male", false);
////			htblColNameValue.put("birthdate", new SimpleDateFormat("yyyy-MM-dd").parse("2013-12-20"));
////			htblColNameValue.put("polygon", p);
////			app.deleteFromTable("student", htblColNameValue);
//			
//			//load and view table
//			String currentDirectory = System.getProperty("user.dir");
//			Table t = (Table) DBApp.readObject(currentDirectory + "\\data\\tables\\student\\student.ser");
//			t.viewTable();
//			System.out.println();
//			
//			//load and view index
//			BPTree tree = (BPTree) DBApp.readObject(currentDirectory + "\\data\\tables\\student\\indices\\id\\bptree.ser");
//			tree.view();
//			System.out.println();
//			tree.viewPointers();
//			
//			BPTree tree1 = (BPTree) DBApp.readObject(currentDirectory + "\\data\\tables\\student\\indices\\gpa\\bptree.ser");
//			tree1.view();
//			System.out.println();
//			tree1.viewPointers();
//			
//			BPTree tree2 = (BPTree) DBApp.readObject(currentDirectory + "\\data\\tables\\student\\indices\\name\\bptree.ser");
//			tree2.view();
//			System.out.println();
//			tree2.viewPointers();
//			
//			BPTree tree3 = (BPTree) DBApp.readObject(currentDirectory + "\\data\\tables\\student\\indices\\birthdate\\bptree.ser");
//			tree3.view();
//			System.out.println();
//			tree3.viewPointers();
//			
//			BPTree tree4 = (BPTree) DBApp.readObject(currentDirectory + "\\data\\tables\\student\\indices\\male\\bptree.ser");
//			tree4.view();
//			System.out.println();
//			tree4.viewPointers();
//			
//			RTree tree5 = (RTree) DBApp.readObject(currentDirectory + "\\data\\tables\\student\\indices\\polygon\\rtree.ser");
//			tree5.view();
//			System.out.println();
//			tree5.viewPointers();
//		}
//		catch (DBAppException e) {
//			System.out.println(e.getMessage());
//			e.printStackTrace();
//		}
		
		
		
		//updating table
//		try {
//			Polygon p = new Polygon();
//			p.addPoint(0, 0);
//			
////			Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>( );
////			htblColNameValue.put("id", 0);
////			htblColNameValue.put("name", "Boody");
////			htblColNameValue.put("gpa", 0.0);
////			htblColNameValue.put("male", false);
////			htblColNameValue.put("birthdate", new SimpleDateFormat("yyyy-MM-dd").parse("1998-7-28"));
////			htblColNameValue.put("polygon", p);
//			app.updateTable("student", "(0,0),(3,3)", htblColNameValue);
//			
//			//load and view table
//			String currentDirectory = System.getProperty("user.dir");
//			Table t = (Table) DBApp.readObject(currentDirectory + "\\data\\tables\\student\\student.ser");
//			t.viewTable();
//			System.out.println();
//			
//			//load and view index
//			BPTree tree = (BPTree) DBApp.readObject(currentDirectory + "\\data\\tables\\student\\indices\\id\\bptree.ser");
//			tree.view();
//			System.out.println();
//			tree.viewPointers();
//			
//			BPTree tree1 = (BPTree) DBApp.readObject(currentDirectory + "\\data\\tables\\student\\indices\\gpa\\bptree.ser");
//			tree1.view();
//			System.out.println();
//			tree1.viewPointers();
//			
//			BPTree tree2 = (BPTree) DBApp.readObject(currentDirectory + "\\data\\tables\\student\\indices\\name\\bptree.ser");
//			tree2.view();
//			System.out.println();
//			tree2.viewPointers();
//			
//			BPTree tree3 = (BPTree) DBApp.readObject(currentDirectory + "\\data\\tables\\student\\indices\\birthdate\\bptree.ser");
//			tree3.view();
//			System.out.println();
//			tree3.viewPointers();
//			
//			BPTree tree4 = (BPTree) DBApp.readObject(currentDirectory + "\\data\\tables\\student\\indices\\male\\bptree.ser");
//			tree4.view();
//			System.out.println();
//			tree4.viewPointers();
//			
//			RTree tree5 = (RTree) DBApp.readObject(currentDirectory + "\\data\\tables\\student\\indices\\polygon\\rtree.ser");
//			tree5.view();
//			System.out.println();
//			tree5.viewPointers();
//		}
//		catch (DBAppException e) {
//			System.out.println(e.getMessage());
//			e.printStackTrace();
//		}
		
		
	}
		
}
