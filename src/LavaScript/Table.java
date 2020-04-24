package LavaScript;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Vector;

public class Table implements Serializable{
	
	String Tablename;
	Vector vector= new Vector();
	
	
	
	
	
	public static Table Deserialize(String Path) {
		try {

			ObjectInputStream in = new ObjectInputStream(new FileInputStream(Path));
			Table t = (Table) in.readObject();
			in.close();
			return t;

		} catch (IOException i) {
			i.printStackTrace();

		} catch (ClassNotFoundException c) {
			System.out.println("Employee class not found");
			c.printStackTrace();

		}
		return null;
	}
	
	
	
	
	public static void  Serialize(String Path,Table x) {
		try {

			ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(Path));
			out.writeObject(x);
			out.close();

		} catch (IOException i) {
			i.printStackTrace();

		} 
	}
	

}
