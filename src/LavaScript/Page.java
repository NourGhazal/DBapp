package LavaScript;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Vector;

public class Page implements Serializable {
	
	String PageName;
	Vector vector= new Vector();
	
	 
	
	
	
	public static Page Deserialize(String Path) {
		try {

			ObjectInputStream in = new ObjectInputStream(new FileInputStream(Path));
			Page p = (Page) in.readObject();
			in.close();
			return p;

		} catch (IOException i) {
			i.printStackTrace();		} 
		catch (ClassNotFoundException c) {
			c.printStackTrace();

		}
		return null;
	}
	
	
	
	
	public static void  Serialize(String Path,Page x) {
		try {

			ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(Path));
			out.writeObject(x);
			out.close();

		} catch (IOException i) {
			i.printStackTrace();

		} 
	}
	
	
	
	
	

	

}
