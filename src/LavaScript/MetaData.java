package LavaScript;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;

public class MetaData {

	public static void CreateNEWMetaFile(String Name, String Key, Hashtable x) {

		String Output = "Table Name, Column Name, Column Type, Key, Indexed \n"
				;


		for (Object o : x.entrySet()) {
			Map.Entry entry = (Map.Entry) o;
			String y = (String) entry.getKey();
			Object u = entry.getValue();
			String Type = u.toString();

			Output = Output+Name+","+ y + "," + Type + ",";

			if (y == Key) {
				Output = Output + "TRUE,FALSE\n";

			} else {
				Output = Output + "FALSE,FALSE\n";
				
			}

		}
		
		
		
		File ExampleB = new File("data//metadata.csv");

		byte[] data1 = Output.getBytes(StandardCharsets.UTF_8);
		try (FileOutputStream fos = new FileOutputStream(ExampleB)) {
			fos.write(data1);
			System.out.println("Successfully written data to the file");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		
		
		
	}

	public void AddOnMetaFile(String Name, String Key, Hashtable x) {
		
		String str="";
		
		Path pathtoTables = Paths.get("data//metadata.csv");
		
		try {
			byte[] fileContents = Files.readAllBytes(pathtoTables);
			 str = new String(fileContents, "UTF-8");
			

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		
		
		
		
		
		}
		String Output = "";


		for (Object o : x.entrySet()) {
			Map.Entry entry = (Map.Entry) o;
			String y = (String) entry.getKey();
			Object u = entry.getValue();
			String Type = u.toString();

			Output = Output+Name+","+ y + "," + Type + ",";

			if (y == Key) {
				Output = Output + "True,False";

			} else {
				Output = Output + "False,False\n";
			}

		}
		
		
		
		 
		str=str+"\n"+Output;
		byte[] data1 = str.getBytes(StandardCharsets.UTF_8);
		try (FileOutputStream fos = new FileOutputStream(new File("metadata.csv"))) {
			fos.write(data1);
			System.out.println("Successfully written data to the file");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		
	}

}