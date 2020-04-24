package LavaScript;

/*
 * 
 * 
 * 
 * 
 * 
 * 
 * */

import java.nio.charset.StandardCharsets;
import java.awt.Polygon;
import java.awt.RenderingHints.Key;
import java.io.*;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.crypto.spec.DESedeKeySpec;

import java.util.Objects;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
 
public class DBApp  {
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,String[] strarrOperators)throws DBAppException, IOException
	{
		String directoryPath = "data//metadata.csv";
		Path path = Paths.get(directoryPath);
		boolean isDir = Files.exists(path);
		File file = new File(path + "");
		String[] splitter = {};
		Scanner reader = new Scanner(file);
		String str ;
		ArrayList<ArrayList<Object>> ourResult = new ArrayList();
		if(isDir)
		{
			reader.nextLine();
			
			while(reader.hasNextLine())
			{
				str = reader.nextLine();
				for(SQLTerm x : arrSQLTerms)
				{
					
					
					splitter = str.split(",");
					
					if(splitter[2].equals("java.awt.Polygon") && splitter[1].equals(x.strColumnName))
					{
						RTree t = new RTree();
						RTree b =t.Deserialize("data//"+x.strTableName+"RTree"+x.strColumnName+".bin");
						
						if(x.strOperator.equals("="))
						{
							ArrayList<Object> acc = new ArrayList();
							ArrayList<Object> r = b.eappear(b.root, (Comparable) x.objValue, acc) ;
							ourResult.add(r);
						}else if(x.strOperator.equals("<"))
						{
							ArrayList<Object> acc = new ArrayList();
							ArrayList<Object> r = b.sappear(b.root, (Comparable) x.objValue, acc) ;
							ourResult.add(r);
						}else if(x.strOperator.equals(">"))
						{
							
							ArrayList<Object> acc = new ArrayList();
							ArrayList<Object> r = b.gappear(b.root, (Comparable) x.objValue, acc) ;
							ourResult.add(r);
							
						}else if(x.strOperator.equals("!="))
						{
							ArrayList<Object> acc = new ArrayList();
							ArrayList<Object> r = b.gappear(b.root, (Comparable) x.objValue, acc) ;
							ourResult.add(r);
							r = b.sappear(b.root, (Comparable) x.objValue, acc);
							ourResult.get(ourResult.size()-1).addAll(r);
						}else if(x.strOperator.equals(">="))
						{
							ArrayList<Object> acc = new ArrayList();
							ArrayList<Object> r = b.gappear(b.root, (Comparable) x.objValue, acc) ;
							ourResult.add(r);
							r = b.eappear(b.root, (Comparable) x.objValue, acc) ;
							ourResult.get(ourResult.size()-1).addAll(r);
							
						}else
						{
							ArrayList<Object> acc = new ArrayList();
							ArrayList<Object> r = b.sappear(b.root, (Comparable) x.objValue, acc) ;
							ourResult.add(r);
							r = b.eappear(b.root, (Comparable) x.objValue, acc) ;
							ourResult.get(ourResult.size()-1).addAll(r);
						}
						
					}else
					{
						if(splitter[4].equals("TRUE") && splitter[1].equals(x.strColumnName))
						{
							
							BTree t = new BTree();
							BTree b =t.Deserialize("data//"+x.strTableName+"BTree"+x.strColumnName+".bin");
							
							if(x.strOperator.equals("="))
							{
								ArrayList<Object> acc = new ArrayList();
								ArrayList<Object> r = b.eappear(b.root, (Comparable) x.objValue, acc) ;
								ourResult.add(r);
							}else if(x.strOperator.equals("<"))
							{
								ArrayList<Object> acc = new ArrayList();
								ArrayList<Object> r = b.sappear(b.root, (Comparable) x.objValue, acc) ;
								ourResult.add(r);
							}else if(x.strOperator.equals(">"))
							{
								
								ArrayList<Object> acc = new ArrayList();
								ArrayList<Object> r = b.gappear(b.root, (Comparable) x.objValue, acc) ;
								ourResult.add(r);
								
							}else if(x.strOperator.equals("!="))
							{
								ArrayList<Object> acc = new ArrayList();
								ArrayList<Object> r = b.gappear(b.root, (Comparable) x.objValue, acc) ;
								ourResult.add(r);
								r = b.sappear(b.root, (Comparable) x.objValue, acc);
								ourResult.get(ourResult.size()-1).addAll(r);
							}else if(x.strOperator.equals(">="))
							{
								ArrayList<Object> acc = new ArrayList();
								ArrayList<Object> r = b.gappear(b.root, (Comparable) x.objValue, acc) ;
								ourResult.add(r);
								r = b.eappear(b.root, (Comparable) x.objValue, acc) ;
								ourResult.get(ourResult.size()-1).addAll(r);
								
							}else
							{
								ArrayList<Object> acc = new ArrayList();
								ArrayList<Object> r = b.sappear(b.root, (Comparable) x.objValue, acc) ;
								ourResult.add(r);
								r = b.eappear(b.root, (Comparable) x.objValue, acc) ;
								ourResult.get(ourResult.size()-1).addAll(r);
							}
							
							
						}else
						{
							if(splitter[1].equals(x.strColumnName)) {
							String u = "data//"+x.strTableName+".bin";
							Table t = new Table();
							t= t.Deserialize(u);
							Vector names = (Vector) t.Deserialize(u).vector;
							String operator = x.strOperator ;
							int attLocation = -1 ;
							ArrayList<String[]> locations =  getTableInfo(x.strTableName);
							for(String[] array : locations)
							{
								if(array[0].equals(x.strColumnName))
								{
									attLocation =Integer.parseInt(array[1]);
									break;
								}
							}
							if(operator.equals("="))
							{
								ArrayList<Object> r = equality(names, x.strTableName, x.objValue, attLocation);
								ourResult.add(r);
							}
							else if(operator.equals(">"))
							{
								ArrayList<Object> r = greater(names, x.strTableName, x.objValue, attLocation);
								ourResult.add(r);
							}
							else if(operator.equals("<"))
							{
								ArrayList<Object> r = smaller(names, x.strTableName, x.objValue, attLocation);
								ourResult.add(r);
							}
							else if(operator.equals("!="))
							{
								ArrayList<Object> r = greater(names, x.strTableName, x.objValue, attLocation);
								ourResult.add(r);
								r = smaller(names, x.strTableName, x.objValue, attLocation);
								ourResult.get(ourResult.size()-1).addAll(r);
							}
							else if(operator.equals(">="))
							{
								ArrayList<Object> r = equality(names, x.strTableName, x.objValue, attLocation);
								ourResult.add(r);
								r = greater(names, x.strTableName, x.objValue, attLocation);
								ourResult.get(ourResult.size()-1).addAll(r);
							}
							else if(operator.equals("<="))
							{
								ArrayList<Object> r = equality(names, x.strTableName, x.objValue, attLocation);
								ourResult.add(r);
								
								r = smaller(names, x.strTableName, x.objValue, attLocation);
								ourResult.get(ourResult.size()-1).addAll(r);
							}
							
						}
						}
					}
					
				}
			} //end of while
			
			for(int i = 0 ; i<strarrOperators.length ; i++)
			{
				ArrayList<Object> result = new ArrayList<Object>();
				ArrayList<Object> x = ourResult.get(i);
			//	System.out.println("X");
			//	System.out.println(x);
				ArrayList<Object> y = ourResult.get(i+1);
				if(strarrOperators[i].equals("AND"))
				{
					for(Object record : x)
					{
						ArrayList<Object> row = (ArrayList<Object>) record;
						if(y.contains(row))
						{
							result.add(row);
						}
					}
					ourResult.remove(i+1);
					ourResult.add(i+1, result);
					
					
				}else if(strarrOperators[i].equals("OR"))
				{
					
					for(Object record : x)
					{
						ArrayList<Object> row = (ArrayList<Object>) record;
						result.add(row);
						
					}
					for(Object record : y)
					{
						ArrayList<Object> row =  (ArrayList<Object>) record;
						if(x.contains(row))
							continue;
						else {
							result.add(row);
					//	System.out.println("HI");	
						}
					}
					//System.out.println("new Print");
					for(ArrayList<Object>c : ourResult) {
			//			System.out.println(c);
					}
					ourResult.remove(i+1);
					ourResult.add(i+1, result);
				//	System.out.println("after");
					for(ArrayList<Object>c : ourResult) {
					//	System.out.println(c);
					}
				//	System.out.println("finished");
					
				}else if(strarrOperators[i].equals("XOR"))
				{
					for(Object record : x)
					{
						ArrayList<Object> row = (ArrayList<Object>) record;
						if(y.contains(row))
							continue;
						else
							result.add(row);
						
					}
					for(Object record : y)
					{
						ArrayList<Object> row =  (ArrayList<Object>) record;
						if(x.contains(row))
							continue;
						else
							result.add(row);
					}
					ourResult.remove(i+1);
					ourResult.add(i+1, result);
				}
				
			}
			
			
		}
		ArrayList<Object> x  = ourResult.get(ourResult.size()-1);
		Iterator lastResult = x.iterator();
		return lastResult;
		
	}
	static ArrayList<String> TablesNames = new ArrayList<String>();
	static File fileTable = new File("TablesNameCreated.txt");

    public static ArrayList<Object> equality(Vector names , String tableName , Comparable<Object> objValue , int location)
    {
        ArrayList<Object> res = new ArrayList<Object>();
        for(Object c : names)
        {
            
            String i = (String) c;
            Page check = Page.Deserialize("data//"+tableName+i+".bin")    ;
            Vector rows = check.vector;
            
            //
            for(Object row : rows)
            {
                ArrayList<Comparable<Object>> finalRow = (ArrayList<Comparable<Object>>) row ;
                if(finalRow.get(location).compareTo(objValue)==0)
                {
                    res.add(row);
                }
            }
                
        }

        
        
        return res;
    }

    public static ArrayList<Object> greater(Vector names , String tableName , Comparable<Object> objValue , int location)
    {
        ArrayList<Object> res = new ArrayList<Object>();
        for(Object c : names)
        {
            
            String i = (String) c;
            Page check = Page.Deserialize("data//"+tableName+i+".bin")    ;
            Vector rows = check.vector;
            
            //
            for(Object row : rows)
            {
                ArrayList<Comparable<Object>> finalRow = (ArrayList<Comparable<Object>>) row ;;
                if(finalRow.get(location).compareTo(objValue)>0)
                {
                    res.add(row);
                }
            }
                
        }

        
        
        return res;
    }
    public static ArrayList<Object> smaller(Vector names , String tableName , Comparable<Object> objValue , int location)
    {
        ArrayList<Object> res = new ArrayList<Object>();
        for(Object c : names)
        {
            
            String i = (String) c;
            Page check = Page.Deserialize("data//"+tableName+i+".bin")    ;
            Vector rows = check.vector;
            
            //
            for(Object row : rows)
            {
                ArrayList<Comparable<Object>> finalRow = (ArrayList<Comparable<Object>>) row ;
                if(finalRow.get(location).compareTo(objValue)<0)
                {
                    res.add(row);
                }
            }
                
        }

        
        
        return res;
    }
public static void main(String[] args) throws DBAppException {}

	
	////  new methodd

	static void print(String name) {
		int i=0;
		boolean f=true;
		while(f) {

			String path="data//"+name+"P"+i+".bin";
			File file = new File(path);
		if(file.exists() && !file.isDirectory()) {
			Page	p=Page.Deserialize(path);
			for(Object o:p.vector) {
				ArrayList<Object> tuple=  (ArrayList<Object>) o ;
			
					CompareablePolygons fs =new CompareablePolygons(tuple.get(0));
					//System.out.println(fs+" area ");
					//System.out.println(tuple.get(1)+" name");
				//	System.out.println(tuple.get(2)+" id ");
					
					
				
				
			}
			
			
			i++;
				}
				else	
				f=!f;
				
				}
		
	}

	public void createBTreeIndex(String strTableName,String strColName) throws DBAppException 
	{
		
		
		
		String directoryPath = "data//metadata.csv";
		String[] splitter = {};
		Path path = Paths.get(directoryPath);
		boolean isDir = Files.exists(path);

		String str = "";
		String Writer="";

		if (isDir) {
			Path pathtoTables = Paths.get("data//metadata.csv");
			try {
				byte[] fileContents = Files.readAllBytes(pathtoTables);
				
				str = new String(fileContents, "UTF-8");
				
				splitter = str.split(strTableName+",");
				
				if(splitter.length<1)
				{
				throw	new DBAppException("You didnt create a table with this Name,Pleasew check the Name");
					
				}
				Writer=Writer+str.split(strTableName+",")[0];
				for(int i =1;i<splitter.length;i++)
					
				{
					String[] MetaTableRowwithSpaces=str.split(strTableName+",")[i].split("/n");
					
					for(int j=0;j<MetaTableRowwithSpaces.length;j++)
					{
						
						String[] MetaTableRow=MetaTableRowwithSpaces[0].split(",");
						
						String ColumnName = MetaTableRow[0];
						String Index = MetaTableRow[3];
						if(ColumnName.equals(strColName)) 
						{
							if(Index=="TRUE")
							{
								return;
							}
							else 
							{
								Index= "TRUE";
								Writer=Writer+strTableName+","+MetaTableRow[0]+","+MetaTableRow[1]+","+MetaTableRow[2]+","+Index+"\n";
								
							}	
								
							
							
						}
						else
						{
							Writer = Writer+strTableName+","+MetaTableRowwithSpaces[0];
						}

					}
				}
				
			
			
						BTree t = new BTree();
						t.root = null;
						t.Serialize("data//"+strTableName+"BTree"+strColName+".bin");
				byte[] data1 = Writer.getBytes(StandardCharsets.UTF_8);
				try (FileOutputStream fos = new FileOutputStream(new File("data//metadata.csv"))) {
					fos.write(data1);
					//System.out.println("Successfully written data to the file");
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				
				
				

			} catch (IOException e) {
				
				
				// TODO Auto-generated catch block
			}

		}
		else
		{
		throw new DBAppException("You did not create any table you want to use B+ tree on ");
		}
		
		
		//////////////////here we start  this code inserts all the previous tuples on the tree
		//boolean add(Object value,String TableName,String ColName,ArrayList<Object> array)
		int index = getcolumnNmuber(strTableName, strColName);
		
		

		int i=0;
		boolean f=true;
		while(f) {

			String paths="data//"+strTableName+"P"+i+".bin";
			File file = new File(paths);
		if(file.exists() && !file.isDirectory()) {
			Page	p=Page.Deserialize(paths);
			for(Object o:p.vector) {
				ArrayList<Object> tuple=  (ArrayList<Object>) o ;
				BTree bts= new BTree();
					boolean flag=	bts.add(tuple.get(index),strTableName,strColName,tuple);
	
				
			}
			
			
			i++;
				}
				else	
				f=!f;
				
				}
		
	
		
		
		
		
		
	}
	
	
	public void createRTreeIndex(String strTableName,String strColName) throws DBAppException 
	{
		
		
		
		String directoryPath = "data//metadata.csv";
		String[] splitter = {};
		Path path = Paths.get(directoryPath);
		boolean isDir = Files.exists(path);

		String str = "";
		String Writer="";

		if (isDir) {
			Path pathtoTables = Paths.get("data//metadata.csv");
			try {
				byte[] fileContents = Files.readAllBytes(pathtoTables);
				
				str = new String(fileContents, "UTF-8");
				
				splitter = str.split(strTableName+",");
				
				if(splitter.length<1)
				{
				throw	new DBAppException("You didnt create a table with this Name,Pleasew check the Name");
					
				}
				Writer=Writer+str.split(strTableName+",")[0];
				for(int i =1;i<splitter.length;i++)
					
				{
					String[] MetaTableRowwithSpaces=str.split(strTableName+",")[i].split("/n");
					
					for(int j=0;j<MetaTableRowwithSpaces.length;j++)
					{
						
						String[] MetaTableRow=MetaTableRowwithSpaces[0].split(",");
						
						String ColumnName = MetaTableRow[0];
						String Index = MetaTableRow[3];
						if(ColumnName.equals(strColName)) 
						{
							if(Index=="TRUE")
							{
								return;
							}
							else 
							{
								Index= "TRUE";
								Writer=Writer+strTableName+","+MetaTableRow[0]+","+MetaTableRow[1]+","+MetaTableRow[2]+","+Index+"\n";
								
							}	
								
							
							
						}
						else
						{
							Writer = Writer+strTableName+","+MetaTableRowwithSpaces[0];
						}

					}
				}
				
			
			
						RTree t = new RTree();
						t.root = null;
						t.Serialize("data//"+strTableName+"RTree"+strColName+".bin");
				byte[] data1 = Writer.getBytes(StandardCharsets.UTF_8);
				try (FileOutputStream fos = new FileOutputStream(new File("data//metadata.csv"))) {
					fos.write(data1);
					//System.out.println("Successfully written data to the file");
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				
				
				

			} catch (IOException e) {
				
				
				// TODO Auto-generated catch block
			}

		}
		else
		{
		throw new DBAppException("You did not create any table you want to use B+ tree on ");
		}
		
		
		//////////////////here we start  this code inserts all the previous tuples on the tree
		//boolean add(Object value,String TableName,String ColName,ArrayList<Object> array)
		int index = getcolumnNmuber(strTableName, strColName);
		
		

		int i=0;
		boolean f=true;
		while(f) {

			String paths="data//"+strTableName+"P"+i+".bin";
			File file = new File(paths);
		if(file.exists() && !file.isDirectory()) {
			Page	p=Page.Deserialize(paths);
			for(Object o:p.vector) {
				ArrayList<Object> tuple=  (ArrayList<Object>) o ;
				RTree bts= new RTree();
				Polygon pst=(Polygon) tuple.get(index);
					boolean flag=	bts.add(pst,strTableName,strColName,tuple);
	
				
			}
			
			
			i++;
				}
				else	
				f=!f;
				
				}
		
	
		
		
		
		
		
	}


	
	public static ArrayList<ArrayList> deleteFromTable2(String strTableName,Hashtable<String,Object> htblColNameValue) throws DBAppException {
		ArrayList<ArrayList> ret=new ArrayList<ArrayList>();

		try {
		String path = "data//"+strTableName+".bin";
		Table t = new Table();
		t= t.Deserialize(path);
		Vector names = (Vector) t.Deserialize(path).vector;
		//Set<String> keys = htblColNameValue.keySet();
		//_______________________________
		ArrayList<String[] > atributes =getTableInfo(strTableName);
		// 	System.out.println(names);
			ret=deletehelp2(names,htblColNameValue,atributes, strTableName);
		//___________________________________
		
			if(names.size()==0) {
				////////////////////////////here we are
//				System.out.println("There is no record in the TABLE");
				
				return null;
			}
			
			
			for(int i=0;i<names.size();i++)
		 {
			String pageName = (String)names.get(i);
			boolean boftek=false;

			Page check = Page.Deserialize("data//"+strTableName+pageName+".bin")	;
			if(check.vector.isEmpty()) {
				File deletePage = new File("data//"+strTableName+pageName+".bin");
				
				deletePage.delete();
				names.remove(names.get(i));
				
				t.vector=names;
				
				if(t.vector.size()==0)
				{
					 deletePage = new File("data//"+strTableName+".bin");
					 deletePage.delete();
					 boftek= true;
				}
				i--;
			}
			if(!boftek)
			t.Serialize("data//"+strTableName+".bin", t);
		}
		
		
		 
		
	}
		catch (Exception e) {
			e.printStackTrace();
			throw new DBAppException("The page you want to delete inside is not created yet");
			
			}
		return ret;
		
		
	}
	
	
	//////////////////////////////////////hallo world
	public static ArrayList<ArrayList> deletehelp2(Vector names,Hashtable<String,Object> values,ArrayList<String[]> atributes,String strTableName) throws DBAppException {
		
	
		
		ArrayList<ArrayList> ret=new ArrayList<ArrayList>();
		try {
		
			for(Object name:names) {
				
				String depath = (String) name;
				Page p = Page.Deserialize("data//"+strTableName+depath+".bin");
				Vector vector = p.vector;
				Vector newvector = new Vector();
				//_____________________________________
			for( Object x : vector) {
				ArrayList<Object> y = (ArrayList) x;
				boolean flag= true; 
				int p1=0;
				String[] Keysets = {};
				Set<String> set = values.keySet();
				 Keysets=(set).toArray(Keysets);
				
				boolean isthere =false;
				
					
					for(int i0=0;i0<Keysets.length;i0++) {
						
						int counter2=0;
						for(String[] index2 :atributes) {
							if(Keysets[i0].equals(index2[0]))
							{
								isthere=true;
								break;
								
								
							}
							else {counter2++; isthere=false;}
									
							
							
							
						}
						
						if(!isthere) {
					//		System.out.println("the column type u input is not right (You inserted a column name in A wrong way)");
							}
						
						if(!(values.get(Keysets[i0]).getClass().toString().equals("class java.awt.Polygon"))) 
						{
							Comparable j = (Comparable) values.get(Keysets[i0]);
							Comparable j2 = (Comparable) y.get(counter2);
							
						if(j.compareTo(j2)!=0)
						{
							flag=false;
							break;
						}
						else {
							
							flag=true;
							if(i0==Keysets.length-1)
							{
							if(isBTreeIndexed(strTableName)) {
								ArrayList<String> abdob =getIndexedBTree(strTableName);
								
								//System.out.println(abdob);
								for(String colNa:abdob) {
									int indexb=getcolumnNmuber(strTableName, colNa);
									
									Object value1 =y.get(indexb);
									
									Comparable value = (Comparable) value1;
									ArrayList<Object>hello = (ArrayList<Object>) x;
									ArrayList<Object> myattributes=new ArrayList<Object>();
									
									
										BTree bts= new BTree();
//									System.out.println(value);
//									System.out.println(hello);
//									System.out.println(colNa);
										bts.delete(strTableName,value, hello, colNa);
						
									
									
									
								}
								
								
								
							}
						}
							
						}
						
					}
						else
						{
							
							
							
							CompareablePolygons j = new CompareablePolygons(values.get(Keysets[i0])) ;
							Object j2 =  y.get(counter2);
						if(j.compareTo(j2)!=0)
						{
							flag=false;
							break;
						}
						else {
							
							flag=true;
							
							if(i0==Keysets.length-1)
							{
							if(isRTreeIndexed(strTableName)) {
								ArrayList<String> abdob =getIndexedRTree(strTableName);
						//		System.out.println(abdob);
								for(String colNa:abdob) {
									int indexb=getcolumnNmuber(strTableName, colNa);
									Object value1 =values.get(colNa);
									Comparable value = (Comparable) value1;
									ArrayList<Object>hello = (ArrayList<Object>) x;
									ArrayList<Object> myattributes=new ArrayList<Object>();
									
									
										RTree bts= new RTree();
									//	System.out.println(hello);
										bts.delete(strTableName,(CompareablePolygons)value, hello, colNa);
						
									
									
									
								}
								
								
								
							}
						}
							
						}
						}
					
					
					}
				
						if(!flag) 
							newvector.add(x);
						else
							ret.add((ArrayList) x);
							
					
			
			}//____________________________________________
			p.vector=newvector;
			p.Serialize("data//"+strTableName+depath+".bin", p);
			
			}
			}
			catch (Exception e) {
				
				
				throw new DBAppException("The page is empty there is no records inside the TABLE");
			}
		return ret;
	}
	
	public static boolean helpispolygon(String tablename) throws DBAppException {
		
		//File file=new File("/Users/apple/eclipse-workspace/DBMS/src/metadata 2.csv");
		try {
			Scanner scanner = new Scanner(new File("data//metadata.csv"));
			while (scanner.hasNextLine()) {
				String[] x =scanner.nextLine().split(",");
				if(x[0].equals(tablename)&&x[2].equals("java.awt.Polygon")&&x[3].equals("TRUE")) 
					return true;
				}
			scanner.close();
		} 
		catch (Exception e) {
			throw new DBAppException("File not found.");
		}
		return false;
	}
	public static Polygon helpCreatePolygon2(String x) {
		int noTuples=0;
		for(int i=0;i<x.length();i++) {
			if(x.charAt(i)=='(')
			noTuples++;
		}
		int[] xs=new int[noTuples];
		int[] ys=new int[noTuples];
		int i=0;
		ArrayList<String> tuples=new ArrayList<String>();
		String temp="";
		while(i<x.length()) {
			while(i<x.length()&&x.charAt(i)!=')') {
				temp+=x.charAt(i);
				i++;
			}
			temp+=')';
			i+=2;
			if(i<x.length()&&x.charAt(i)==',') {
				i++;
			}
			tuples.add(temp);
			temp="";
			 
		}
		int mycurrenttup=0;
		for(String tuple:tuples) {
			int s =1;
			String subx="";
			String suby="";
			while(tuple.charAt(s)!=',') {
				subx+=tuple.charAt(s);
				s++;
			}
			s++;
			while(s<tuple.length()-1) {
				suby+=tuple.charAt(s);
				s++;
			}
			int tempx =Integer.parseInt(subx);
			int tempy =Integer.parseInt(suby);
			
			xs[mycurrenttup]=tempx;
			ys[mycurrenttup]=tempy;
			mycurrenttup++;
		}
		
		 Polygon pol=new Polygon(xs,ys,xs.length); 

		 return pol;
	 }

	public static CompareablePolygons helpCreatePolygon(String x) {
		int noTuples=0;
		for(int i=0;i<x.length();i++) {
			if(x.charAt(i)=='(')
			noTuples++;
		}
		int[] xs=new int[noTuples];
		int[] ys=new int[noTuples];
		int i=0;
		ArrayList<String> tuples=new ArrayList<String>();
		String temp="";
		while(i<x.length()) {
			while(i<x.length()&&x.charAt(i)!=')') {
				temp+=x.charAt(i);
				i++;
			}
			temp+=')';
			i+=2;
			if(i<x.length()&&x.charAt(i)==',') {
				i++;
			}
			tuples.add(temp);
			temp="";
			 
		}
		int mycurrenttup=0;
		for(String tuple:tuples) {
			int s =1;
			String subx="";
			String suby="";
			while(tuple.charAt(s)!=',') {
				subx+=tuple.charAt(s);
				s++;
			}
			s++;
			while(s<tuple.length()-1) {
				suby+=tuple.charAt(s);
				s++;
			}
			int tempx =Integer.parseInt(subx);
			int tempy =Integer.parseInt(suby);
			
			xs[mycurrenttup]=tempx;
			ys[mycurrenttup]=tempy;
			mycurrenttup++;
		}
		
		 Polygon pol=new Polygon(xs,ys,xs.length); 
		 CompareablePolygons p=new CompareablePolygons(pol);

		 return p;
	 }
///////////////////////////////////////////////bhrsjbfhaekbvkhabvkhab
public  void updateTable(String tableName,String key,Hashtable hash) throws DBAppException {
	DBApp s =new DBApp();
	ArrayList<ArrayList> x =new ArrayList<ArrayList>();
	Hashtable h=new Hashtable();
	if(helpispolygon(tableName))
	h.put(getClustringKey(tableName), helpCreatePolygon2(key));
	else
	{//System.out.println(getClustringKey(tableName));
		h.put(getClustringKey(tableName), key);
	}
	/////////////here 
	x=deleteFromTable2(tableName, h);
	ArrayList<String> att=getColumnsNames(tableName);
	Set<String> set= hash.keySet();
	
	String[] attr=new String[att.size()];
	for(int i=0;i<attr.length;i++) {
		attr[i]=att.get(i);
	}
	ArrayList<Hashtable> answer=new ArrayList<Hashtable>();
	for(ArrayList c:x) {
		h.clear();
		for(int i=0;i<attr.length;i++) {
			h.put(attr[i], c.get(i));
		}
		for(String b:set) {
			h.put(b, hash.get(b));
		}
		s.insertIntoTable(tableName, h);
			
		
	}
	
		
	
	
	
	
}
	/////// new method for update 
public static String getClustringKey(String tablename) throws DBAppException {
		try {
			Scanner scanner = new Scanner(new File("data//metadata.csv"));
			while (scanner.hasNextLine()) {
				String[] x =scanner.nextLine().split(",");
				
				if(x[0].equals(tablename)&&x[3].equals("TRUE"))
				{	
					return x[1];	
				}
				}
			
			
			scanner.close();
			
			
		} 
		catch (Exception e) {
			
			throw new DBAppException("File not found.");
			
			
		}
		return null;
	}
	public static ArrayList<String> getColumnsNames(String tablename) throws DBAppException {
		//File file=new File("/Users/apple/eclipse-workspace/DBMS/src/metadata 2.csv");
		ArrayList<String> w =new ArrayList<String>();
		try {
			Scanner scanner = new Scanner(new File("data//metadata.csv"));
		
			while (scanner.hasNextLine()) {
				String[] x =scanner.nextLine().split(",");
				if(x[0].equals(tablename))
					w.add(x[1]);		
				}
			
			
			scanner.close();
			
			
		} 
		catch (Exception e) {
			
			
			throw new DBAppException("File not found.");
			
		}
		return w;
	}
	 
	
	public  void deleteFromTable(String strTableName,Hashtable<String,Object> htblColNameValue) throws DBAppException {
	// delete from btree 
		
	
		

		
		try {
		String path = "data//"+strTableName+".bin";
		Table t = new Table();
		t= t.Deserialize(path);
		Vector names = (Vector) t.Deserialize(path).vector;
		//Set<String> keys = htblColNameValue.keySet();
		//_______________________________
		ArrayList<String[]	> atributes =getTableInfo(strTableName);
	//	System.out.println(names);
			deletehelp(names,htblColNameValue,atributes, strTableName);
		//___________________________________
		
			if(names.size()==0) {
				
				
				throw new DBAppException("There is no record in the TABLE");

				
			}
			
			
			for(int i=0;i<names.size();i++)
		 {
			String pageName = (String)names.get(i);
			
			Page check = Page.Deserialize("data//"+strTableName+pageName+".bin")	;
			if(check.vector.isEmpty()) {
				File deletePage = new File("data//"+strTableName+pageName+".bin");
				
				deletePage.delete();
				names.remove(names.get(i));
				t.vector=names;
				i--;
			}
			t.Serialize("data//"+strTableName+".bin", t);
		}
		
		
		 
		
	}
		catch (Exception e) {
			throw new DBAppException("The page you want to delete inside is not created yet");
				}
		
		
	}
	public static void deletehelp(Vector names,Hashtable<String,Object> values,ArrayList<String[]> atributes,String strTableName) throws DBAppException {
		try {
	//	System.out.println(values);
			for(Object name:names) {
				String depath = (String) name;
				Page p = Page.Deserialize("data//"+strTableName+depath+".bin");
				Vector vector = p.vector;
				Vector newvector = new Vector();
				//_____________________________________
			for( Object x : vector) {
				ArrayList<Object> y = (ArrayList) x;
				boolean flag= true; 
				int p1=0;
				Set<String> set = values.keySet();
				String[] Keysets = new String[values.keySet().size()];
int tofa7=0;
				for(String kd : set) {
					Keysets[tofa7++]=kd;
					
				}
				 
				boolean isthere =false;
				
					
					for(int i0=0;i0<Keysets.length;i0++) {
						
						int counter2=0;
						for(String[] index2 :atributes) {
							
							if(Keysets[i0].equals(index2[0]))
							{
								
								isthere=true;
								break;
								
								
							}
							else {counter2++; isthere=false;}
									
							
							
							
						}
						
						if(!isthere) {
							
							throw new DBAppException("the column type u input is not right (You inserted a column name in A wrong way)");

							
							}
				//		System.out.println(Keysets+ " hhhhhhhhhhhhhhhhhhhhhhhhhh");
						if(!(values.get(Keysets[i0]).getClass().toString().equals("class java.awt.Polygon"))) 
						{
							Comparable j = (Comparable) values.get(Keysets[i0]);
					//		System.out.println(j);
							Comparable j2 = (Comparable) y.get(counter2);
						//	System.out.println(j2);
						if(j.compareTo(j2)!=0)
						{
							flag=false;
							break;
						}
						else {
							
							flag=true;
							
							if(i0==Keysets.length-1)
							{
								if(isBTreeIndexed(strTableName)) {
									ArrayList<String> abdob =getIndexedBTree(strTableName);
									
							//		System.out.println(abdob);
									for(String colNa:abdob) {
										int indexb=getcolumnNmuber(strTableName, colNa);
										
										Object value1 =y.get(indexb);
										
										Comparable value = (Comparable) value1;
										ArrayList<Object>hello = (ArrayList<Object>) x;
										ArrayList<Object> myattributes=new ArrayList<Object>();
										
										
											BTree bts= new BTree();
									//	System.out.println(value);
									//	System.out.println(hello);
									//	System.out.println(colNa);
											bts.delete(strTableName,value, hello, colNa);
							
										
										
										
									}
									
									
									
								}
								
								if(isRTreeIndexed(strTableName)) {

									
									
									
								//	CompareablePolygons jt = new CompareablePolygons(values.get(Keysets[i0])) ;
									Object j2t =  y.get(counter2);
						//		System.out.println("y"+y);
						//		System.out.println("values"+values);
									if(i0==Keysets.length-1)
									{
									if(isRTreeIndexed(strTableName)) {
										ArrayList<String> abdob =getIndexedRTree(strTableName);
								//		System.out.println(abdob);
										for(String colNa:abdob) {
											int indexb=getcolumnNmuber(strTableName, colNa);
											Polygon value1 =(Polygon) y.get(indexb);
								//			System.out.println(value1+" value 1");
											CompareablePolygons value=new CompareablePolygons((Polygon)value1);
											ArrayList<Object>hello = (ArrayList<Object>) x;
											ArrayList<Object> myattributes=new ArrayList<Object>();
											
									//		System.out.println(value1+"     0000000000");
								//			System.out.println(value+"     111111111");
 
												RTree bts= new RTree();
												//System.out.println(hello);
												if(null!=value) {
													
						//							System.out.println(value+"ma7ade4 y2dar ye8ayarak");
												bts.delete(strTableName,(CompareablePolygons)value, hello, colNa);
								
											
												}
											
										}
										
										
										
									}
								}
									
								}
								
								}
							}

							
						}
						
					
						else
						{
							
							
							
							CompareablePolygons j = new CompareablePolygons(values.get(Keysets[i0])) ;
							Object j2 =  y.get(counter2);
						if(j.compareTo(j2)!=0)
						{
							flag=false;
							break;
						}
						else {
							
							flag=true;
							
							if(i0==Keysets.length-1)
							{
							if(isRTreeIndexed(strTableName)) {
								ArrayList<String> abdob =getIndexedRTree(strTableName);
							//	System.out.println(abdob);
								for(String colNa:abdob) {
									int indexb=getcolumnNmuber(strTableName, colNa);
									Object value1 =values.get(colNa);
									Comparable value = (Comparable) value1;
									ArrayList<Object>hello = (ArrayList<Object>) x;
									ArrayList<Object> myattributes=new ArrayList<Object>();
									
									
										RTree bts= new RTree();
								//		System.out.println(hello);
										bts.delete(strTableName,(CompareablePolygons)value, hello, colNa);
						
									
									
									
								}
								
								
								
							}
						}
							
						}
						}
					
					
					}
				
							newvector.add(x);
					
					
			
			}//____________________________________________
			p.vector=newvector;
			p.Serialize("data//"+strTableName+depath+".bin", p);
			
			}
			}
			catch (Exception e) {
throw new DBAppException("The page is empty there is no records inside the TABLE");				}
	}
	
	public static ArrayList<String[]> getTableInfo(String tablename) throws DBAppException {
		ArrayList<String[]> tt=new ArrayList<String[]>();
		ArrayList<String[]> ss=new ArrayList<String[]>();

		try {
			Scanner scanner = new Scanner(new File("data//metadata.csv"));
			while (scanner.hasNextLine()) {
				String[] x =scanner.nextLine().split(",");
				if(x[0].equals(tablename))
					tt.add(x);
			}
			
			
			scanner.close();
			for(int i=0;i<tt.size();i++) {
				String [] k=new String[2];
				String [] temp=tt.get(i);
				k[0]=temp[1];
				String l="";
				l+=i;
				k[1]=l;
				ss.add(k);
			}
			
		} 
		catch (Exception e) {
			throw new DBAppException("File not found.");
			
		}
		return ss;
	}
	

	public void createTable(String TableName, String clusteredKey, Hashtable x) throws DBAppException {
		String directoryPath = "TablesNameCreated.txt";
		String[] splitter = {};
		Path path = Paths.get(directoryPath);
		boolean isDir = Files.exists(path);

		String str = "";

		if (isDir) {
			Path pathtoTables = Paths.get("TablesNameCreated.txt");
			try {
				byte[] fileContents = Files.readAllBytes(pathtoTables);
				str = new String(fileContents, "UTF-8");
				splitter = str.split("\n");

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		boolean TableFlag = true;
		for (String b : splitter) {
			if (b.equals(TableName)) {

				TableFlag = false;
			}
		}

		if (TableFlag) {

			directoryPath = "data//metadata.csv";
			path = Paths.get(directoryPath);
			isDir = Files.exists(path);

			if (isDir) {

				MetaData M = new MetaData();
				M.AddOnMetaFile(TableName, clusteredKey, x);

			} else {

				MetaData M = new MetaData();
				M.CreateNEWMetaFile(TableName, clusteredKey, x);

			}

			String InputString = TableName + "\n" + str;

			byte[] data = InputString.getBytes(StandardCharsets.UTF_8);
			try (FileOutputStream fos = new FileOutputStream(fileTable)) {
				fos.write(data);
			//	System.out.println("Successfully written data to the file");
			} catch (IOException e) {
				e.printStackTrace();
			}

		} else {
			throw new DBAppException("There already a table with same name");
		}

	}

	public  void insertIntoTable(String strTableName, Hashtable x) throws DBAppException {
		
		// insert into btree 
		
		
		
		
		
		
		
		
		
		String directoryPath = "TablesNameCreated.txt";
		String[] splitter = {};
		Path path = Paths.get(directoryPath);
		boolean isDir = Files.exists(path);

		String str = "";

		if (isDir) 
		{

			Path pathtoTables = Paths.get("TablesNameCreated.txt");
			try 
			{

				byte[] fileContents = Files.readAllBytes(pathtoTables);
				str = new String(fileContents, "UTF-8");
				splitter = str.split("\n");

			} 
			catch (IOException e) 
			{

				// TODO Auto-generated catch block
				e.printStackTrace();

			}

		}

		boolean TableFlag = false;
		for (String b : splitter) 
		{

			if (b.equals(strTableName)) 
			{

				TableFlag = true;
			}

		}
		if (TableFlag = false) 
		{

	//		System.out.println("you didnt create a Table with this Name");
			return;

		}

		directoryPath = "data//"+strTableName + ".bin";
		path = Paths.get(directoryPath);
		isDir = Files.exists(path);
		if (!isDir) 
		{

			Path pathtoTables = Paths.get("data//metadata.csv");
			String str1 = "";
			try
			{
				byte[] fileContents = Files.readAllBytes(pathtoTables);
				str1 = new String(fileContents, "UTF-8");

			} catch (IOException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			String[] TableMetaRow = str1.split(strTableName+",");
			if (x.size() > TableMetaRow.length)
			{

				throw new DBAppException("You entered a wrong insertion please insert right one");

				
			}

			if (x.isEmpty()) 
			{
				throw new DBAppException("you didnt make a tuple ,please add the tuple You want");
				
			}
			String[] HashKeys = ((((x.keySet().toString()).replace("[", "")).replaceAll("]", ""))).replaceAll(" ", "").split(",");
			Page p = new Page();
			p.PageName = strTableName;
			ArrayList<Object> array = new ArrayList<Object>();
			ArrayList<String> columnName= new ArrayList<String>();
			ArrayList<Object> columnType = new ArrayList<Object>();
			ArrayList<Integer> columnNameIndex= new ArrayList<Integer>();
			for (int i = 1; i < TableMetaRow.length; i++) {
				String FUll = (TableMetaRow[i]).replaceAll(" ", "");
				String[] FullTable = FUll.split("\n");
				
				
				String[] TupleMeta = (FullTable[0].split(","));
				String TupleType = (TupleMeta[1]).replace(" ", "");    

				String ColumnName = (TupleMeta[0]);
				String typer= (TupleMeta[3]).replace(" ", "");
				int HashKeySize = 0;
				boolean KWAR3=false;
				if(typer.equals("TRUE"))
				{
					columnName.add(ColumnName);
					columnType.add(TupleType);
					columnNameIndex.add(i-1);
				}

				for (String s : HashKeys) {
					
					
					if (!(ColumnName.equals(s)) && HashKeySize == HashKeys.length - 1) {
						throw new DBAppException(
								"please insert the column name right as the way you inserted it before when u created the Table");
						
					} else {
						if (!(ColumnName.equals(s))) {
							HashKeySize++;

						} else {if((ColumnName.equals(s)) &&  !((((((x.get(s)).getClass()).toString()).split("class"))[1]).replace(" ", "").equals(TupleType))) {
							HashKeySize++;}
						
						else {
							
							KWAR3=true;
							break;							
						}
						}

					}

				}
				if(!KWAR3) {throw new DBAppException(
						"please insert the column Type right as the way you inserted it before when u created the Table"); }
				array.add(x.get(ColumnName));
				

			}
			for(int looper = 0; looper<columnName.size();looper++)
			{
				if(!(columnType.get(looper).equals("class java.awt.Polygon")))
				{
					 directoryPath = "data//"+strTableName+"BTree"+columnName.get(looper)+".bin";
					 path = Paths.get(directoryPath);
					 isDir = Files.exists(path);
					

				}
				else
				{
					 directoryPath = "data//"+strTableName+"RTree"+columnName.get(looper)+".bin";
					 path = Paths.get(directoryPath);
					 isDir = Files.exists(path);
					 if(isDir)
					 {
						 // make a rtree and add inside it
					 }
				}				// check if here an Rtree
				
				
				}

			java.util.Date date = Calendar.getInstance().getTime();
			DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
			String now = dateFormat.format(date);
			array.add(now);
			
			// here we insert btree
			
			if(isBTreeIndexed(strTableName)) {
				ArrayList<String> abdob =getIndexedBTree(strTableName);
				for(String colNa:abdob) {
					int indexb=getcolumnNmuber(strTableName, colNa);
					Object value =x.get(colNa);
					ArrayList<Object> myattributes=new ArrayList<Object>();
					Set<Object> myhash=x.entrySet();
					
						BTree bts= new BTree();

						boolean flag=	bts.add(value,strTableName,colNa,array);
		
					
					
					
				}
				
				
				
			}
			
			
			
			
			
			
			
			
			
			
			// here we insert rtree
			
			if(isRTreeIndexed(strTableName)) {
				ArrayList<String> abdob =getIndexedRTree(strTableName);
				
				for(String colNa:abdob) {
					int indexb=getcolumnNmuber(strTableName, colNa);
					Object value =x.get(colNa);
					ArrayList<Object> myattributes=new ArrayList<Object>();
					Set<Object> myhash=x.keySet();
					
						RTree bts= new RTree();
						Polygon polyg=(Polygon) value;
						boolean flag=	bts.add(polyg,strTableName,colNa,array);
		
					
					
					
				}
				
				
				
			}
			(p.vector).add(array);
			
			p.Serialize("data//"+strTableName + "P0.bin", p);
			Table t = new Table();
			t.Tablename = strTableName;
			t.vector.add("P0");
			t.Serialize("data//"+strTableName + ".bin", t);
		//	System.out.println("successfully serialized The Table");

		} else {

			Table t = new Table();
			Table t1 = t.Deserialize("data//"+strTableName +".bin");
			Vector PagesNames = t1.vector;
			int clusterkeyindex = 0;

			Iterator<Object> i = PagesNames.iterator();

			Path pathtoTables = Paths.get("data//metadata.csv");
			String str1 = "";
			try {
				byte[] fileContents = Files.readAllBytes(pathtoTables);
				str1 = new String(fileContents, "UTF-8");

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String[] TableMetaRow = str1.split(strTableName+",");

			String[] HashKeys = ((((x.keySet().toString()).replace("[", "")).replaceAll("]", ""))).replaceAll(" ", "")
					.split(",");
			Page p = new Page();
			p.PageName = strTableName;
			ArrayList<Object> array = new ArrayList<Object>();
			ArrayList<String> columnName= new ArrayList<String>();
			ArrayList<Object> columnType = new ArrayList<Object>();
			ArrayList<Integer> columnNameIndex= new ArrayList<Integer>();
			for (int i1 = 1; i1 < TableMetaRow.length; i1++) {
				String FUll = (TableMetaRow[i1]).replaceAll(" ", "");
				String[] FullTable = FUll.split("\n");
		
				String[] TupleMeta = (FullTable[0].split(","));

				String ColumnName = (TupleMeta[0]);
				String ClusterKey = (TupleMeta[2]).replace(" ", "");
				String TupleType = (TupleMeta[1]).replace(" ", "");    

				if (ClusterKey.equals("TRUE")) {
					clusterkeyindex = i1 - 1;

				}
				int HashKeySize = 0;
				boolean KWAR3=false;
				for (String s : HashKeys) {
					
					
					
					
					
					if (!(ColumnName.equals(s)) && HashKeySize == HashKeys.length - 1) {
						throw new DBAppException(
								"please insert the column name right as the way you inserted it before when u created the Table");
						
					} else {
						if (!(ColumnName.equals(s))) {
							HashKeySize++;

						} else {if((ColumnName.equals(s)) &&  !((((((x.get(s)).getClass()).toString()).split("class"))[1]).replace(" ", "").equals(TupleType))) {
							HashKeySize++;}
						
						else {
							KWAR3=true;
							
							break;							
						}
						}

					}

				}
				
				if(!KWAR3) {throw new DBAppException(
						"please insert the column Type right as the way you inserted it before when u created the Table"); }
				array.add(x.get(ColumnName));

			}
			
			
			for(int looper = 0; looper<columnName.size();looper++)
			{
				if(!(columnType.get(looper).equals("class java.awt.Polygon")))
				{
					 directoryPath = "data//"+strTableName+"BTree"+columnName.get(looper)+".bin";
					 path = Paths.get(directoryPath);
					 isDir = Files.exists(path);
					 if(isDir)
					 {
						 BTree t01 = new BTree();
						 BTree q= t01.Deserialize("data//"+strTableName+"BTree"+columnName.get(looper)+".bin");
						 q.add(array.get(columnNameIndex.get(looper)), strTableName, columnName.get(looper),array);
					 }

				}
				else
				{
					 directoryPath = "data//"+strTableName+"RTree"+columnName.get(looper)+".bin";
					 path = Paths.get(directoryPath);
					 isDir = Files.exists(path);
					 if(isDir)
					 {
						 // make a rtree and add inside it
					 }
				}				// check if here an Rtree
				
				
				}

			java.util.Date date = Calendar.getInstance().getTime();
			DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
			String now = dateFormat.format(date);
			array.add(now);
			
			
			
			if(isBTreeIndexed(strTableName)) {
	//			System.out.println("kofta");
				ArrayList<String> abdob =getIndexedBTree(strTableName);
	//			System.out.println(abdob);
				for(String colNa:abdob) {
					int indexb=getcolumnNmuber(strTableName, colNa);
					Object value =x.get(colNa);
					ArrayList<Object> myattributes=new ArrayList<Object>();
					Set<Object> myhash=x.entrySet();
					
						BTree bts= new BTree();
					
						boolean flag=	bts.add(value,strTableName,colNa,array);
		
					
					
					
				}
				
				
				
			}
			
			
			
			
			
			
			
			
			
			
			
			
			if(isRTreeIndexed(strTableName)) {
				ArrayList<String> abdob =getIndexedRTree(strTableName);
				
				for(String colNa:abdob) {
					int indexb=getcolumnNmuber(strTableName, colNa);
					Object value =x.get(colNa);
					ArrayList<Object> myattributes=new ArrayList<Object>();
					Set<Object> myhash=x.keySet();
					for(Object o:myhash) {
						myattributes.add(o);
						RTree bts= new RTree();
						Polygon polyg=(Polygon) value;
						boolean flag=	bts.add(polyg,strTableName,colNa,array);
		
					}
					
					
				}
				
				
				
			}
			
			
			
			
			
			// putting the array in the location using binary search  and moveing on the next 
			//array to the next Page
			ArrayList<Object> MoveOnArray=  array;
		for(int j=0; j<(t1.vector).size();j++) 
		{
			
			String PAGENum = (String) t1.vector.get(j);
			Page pageretreive = p.Deserialize("data//"+strTableName+PAGENum+".bin");
			Page pageretreiveCopy = p.Deserialize("data//"+strTableName+PAGENum+".bin");

			if(pageretreive.vector.size()==200) 
			{
				
				if(!( array.get(clusterkeyindex).getClass().toString().equals("class java.awt.Polygon")))
				{
				createSorted(pageretreive.vector, MoveOnArray, clusterkeyindex);
				}
				else
				{
					createPolygonSorted(pageretreive.vector, MoveOnArray, clusterkeyindex);
				}
				pageretreive.vector.remove(200);
				pageretreive.Serialize("data//"+strTableName+"P"+j+".bin", pageretreive);
				MoveOnArray = (ArrayList<Object>) (pageretreive.vector.get(199));
				
			}
			else 
			{	if(!( array.get(clusterkeyindex).getClass().toString().equals("class java.awt.Polygon")))
				{
				if(j+1<t1.vector.size()) 
					{
					Comparable X = (Comparable) ((ArrayList<Object>) (pageretreive.vector).lastElement()).get(clusterkeyindex);
				
					Comparable Y = (Comparable) MoveOnArray.get(clusterkeyindex);
					
					if( X.compareTo(Y)<0) 
						{
					
					PAGENum = (String) t1.vector.get(j+1);
					
					pageretreive = p.Deserialize("data//"+strTableName+PAGENum+".bin");
					
					X = (Comparable) ((ArrayList<Object>) (pageretreive.vector).firstElement()).get(clusterkeyindex);
					
					if(X.compareTo(Y)>0)
						{

						createSorted(pageretreiveCopy.vector, MoveOnArray, clusterkeyindex);
						
						pageretreiveCopy.Serialize("data//"+strTableName+"P"+j+".bin", pageretreiveCopy);

						MoveOnArray=null;
						
						break;
						
						
						
					}
					
					
					}else 
						{
						
						
						createSorted(pageretreive.vector, MoveOnArray, clusterkeyindex);
						pageretreive.Serialize("data//"+strTableName+"P"+j+".bin", pageretreive);

						MoveOnArray=null;
						break;
						
						}
				
				
				
				}
				else
				{
				createSorted(pageretreive.vector, MoveOnArray, clusterkeyindex);
				pageretreive.Serialize("data//"+strTableName+"P"+j+".bin", pageretreive);

				MoveOnArray=null;
				break;
				}
			}
			else 
			{
				if(j+1<t1.vector.size()) 
				{
					CompareablePolygons X =  (CompareablePolygons) ((ArrayList<Object>) (pageretreive.vector).lastElement()).get(clusterkeyindex);
			
				Object Y =  MoveOnArray.get(clusterkeyindex);
				
				if( X.compareTo(Y)<0) 
					{
				
				PAGENum = (String) t1.vector.get(j+1);
				
				pageretreive = p.Deserialize("data//"+strTableName+PAGENum+".bin");
				
				X = (CompareablePolygons) ((ArrayList<Object>) (pageretreive.vector).firstElement()).get(clusterkeyindex);
				
				if(X.compareTo(Y)>0)
					{

					createPolygonSorted(pageretreiveCopy.vector, MoveOnArray, clusterkeyindex);
					
					pageretreiveCopy.Serialize("data//"+strTableName+"P"+j+".bin", pageretreiveCopy);

					MoveOnArray=null;
					
					break;
					
					
					
				}
				
				
				}else 
					{
					
					
					createPolygonSorted(pageretreive.vector, MoveOnArray, clusterkeyindex);
					pageretreive.Serialize("data//"+strTableName+"P"+j+".bin", pageretreive);

					MoveOnArray=null;
					break;
					
					}
			
			
			
			}
			else
			{
				createPolygonSorted(pageretreive.vector, MoveOnArray, clusterkeyindex);
			pageretreive.Serialize("data//"+strTableName+"P"+j+".bin", pageretreive);

			MoveOnArray=null;
			break;
			}
			}
			
			}
			
			
		}
			
			// if moving array is null then we have spaces in the other pages that we put succesfully the array in to if
		// if not then we have to create new page;
			
			
			if(MoveOnArray!=null)
			{
				
				Page NewPAGE= new Page();
				
				NewPAGE.PageName= strTableName;
				
				Vector NewVector = NewPAGE.vector;
				
				NewVector.add(MoveOnArray);
				NewPAGE.Serialize("data//"+strTableName+"P"+(t1.vector).size()+".bin", NewPAGE);

				
				t1.vector.add("P"+(t1.vector).size());
				
				t1.Serialize("data//"+strTableName+".bin",t1);
				
			}
			
			
			
			
			
			
			

		}

///////////////////////////////////////////

	}
	
	public static void createSorted(Vector b,ArrayList<Object> a, int n) 
	{ 
	  
	   
	        // if b is empty any element can be at 
	        // first place 
	        if (b.isEmpty()) 
	        	b.add(a); 
	        else { 
	  
	            //
	            int start = 0, end = b.size() - 1; 
	  
	            int pos = 0; 
	        /*    if(!((a.get(n)).getClass() .toString().equals("class java.lang.Integer")) ||
	            		!((a.get(n)).getClass() .toString().equals("class java.lang.Double")) ||
	            		!((a.get(n)).getClass() .toString().equals("class java.lang.Float")))
	            {*/
	            while (start <= end) { 
	  
	                int mid = start + (end - start) / 2; 
	                Comparable x = (Comparable) (((ArrayList<Object>) b.get(mid)).get(n));
	                Comparable y = (Comparable) a.get(n);
	  
	                if (x.compareTo(y)==0) { 
	                    b.add(mid+1, a); 
	                    break; 
	                } 
	                
	               
	                
	                else 
	                {	
	                	 
	                	
	                	if (x.compareTo(y)>0) 
	                		
	                    pos = end = mid - 1; 
	                else
	                	
	                    pos = start = mid + 1; 
	  
	                if (start > end) 
	                { 
	                	
	                    pos = start; 
	                    
	                    b.add(pos, a); 
	                  
	                    break; 
	                } 
	               }
	                }
	      
	            } 
	       
	    } 
	
	
	////////////////////my new method
	
	public static ArrayList<String> getIndexed(String tablename) throws DBAppException {
		ArrayList<String> res=new ArrayList<String>();
		//File file=new File("/Users/apple/eclipse-workspace/DBMS/src/metadata 2.csv");
		try {
			Scanner scanner = new Scanner(new File("data//metadata.csv"));
			while (scanner.hasNextLine()) {
				String[] x =scanner.nextLine().split(",");
				if(x[0].equals(tablename)&&x[4].equals("TRUE"))
					res.add(x[1]);		
				}
			
			
			scanner.close();
			
			
		} 
		catch (Exception e) {
			
			throw new DBAppException("File not found.");
			
			
		}
		return res;
	}

	/////////////////my new method
	
	public static boolean isIndexed(String tablename) throws DBAppException {
		//File file=new File("/Users/apple/eclipse-workspace/DBMS/src/metadata 2.csv");
		try {
			Scanner scanner = new Scanner(new File("data//metadata.csv"));
			while (scanner.hasNextLine()) {
				String[] x =scanner.nextLine().split(",");
				if(x[0].equals(tablename)&&x[4].equals("TRUE"))
					return true;		
				}
			
			
			scanner.close();
			
			
		} 
		catch (Exception e) {
			
			throw new DBAppException("File not found.");
			
			
		}
		return false;
	}

	/////////new method
	public static int getcolumnNmuber(String tablename,String colName) throws DBAppException {
		//File file=new File("/Users/apple/eclipse-workspace/DBMS/src/metadata 2.csv");
		int i=0;
		try {
			Scanner scanner = new Scanner(new File("data//metadata.csv"));
			while (scanner.hasNextLine()) {
				String[] x =scanner.nextLine().split(",");
				if(x[0].equals(tablename)&&x[1].equals(colName)) {
					return i;	}
				
				
				
			
			if(x[0].equals(tablename)&&!(x[1].equals(colName))) {
				
				i++;
			}
			}

			scanner.close();
			
			
		} 
		catch (Exception e) {
			
			throw new DBAppException("File not found.");
			
			
		}
		return -1;
	}

	
	public static void createPolygonSorted(Vector b,ArrayList<Object> a, int n) 
	{ 
	  
	   
	        // if b is empty any element can be at 
	        // first place 
	        if (b.isEmpty()) 
	        	b.add(a); 
	        else { 
	  
	            //
	            int start = 0, end = b.size() - 1; 
	  
	            int pos = 0; 
	        /*    if(!((a.get(n)).getClass() .toString().equals("class java.lang.Integer")) ||
	            		!((a.get(n)).getClass() .toString().equals("class java.lang.Double")) ||
	            		!((a.get(n)).getClass() .toString().equals("class java.lang.Float")))
	            {*/
	            while (start <= end) { 
	  
	                int mid = start + (end - start) / 2; 
	                Object p =  (Object) (((ArrayList<Object>) b.get(mid)).get(n));
	                CompareablePolygons x =  new CompareablePolygons(p);
	                Object y =  a.get(n);
	  
	                if (x.compareTo(y)==0) { 
	                    b.add(mid+1, a); 
	                    break; 
	                } 
	                
	               
	                
	                else 
	                {	
	                	 
	                	
	                	if (x.compareTo(y)>0) 
	                		
	                    pos = end = mid - 1; 
	                else
	                	
	                    pos = start = mid + 1; 
	  
	                if (start > end) 
	                { 
	                	
	                    pos = start; 
	                    
	                    b.add(pos, a); 
	                  
	                    break; 
	                } 
	               }
	                }
	      
	            } 
	       
	    } 
		public int max(int num1, int num2)
		{
		if(num1>num2) {return num1;}
		if(num2>num1) {return num2;}
		else return num1;
		}

		
		// new method
		public static ArrayList<String> getIndexedBTree(String tablename) throws DBAppException {
			ArrayList<String> res=new ArrayList<String>();
			//File file=new File("/Users/apple/eclipse-workspace/DBMS/src/metadata 2.csv");
			try {
				Scanner scanner = new Scanner(new File("data//metadata.csv"));
				while (scanner.hasNextLine()) {
					String[] x =scanner.nextLine().split(",");
	//				System.out.println(x[0]+" "+x[4]+" "+x[2]);

					if(x[0].equals(tablename)&&x[4].equals("TRUE")&&!(x[2].equals("java.awt.Polygon")))
						res.add(x[1]);		
					}
				
				
				scanner.close();
				
				
			} 
			catch (Exception e) {
				
				throw new DBAppException("File not found.");
				
				
			}
			return res;
		}

		// new method
		
		public static ArrayList<String> getIndexedRTree(String tablename) throws DBAppException {
			ArrayList<String> res=new ArrayList<String>();
			//File file=new File("/Users/apple/eclipse-workspace/DBMS/src/metadata 2.csv");
			try {
				Scanner scanner = new Scanner(new File("data//metadata.csv"));
				while (scanner.hasNextLine()) {
					String[] x =scanner.nextLine().split(",");
					if(x[0].equals(tablename)&&x[4].equals("TRUE")&&(x[2].equals("java.awt.Polygon")))
						res.add(x[1]);		
					}
				
				
				scanner.close();
				
				
			} 
			catch (Exception e) {
				
				throw new DBAppException("File not found.");
				
				
			}
			return res;
		}
		// new method 
		public static boolean isBTreeIndexed(String tablename) throws DBAppException {
			//File file=new File("/Users/apple/eclipse-workspace/DBMS/src/metadata 2.csv");
			try {
				Scanner scanner = new Scanner(new File("data//metadata.csv"));
				while (scanner.hasNextLine()) {
					String[] x =scanner.nextLine().split(",");
					if(x[0].equals(tablename)&&x[4].equals("TRUE")&&!(x[2].equals("java.awt.Polygon")))
						return true;		
					}
				
				
				scanner.close();
				
				
			} 
			catch (Exception e) {
				
				throw new DBAppException("File not found.");
				
				
			}
			return false;
		}
		//new method 
		public static boolean isRTreeIndexed(String tablename) throws DBAppException {
			//File file=new File("/Users/apple/eclipse-workspace/DBMS/src/metadata 2.csv");
			try {
				Scanner scanner = new Scanner(new File("data//metadata.csv"));
				while (scanner.hasNextLine()) {
					String[] x =scanner.nextLine().split(",");
					if(x[0].equals(tablename)&&x[4].equals("TRUE")&&(x[2].equals("java.awt.Polygon")))
						return true;		
					}
				
				
				scanner.close();
				
				
			} 
			catch (Exception e) {
				
				throw new DBAppException("File not found.");
				
				
			}
			return false;
		}

		
}

		