package LavaScript;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class Noder  implements Serializable{
	private static final long serialVersionUID = -1476815691062655563L;
	Noder parent=null;
	String NoderName= "";
    int maxChildSize;
    int maxkeysize;
	ArrayList<Object> keys;
	ArrayList<Pointer> p;
	ArrayList<String> ChildNodersNames;
	ArrayList<Integer> counter;
	
	public Noder ()
	{
		Properties prop=new Properties(); 
		try {
			FileInputStream ip= new FileInputStream("config//DBApp.properties");
			try {
				prop.load(ip);
				this.keys= new ArrayList<Object>();
				this.maxkeysize=Integer.parseInt(prop.getProperty("NoderSize"));
				this.p=  new ArrayList<Pointer>();

				this.maxChildSize=this.maxkeysize+1;
				this.ChildNodersNames=new ArrayList<String>();
				this.counter=new ArrayList<Integer>();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		
		
	}
	
	
	
	public Object  getKey(int index)
	{
		
		for(int i =0; i<keys.size();i++)
		{
			if(index == i)
			{
				return keys.get(i);
			}
		}
		return null;
		
		
	}
	
	public static  Noder Deserialize(String Path) {
		try {

			ObjectInputStream in = new ObjectInputStream(new FileInputStream(Path));
			Noder p = (Noder) in.readObject();
			in.close();
			return p;

		} catch (IOException i) {
			i.printStackTrace();		} 
		catch (ClassNotFoundException c) {
			c.printStackTrace();

		}
		return null;
	}
	
	public void addChild(Noder n) {
		
		this.ChildNodersNames.add(n.NoderName);
	}
public void addChild(Noder n,int index) {
		
		this.ChildNodersNames.add(index,n.NoderName);
		
		
	}
	
	
	
	public  void  Serialize(String Path) {
		try {

			ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(Path));
			out.writeObject(this);
			out.close();

		} catch (IOException i) {
			i.printStackTrace();

		} 
	}
	 public String  addKey(Object value) throws DBAppException {
		 
			return  addsorted(value);
		
     }
	 public String   addsorted(Object value) throws DBAppException 
	 {
		if(this.keys.size()==0)
		{
			this.keys.add(value);
			this.counter.add(1);
			return "";
		}
		else
		{
		for(int i=0;i<this.keys.size();i++)
		{
			Comparable c = (Comparable) keys.get(i);
			Comparable cvalue= (Comparable) value;
			if(c.compareTo(cvalue)==0 )
			{
				int counterincre = this.counter.get(i)+1;
				this.counter.add(i, counterincre);
				this.counter.remove(i+1);

				
				return ""+i+","+0+","+1;
			}
			if(c.compareTo(cvalue)>0)
			{
				keys.add(i,cvalue);
				
				this.counter.add(i,1);
				return ""+i+","+0+","+0;	
			}
			if(c.compareTo(cvalue)<0 && this.keys.size()>i+1)
			{
				continue;
			}
			if(c.compareTo(cvalue)<0)
			{
				keys.add(cvalue);
				this.counter.add(i+1,1);
				return ""+(i+1)+","+0+","+0;	
			}
		
		}

			
			
		}
		 throw new DBAppException("Error");

		 
	 }
	 
	 
	 public void addKeySpliter(Object Value)
	 {
		   addsortedSpliter(Value);
	 }
	 public void addsortedSpliter(Object value)
	 {
		 if(this.keys.size()==0)
			{
				this.keys.add(value);
				return ;
			}
			else
			{
			for(int i=0;i<this.keys.size();i++)
			{
				Comparable c = (Comparable) keys.get(i);
				Comparable cvalue= (Comparable) value;
				if(c.compareTo(cvalue)==0 || c.compareTo(cvalue)>0 )
				{
					keys.add(i,cvalue);
					return ;
				}
				
				if(c.compareTo(cvalue)<0 && this.keys.size()>i+1)
				{
					continue;
				}
				if(c.compareTo(cvalue)<0)
				{
					keys.add(cvalue);
					
					return ;	
				}
			
			}

				
				
			}
	 }
	 public int numberOfChildren()
	 {
		 return (this.ChildNodersNames.size());
		 
		 
	 }
	 

	 public int numberOfKeys() 
	 {
		 
		 return this.keys.size();
		 
	 }
	 
	 
	 public Noder getChild(int index)
	 
	 {
		 
		 if(this.ChildNodersNames.size()<=index)
		 	{
			 return null; 
		 	}
		 else
		 {
			 Noder n = Deserialize("data//"+this.ChildNodersNames.get(index)+".bin");
			 return n;
		 }
	 }
	 public int  removeChild(Noder n) throws DBAppException
	 { 
		 String nName=n.NoderName;
		 int index = this.getNoderNameindex(nName);
		 this.ChildNodersNames.remove(index);
		 File deletePage = new File("data//"+nName+".bin");
			
			deletePage.delete();
		 return index;
		 
	 }
	 public int  getNoderNameindex(String name) throws DBAppException 
	 {
		 for(int i =0;i<this.ChildNodersNames.size();i++)
		 {
			 if(this.ChildNodersNames.get(i).equals(name))
			 {
				 return i;
			 }
		 }
		 throw new DBAppException("There is no Noder with this Name");
	 }
	 
	 
	 public static String repeater(int multi,String alpha)
	 {String s ="";
		 for(int i=0;i<multi;i++)
		 {
			 s=s+alpha;
		 }
		 return s;
	 }
	 
	 
	 public String getChildNumber()
	 {
		int keysinside=this.ChildNodersNames.size();
		int alphapet=(keysinside%26)+1;
		int multiplier=(keysinside/26)+1;
		switch(alphapet)
		{
		case 1: return repeater(multiplier,"A");
		case 2: return repeater(multiplier,"B");
		case 3: return repeater(multiplier,"C");
		case 4: return repeater(multiplier,"D");
		case 5: return repeater(multiplier,"E");
		case 6: return repeater(multiplier,"F");
		case 7: return repeater(multiplier,"G");
		case 8: return repeater(multiplier,"H");
		case 9: return repeater(multiplier,"I");
		case 10: return repeater(multiplier,"J");
		case 11: return repeater(multiplier,"K");
		case 12: return repeater(multiplier,"L");
		case 13: return repeater(multiplier,"M");
		case 14: return repeater(multiplier,"N");
		case 15: return repeater(multiplier,"O");
		case 16: return repeater(multiplier,"P");
		case 17: return repeater(multiplier,"Q");
		case 18: return repeater(multiplier,"R");
		case 19: return repeater(multiplier,"S");
		case 20: return repeater(multiplier,"T");
		case 21: return repeater(multiplier,"U");
		case 22: return repeater(multiplier,"V");
		case 23: return repeater(multiplier,"W");
		case 24: return repeater(multiplier,"X");
		case 25: return repeater(multiplier,"Y");
		default : return  repeater(multiplier,"Z");

		
		
		
		 
		}
		 
		 
	 }
	 public String getChildNumber2(int index)
	 {
		 
		 
			int multiplier=(index/26)+1;
			switch(index+1)
			{
			case 1: return repeater(multiplier,"A");
			case 2: return repeater(multiplier,"B");
			case 3: return repeater(multiplier,"C");
			case 4: return repeater(multiplier,"D");
			case 5: return repeater(multiplier,"E");
			case 6: return repeater(multiplier,"F");
			case 7: return repeater(multiplier,"G");
			case 8: return repeater(multiplier,"H");
			case 9: return repeater(multiplier,"I");
			case 10: return repeater(multiplier,"J");
			case 11: return repeater(multiplier,"K");
			case 12: return repeater(multiplier,"L");
			case 13: return repeater(multiplier,"M");
			case 14: return repeater(multiplier,"N");
			case 15: return repeater(multiplier,"O");
			case 16: return repeater(multiplier,"P");
			case 17: return repeater(multiplier,"Q");
			case 18: return repeater(multiplier,"R");
			case 19: return repeater(multiplier,"S");
			case 20: return repeater(multiplier,"T");
			case 21: return repeater(multiplier,"U");
			case 22: return repeater(multiplier,"V");
			case 23: return repeater(multiplier,"W");
			case 24: return repeater(multiplier,"X");
			case 25: return repeater(multiplier,"Y");
			default : return  repeater(multiplier,"Z");

			
			
			
			
			}
		 
	 }
	 
	 
	public void changenameofotherchilds()
	{ 
		System.out.println(this.NoderName);
		if(this.ChildNodersNames.size()>=2)
		{
			String[] leveler=this.NoderName.split("Noder")[1].split("");

            
            int  level = Integer.parseInt(leveler[leveler.length-2])+1;
			for(int i =0; i<this.ChildNodersNames.size();i++)
			{
			String	alpha= this.getChildNumber2(i);
           	 Noder n = new Noder();
           	 String oldname= "data//"+this.ChildNodersNames.get(i)+".bin";
           	 Noder n1= n.Deserialize(oldname);
           	 String newName = this.NoderName+level+alpha;
           	 n1.NoderName=newName;
           	 n1.parent=this;

           	 this.ChildNodersNames.add(i,newName);
           	 this.ChildNodersNames.remove(i+1);
           	 File deletePage = new File(oldname);
    			 deletePage.delete();

    			 n1.changenameofotherchilds();

               	 n1.Serialize("data//"+n1.NoderName+".bin");


			}

		}
     	 this.Serialize("data//"+this.NoderName+".bin");

	}
	 
	 
	 
	 public static void main (String[] args) {
		 
	 }
	
	
	 public String  addKeyr(CompareablePolygons value) throws DBAppException {
		 
			return  addsortedr(value);
		
  }
	 public String   addsortedr(CompareablePolygons value) throws DBAppException 
	 {
		if(this.keys.size()==0)
		{
			this.keys.add(value);
			this.counter.add(1);
			return "";
		}
		else
		{
		for(int i=0;i<this.keys.size();i++)
		{
			CompareablePolygons c = (CompareablePolygons) keys.get(i);
			CompareablePolygons cvalue= (CompareablePolygons) value;
			if(c.compareTo(cvalue)==0 )
			{
				int counterincre = this.counter.get(i)+1;
				this.counter.add(i, counterincre);
				this.counter.remove(i+1);

				
				return ""+i+","+0+","+1;
			}
			if(c.compareTo(cvalue)>0)
			{
				keys.add(i,cvalue);
				
				this.counter.add(i,1);
				return ""+i+","+0+","+0;	
			}
			if(c.compareTo(cvalue)<0 && this.keys.size()>i+1)
			{
				continue;
			}
			if(c.compareTo(cvalue)<0)
			{
				keys.add(cvalue);
				this.counter.add(i+1,1);
				return ""+(i+1)+","+0+","+0;	
			}
		
		}

			
			
		}
		 throw new DBAppException("Error");

		 
	 }
	 
	 
	 public void addKeySpliterr(CompareablePolygons Value)
	 {
		   addsortedSpliterr(Value);
	 }
	 public void addsortedSpliterr(CompareablePolygons value)
	 {
		 if(this.keys.size()==0)
			{
				this.keys.add(value);
				return ;
			}
			else
			{
			for(int i=0;i<this.keys.size();i++)
			{
				CompareablePolygons c = (CompareablePolygons) keys.get(i);
				CompareablePolygons cvalue= (CompareablePolygons) value;
				if(c.compareTo(cvalue)==0 || c.compareTo(cvalue)>0 )
				{
					keys.add(i,cvalue);
					return ;
				}
				
				if(c.compareTo(cvalue)<0 && this.keys.size()>i+1)
				{
					continue;
				}
				if(c.compareTo(cvalue)<0)
				{
					keys.add(cvalue);
					
					return ;	
				}
			
			}

				
				
			}
	 }
	
	
}

