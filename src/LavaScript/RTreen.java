package LavaScript;

import java.awt.Polygon;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public class RTreen implements Serializable {
	private static final long serialVersionUID = 6141452897052782725L;
	RTreen t;


	int minKeySize = 0;
	int minChildrenSize = 0;
	int maxKeySize = 0;
	int maxChildrenSize = 0;

	Noder root = null;
 
	int size = 0;

	
	 
	
	public  ArrayList<Pointer> search (String tabN,String colN,String operator , String value )
	{
		
		
		
		
		return null ;
	}


	public RTreen()
	{



		Properties prop=new Properties();
		try {
			FileInputStream ip= new FileInputStream("config//DBApp.properties");
			try {
				prop.load(ip);
				this.maxKeySize=Integer.parseInt(prop.getProperty("NoderSize"));
				this.maxChildrenSize=maxKeySize+1;
				this.minKeySize=this.maxKeySize/2;
				this.minChildrenSize=this.minKeySize+1;

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}


	}




	public static  RTreen Deserialize(String Path) {
		try {

			ObjectInputStream in = new ObjectInputStream(new FileInputStream(Path));
			RTreen p = (RTreen) in.readObject();
			in.close();
			return p;

		} catch (IOException i) {
			i.printStackTrace();		}
		catch (ClassNotFoundException c) {
			c.printStackTrace();

		}
		return null;
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

	public boolean add(Polygon value,String TableName,String ColName,ArrayList<Object> array) {
		CompareablePolygons cp =new CompareablePolygons(value);
		try {
			return this.addr(cp, TableName, ColName, array);
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}


	public boolean addr(Comparable str,String TableName,String ColName,ArrayList<Object> array) throws DBAppException {
		CompareablePolygons value=(CompareablePolygons) str;
		t =Deserialize("data//"+TableName+"RTree"+ColName+".bin");
		Comparable value1 = (Comparable) value;

System.out.println();
		if (null==t.root) {
			t.root = new Noder();
			t.root.addKeyr(value);
			t.root.NoderName= TableName+ColName+"Node0";
			t.root.counter.add(1);
			Pointer p = new Pointer();

			p.v=new ArrayList<Object>();
			p.v.add(array);
			t.root.p.add(p);

			t.Serialize("data//"+TableName+"RTree"+ColName+".bin");

		} else {
			Noder node = t.root;
			while (node != null) {
				if (node.numberOfChildren() == 0) {
					int counterbefore = node.counter.get(0);
					String indcobefcoaft= node.addKeyr(value);
					int indexer = Integer.parseInt(indcobefcoaft.split(",")[0]);
					int countbef = Integer.parseInt(indcobefcoaft.split(",")[1]);
					int countafter = Integer.parseInt(indcobefcoaft.split(",")[2]);



					if (node.numberOfKeys() <= maxKeySize) {
						if(node!=t.root)
						{


							if(countbef==countafter)
							{
								Pointer p = new Pointer();
								p.v= new ArrayList<Object>();
								p.v.add(array);
								node.p.add(indexer,p);
							}
							else
							{
								node.p.get(indexer).v.add(array);

							}
							node.Serialize("data//"+node.NoderName+".bin");

						}
						else
						{
							if(countbef==countafter)
							{
								Pointer p = new Pointer();
								p.v= new ArrayList<Object>();
								p.v.add(array);
								node.p.add(indexer,p);
							}
							else
							{
								node.p.get(indexer).v.add(array);

							}

						}



						t.Serialize("data//"+TableName+"RTree"+ColName+".bin");


						break;
					}
					if(countbef==countafter)
					{
						Pointer p = new Pointer();
						p.v= new ArrayList<Object>();
						p.v.add(array);
						node.p.add(indexer,p);
					}
					else
					{
						node.p.get(indexer).v.add(array);

					}

					split(node);

					t.Serialize("data//"+TableName+"RTree"+ColName+".bin");

					break;
				}



				Comparable lesser = (Comparable) node.getKey(0);
				if (value1.compareTo(lesser) <= 0) {
					node = node.getChild(0);
					continue;
				}


				int numberOfKeys = node.numberOfKeys();
				int last = numberOfKeys - 1;
				Comparable greater = (Comparable) node.getKey(last);
				if (value1.compareTo(greater) > 0) {
					node = node.getChild(numberOfKeys);

					continue;
				}


				for (int i = 1; i < node.numberOfKeys(); i++) {
					Comparable prev = (Comparable) node.getKey(i - 1);
					Comparable next = (Comparable) node.getKey(i);
					if (value1.compareTo(prev) > 0 && value1.compareTo(next) <= 0) {
						node = node.getChild(i);
						break;
					}
				}

			}
		}

		size++;

		return true;
	}


	private void split(Noder NoderToSplit) throws DBAppException {


		Noder Noder = NoderToSplit;
		int numberOfKeys = Noder.numberOfKeys();


		int medianIndex = numberOfKeys / 2;
		Object medianValue = Noder.getKey(medianIndex);

		Noder left = new Noder();

		for (int i = 0; i < medianIndex; i++) {
			left.addKeySpliter(Noder.getKey(i));
			left.counter.add(Noder.counter.get(i));

			if(!Noder.p.isEmpty())
			{
				left.p.add(Noder.p.get(i));
			}
		}
		if (Noder.numberOfChildren() > 0) {
			for (int j = 0; j <= medianIndex; j++) {
				Noder c = Noder.getChild(j);

				left.addChild(c);
			}
		}

		Noder right = new Noder();
		if(Noder.ChildNodersNames.size()<2)
		{
			for (int i = medianIndex ; i < numberOfKeys; i++) {
				right.addKeySpliter(Noder.getKey(i));
				right.counter.add(Noder.counter.get(i));
				if(!Noder.p.isEmpty())
				{
					right.p.add(Noder.p.get(i));
				}
			}
		}
		else
		{
			for (int i = medianIndex+1 ; i < numberOfKeys; i++) {
				right.addKeySpliter(Noder.getKey(i));
				right.counter.add(Noder.counter.get(i));
				if(!Noder.p.isEmpty())
				{
					right.p.add(Noder.p.get(i));
				}
			}
		}




		if (Noder.numberOfChildren() > 0) {
			for (int j = medianIndex + 1; j < Noder.numberOfChildren(); j++) {
				Noder c = Noder.getChild(j);
				right.addChild(c);

			}
		}
		if (!Noder.p.isEmpty())
		{
			Noder.p.clear();
			Noder.counter.clear();
		}



		if (Noder.parent == null) {

			// new root, height of tree is increased
			Noder newRoot = new Noder();
			newRoot.addKey(medianValue);
			newRoot.NoderName=t.root.NoderName;
			Noder.parent = newRoot;
			t.root = newRoot;


			left.parent=t.root;
			right.parent=t.root;
			int level = Integer.parseInt((NoderToSplit.NoderName.split("Noder"))[1])+1;
			String alpha= t.root.getChildNumber();
			left.NoderName= t.root.NoderName+level+alpha;
			Noder faker = new Noder();
			faker.NoderName="";
			t.root.addChild(faker);
			alpha= t.root.getChildNumber();
			right.NoderName= t.root.NoderName+level+alpha;
			t.root.ChildNodersNames.remove(t.root.ChildNodersNames.size()-1);

			if(left.ChildNodersNames.size()!=0)
			{
				String[] leveler=left.NoderName.split("Noder")[1].split("");


				level =  level = Integer.parseInt(leveler[leveler.length-2])+1;
				for(int i =0; i<left.ChildNodersNames.size();i++)
				{ alpha= right.getChildNumber2(i);
					Noder n = new Noder();
					String oldname= "data//"+left.ChildNodersNames.get(i)+".bin";
					Noder n1= n.Deserialize(oldname);
					String newName = left.NoderName+level+alpha;
					n1.NoderName=newName;
					n1.parent=left;

					left.ChildNodersNames.add(i,newName);
					left.ChildNodersNames.remove(i+1);
					File deletePage = new File(oldname);
					deletePage.delete();
					n1.changenameofotherchilds();


				}

			}
			if(right.ChildNodersNames.size()!=0)
			{
				String[] leveler=right.NoderName.split("Noder")[1].split("");



				level =  level = Integer.parseInt(leveler[leveler.length-2])+1;
				for(int i =0; i<right.ChildNodersNames.size();i++)
				{	 alpha= right.getChildNumber2(i);
					Noder n = new Noder();
					String oldname= "data//"+right.ChildNodersNames.get(i)+".bin";
					Noder n1= n.Deserialize(oldname);
					String newName = right.NoderName+level+alpha;
					n1.NoderName=newName;
					n1.parent=right;
					right.ChildNodersNames.add(i,newName);
					right.ChildNodersNames.remove(i+1);
					File deletePage = new File(oldname);
					deletePage.delete();
					n1.changenameofotherchilds();


				}

			}

			t.root.addChild(left);
			t.root.addChild(right);

			right.Serialize("data//"+right.NoderName+".bin");
			left.Serialize("data//"+left.NoderName+".bin");
		} else {

			Noder parent = Noder.parent;

			boolean kofta= false;


			if(parent.NoderName.equals(t.root.NoderName))
			{
				parent=t.root;
				kofta=true;
			}
			else
			{
				parent=Noder.Deserialize("data//"+Noder.parent.NoderName+".bin");
			}
			parent.addKey(medianValue);

			String Noderls=Noder.NoderName;


			int index= parent.removeChild(Noder);
			left.parent=parent;
			left.NoderName=Noderls;
			String[] leveler=parent.NoderName.split("Noder")[1].split("");
			int level =0;
			if(kofta)
			{
				level = Integer.parseInt(leveler[leveler.length-1])+1;
			}
			else
			{
				level = Integer.parseInt(leveler[leveler.length-2])+1;
			}
			Noder faker = new Noder();
			faker.NoderName="";
			parent.addChild(faker);
			String alpha= parent.getChildNumber();
			right.parent=parent;
			right.NoderName=parent.NoderName+level+alpha;
			parent.ChildNodersNames.remove(parent.ChildNodersNames.size()-1);



			if(left.ChildNodersNames.size()!=0)
			{
				leveler=left.NoderName.split("Noder")[1].split("");


				level =  level = Integer.parseInt(leveler[leveler.length-2])+1;
				for(int i =0; i<left.ChildNodersNames.size();i++)
				{
					alpha= left.getChildNumber2(i);

					Noder n = new Noder();
					String oldname= "data//"+left.ChildNodersNames.get(i)+".bin";
					Noder n1= n.Deserialize(oldname);
					n1.parent=left;

					String newName = left.NoderName+level+alpha;
					n1.NoderName=newName;
					left.ChildNodersNames.add(i,newName);
					left.ChildNodersNames.remove(i+1);
					File deletePage = new File(oldname);
					deletePage.delete();
					n1.changenameofotherchilds();


				}

			}

			if(right.ChildNodersNames.size()!=0)
			{
				leveler=right.NoderName.split("Noder")[1].split("");


				level =  level = Integer.parseInt(leveler[leveler.length-2])+1;
				for(int i =0; i<right.ChildNodersNames.size();i++)
				{	 alpha= right.getChildNumber2(i);
					Noder n = new Noder();
					String oldname= "data//"+right.ChildNodersNames.get(i)+".bin";
					Noder n1= n.Deserialize(oldname);
					String newName = right.NoderName+level+alpha;
					n1.NoderName=newName;
					n1.parent=right;

					right.ChildNodersNames.add(i,newName);
					right.ChildNodersNames.remove(i+1);
					File deletePage = new File(oldname);
					deletePage.delete();

					n1.changenameofotherchilds();



				}

			}




			parent.addChild(left,index);

			parent.addChild(right,index+1);

			right.Serialize("data//"+right.NoderName+".bin");
			left.Serialize("data//"+left.NoderName+".bin");
			if(!kofta)
			{
				parent.Serialize("data//"+parent.NoderName+".bin");
			}
			else
			{
				t.root=parent;
			}

			if (parent.numberOfKeys() > maxKeySize)
			{
				split(parent);

			}
		}

	}


	public static String getEquivlant(int index) {
		String result="";
		while(index>0) {
			switch (index) {
			case 0: result+="A"; break;
			case 1: result+="B"; break;
			case 2: result+="C"; break;
			case 3: result+="D"; break;
			case 4: result+="E"; break;
			case 5: result+="F"; break;
			case 6: result+="G"; break;
			case 7: result+="H"; break;
			case 8: result+="I"; break;
			case 9: result+="J"; break;
			case 10: result+="K"; break;
			case 11: result+="L"; break;
			case 12: result+="M"; break;
			case 13: result+="N"; break;
			case 14: result+="O"; break;
			case 15: result+="P"; break;
			case 16: result+="Q"; break;
			case 17: result+="R"; break;
			case 18: result+="S"; break;
			case 19: result+="T"; break;
			case 20: result+="U"; break;
			case 21: result+="V"; break;
			case 22: result+="W"; break;
			case 23: result+="X"; break;
			case 24: result+="Y"; break;
			case 25: result+="Z"; break;
			}
			index-=26;
			
			}
		
		return result;
	}
	public static void removeKeyNonleaf(Noder del,String strTableName,CompareablePolygons value,int min,String col)  {
		RTreen tree = Deserialize("data//"+strTableName+"RTreen"+col+".bin");
		int index= del.keys.indexOf(value);
		if(index>=0) {

			del.keys.remove(value);
			//del.counter.remove(index);
			String leastgetter = del.ChildNodersNames.get(index+1);
			Noder least = Noder.Deserialize("data//"+leastgetter+".bin");
			Object key = findLeastLeaf(least);
			del.keys.add(index,key);
			if(least.ChildNodersNames.size()>0) {
				removeKeyNonleaf(least,strTableName, value,min,col);
				if(least.parent!=null)
					least.Serialize("data//"+leastgetter+".bin");
				else
				{
					RTreen temp = Deserialize("data//"+strTableName+"RTreen"+col+".bin");
					temp.root = del;
					temp.Serialize("data//"+strTableName+"RTreen"+col+".bin");
				}
				if(least.keys.size()<min);
				rearrange(least, tree, false, strTableName, col);
			}
		}
	}
	public static void renameTree(Noder Noder,String strTableName,String col,int level) {
		if(level ==0) {
		
		ArrayList<String> children = Noder.ChildNodersNames;
		for(int i=0;i<children.size();i++) {
			Noder NoderToChandge = Noder.Deserialize("data//"+children.get(i)+".bin");
			
			renameTree(NoderToChandge,strTableName, col, level+1 );
		}
			
		}
		else {
			File renameNoder = new File("data//"+Noder.NoderName +".bin");
			Noder parent=null;
			if(level>1) {
			RTreen temp = Deserialize("data//"+strTableName+"RTreen"+col+".bin");
			parent = temp.root;
			
			}
			else
				parent = Noder.Deserialize("data//"+Noder.parent.NoderName+".bin");
			int index = parent.ChildNodersNames.indexOf(Noder.NoderName);
			String a = getEquivlant(index);
			File newName = new File("data//"+Noder.parent.NoderName+level+a+".bin");
			renameNoder.renameTo(newName);
			parent.ChildNodersNames.remove(index);
			parent.ChildNodersNames.add(index,"data//"+Noder.parent.NoderName+level+a+".bin");
			if(level>1) {
				RTreen temp = Deserialize("data//"+strTableName+"RTreen"+col+".bin");
				temp.root = parent;
				temp.Serialize("data//"+strTableName+"RTreen"+col+".bin");
			}
			else
				parent.Serialize("data//"+Noder.parent.NoderName+".bin");
			ArrayList<String> children = Noder.ChildNodersNames;
			for(int i=0;i<children.size();i++) {
				Noder NoderToChandge = Noder.Deserialize("data//"+children.get(i)+".bin");
				
				renameTree(NoderToChandge,strTableName, col, level+1 );
			}

		}

	}
	public static void delete(String strTableName,CompareablePolygons value,ArrayList<Object> array,String col)  {
		//System.out.println(strTableName+"RTreen"+col+".bin");
		RTreen tree = Deserialize("data//"+strTableName+"RTreen"+col+".bin");
		//TODO: add when children less than min
		//TODO: finish handelling the root

		Noder root = tree.root;
		print(root);
//    	ArrayList<Object> keys= root.keys;
//    	Noder Noder= null;
//    	for(int i =0;i<keys.size();i++) {
//    		if(value.compareTo(keys.get(i))<0) {
//    			Noder=Noder.Deserialize("data//"+root.ChildNodersNames.get(i)+".bin");
//    		}
//    	}
		ArrayList<Noder> acc = new ArrayList<>();
		ArrayList<Noder> todelete = appear(root,value,array,acc);
		System.out.println(todelete);
//    	for(int i=0 ; i<todelete.size();i++) {
//        	System.out.println(todelete.get(i).keys);
//        	}
		for(int j = todelete.size()-1;j>=0;j--) {
			Noder delgetter = todelete.get(j);
			Noder del =null;
			if(delgetter.parent!=null)
				del=	Noder.Deserialize("data//"+delgetter.NoderName+".bin");
			else {
				RTreen temp = Deserialize("data//"+strTableName+"RTreen"+col+".bin") ;
				del = temp.root;
			}

			boolean leaf =del.ChildNodersNames.size()==0;
			//System.out.println(del.keys);
			int index = del.keys.indexOf(value);
			if(index>-1) {

				//System.out.println(del.keys.remove(value));



//					System.out.println(del.ChildNodersNames);
//					System.out.println(del.ChildNodersNames.size());
				if(leaf) {
					if(del.counter.get(index)==1) {
						del.p.remove(index);
						System.out.println(del.keys);
						del.keys.remove(value);
						del.counter.remove(index);
					}
					else {
						del.p.get(index).v.remove(array);
						del.counter.add(index,del.counter.get(index)-1 );
						del.counter.remove(index+1);
						del.Serialize("data//"+del.NoderName+".bin");
					}

				}
				else {
					removeKeyNonleaf(del, strTableName, value, tree.minKeySize, col);
				}
				System.out.println(del.keys);
				if(del.parent!=null)
					del.Serialize("data//"+del.NoderName+".bin");
				else {
					
					RTreen temp = Deserialize("data//"+strTableName+"RTreen"+col+".bin") ;
					if(del.keys.size()>0)
					temp.root=del;
					else
						temp.root=null;
					temp.Serialize("data//"+strTableName+"RTreen"+col+".bin");

				}
				//del.counter.add(index, 0);
				System.out.println(tree.minChildrenSize);
				//	System.out.println(del.parent.NoderName);
				if(tree.minKeySize >del.keys.size()||(!leaf &&del.ChildNodersNames.size()<tree.minChildrenSize)) {
					System.out.println("HI");
					System.out.println(del.NoderName);
					rearrange(del,tree,leaf, strTableName, col);

				}


			}

			else {
				if(del.parent!=null)
					del.Serialize("data//"+del.NoderName+".bin");
				else {
					RTreen temp = Deserialize("data//"+strTableName+"RTreen"+col+".bin") ;
					temp.root=del;
					temp.Serialize("data//"+strTableName+"RTreen"+col+".bin");
				}

				if(tree.minKeySize >del.keys.size()||(!leaf &&del.ChildNodersNames.size()<tree.minChildrenSize) ) {

					rearrange(del,tree,del.ChildNodersNames.size()==0, strTableName, col);

				}
			}
		}

	}
	public static void rootHandl(Noder root,String strTableName,String col) {
		System.out.println("Root handel");
		System.out.println(root.keys);
		if(root.keys.size()==0) {
			if(root.ChildNodersNames.size()==0) {
				return;
			}
			if(root.ChildNodersNames.size()==1) {
				//TODO: rename w n5er el level
				RTreen temp = Deserialize("data//"+strTableName+"RTreen"+col+".bin");
				Noder newRoot = Noder.Deserialize("data//"+root.ChildNodersNames.get(0)+".bin");
				File deleteNoder = new File("data//"+newRoot.NoderName+".bin");
				deleteNoder.delete();
				newRoot.parent=null;
				newRoot.NoderName=strTableName+col+"Noder0";
				temp.root=newRoot;
				System.out.println("root handl tany");
				System.out.println(newRoot.ChildNodersNames);
				temp.Serialize("data//"+strTableName+"RTreen"+col+".bin");
				renameTree(newRoot, strTableName, col, 0);
			}
			else {
				Noder rightNoder = Noder.Deserialize("data//"+root.ChildNodersNames.get(1)+".bin");
				Object newRootKey = findLeastLeaf(rightNoder);
				root.keys.add(newRootKey);
			}
		}
		else
		{
			return;
		}
	}
	public static Object findLeastLeaf(Noder Noder) {
		if(Noder.ChildNodersNames.size()>0) {
			Noder least = Noder.Deserialize("data//"+Noder.ChildNodersNames.get(0)+".bin");
			return findLeastLeaf(least);
		}
		else
			return Noder.keys.get(0);

	}
	public static Object findMostLeaf(Noder Noder) {
		if(Noder.ChildNodersNames.size()>0) {
			Noder most = Noder.Deserialize("data//"+Noder.ChildNodersNames.get(Noder.ChildNodersNames.size()-1)+".bin");
			return findMostLeaf(most);
		}
		else
			return Noder.keys.get(0);

	}
	public static void rearrange(Noder Noder,RTreen tree,boolean leaf,String strTableName,String col) {
		//TODO: handling root on the recursion step
		//TODO: case parent b key wa7ed w 2 pointers mohem t3mlha trace
		int max = tree.maxKeySize;
		int min =tree.minKeySize;
		int minChil= tree.minChildrenSize;
		Noder parentgeter = Noder.parent;
		if(parentgeter ==null) {

			rootHandl(Noder,strTableName,col);
			return;
		}
		Noder parent =null;
		if(parentgeter.NoderName.equals(strTableName+col+"Noder0")) {
			RTreen temp = Deserialize("data//"+strTableName+"RTreen"+col+".bin");
			parent = temp.root;
		}
		else {
			parent = Noder.Deserialize("data//"+parentgeter.NoderName+".bin");
		}

		
		int index =-1;
		for(int i=0;i<parent.ChildNodersNames.size();i++) {
			if(parent.ChildNodersNames.get(i).equals(Noder.NoderName))
			{
				index=i;
				break;
			}
		}
		int leftNoderIndex = index-1;
		boolean canMoveLeft= leftNoderIndex>=0;
		int rightNoderIndex = index +1;
		//check if the deleted Noder is a leaf Noder
		if(leaf) {

			if(canMoveLeft ) {
				Noder leftNoder = Noder.Deserialize("data//"+parent.ChildNodersNames.get(leftNoderIndex)+".bin");
				
				if(min <= leftNoder.keys.size()-1) {
					Object movedKey = leftNoder.keys.get(leftNoder.keys.size()-1);
					Noder.p.add(0,leftNoder.p.get(leftNoder.keys.size()-1));
					Noder.counter.add(0,leftNoder.counter.get(leftNoder.keys.size()-1));
					Noder.keys.add(0, movedKey);
					leftNoder.p.remove(leftNoder.keys.size()-1);
					leftNoder.counter.remove(leftNoder.keys.size()-1);
					leftNoder.keys.remove(leftNoder.keys.size()-1);
					parent.keys.remove(leftNoderIndex);
					parent.keys.add(leftNoderIndex, movedKey);
					parent.Serialize("data//"+parent.NoderName+".bin");
					Noder.Serialize("data//"+Noder.NoderName+".bin");
					leftNoder.Serialize("data//"+leftNoder.NoderName+".bin");
					if(parent.keys.size()<min||parent.ChildNodersNames.size()<minChil)
						rearrange(parent, tree,false, strTableName, col);
					return;
				}
				if(max>=leftNoder.keys.size()+Noder.keys.size() && leftNoder.keys.size()<max) {
					leftNoder.keys.addAll(Noder.keys);
					leftNoder.counter.addAll(Noder.counter);
					leftNoder.p.addAll(Noder.p);
					leftNoder.Serialize("data//"+leftNoder.NoderName+".bin");
					File deleteNoder = new File("data//"+Noder.NoderName+".bin");
					deleteNoder.delete();
					parent.keys.remove(leftNoderIndex);
					parent.counter.remove(leftNoderIndex);
					parent.ChildNodersNames.remove(Noder.NoderName);
					parent.Serialize("data//"+parent.NoderName+".bin");
					parent = Noder.Deserialize("data//"+parent.NoderName+".bin");
					if(parent.keys.size()<min || parent.ChildNodersNames.size()<minChil)
						rearrange(parent, tree,false, strTableName, col);
					return;
				}
				//this if condition can be replaced with else as it's never the case that the 2 conditions would be false
			

			}
			else {
				Noder rightNoder = Noder.Deserialize("data//"+parent.ChildNodersNames.get(rightNoderIndex)+".bin");
				if(min <= rightNoder.keys.size()-1) {
					Object movedKey = rightNoder.keys.get(0);
					Noder.keys.add(movedKey);
					Noder.p.add(rightNoder.p.get(0));
					Noder.counter.add(rightNoder.counter.get(0));
					rightNoder.keys.remove(0);
					rightNoder.p.remove(0);
					rightNoder.counter.remove(0);
					parent.keys.remove(index);
					parent.keys.add(index, findLeastLeaf(Noder));
					parent.Serialize("data//"+parent.NoderName+".bin");
					Noder.Serialize("data//"+Noder.NoderName+".bin");
					rightNoder.Serialize("data//"+rightNoder.NoderName+".bin");
					if(parent.keys.size()<min || parent.ChildNodersNames.size()<minChil)
						rearrange(parent, tree,false, strTableName, col);
					return;
				}

				if(Noder.keys.size()+rightNoder.keys.size()<= max) {
					rightNoder.keys.addAll(0, Noder.keys);
					rightNoder.p.addAll(0,Noder.p);
					rightNoder.counter.addAll(0,Noder.counter);
					rightNoder.Serialize("data//"+rightNoder.NoderName+".bin");
					File deleteNoder = new File("data//"+Noder.NoderName+".bin");
					deleteNoder.delete();
					parent.keys.remove(index);
					parent.counter.remove(index);
					parent.ChildNodersNames.remove(Noder.NoderName);
					parent.Serialize("data//"+parent.NoderName+".bin");
					if(parent.keys.size()<min || parent.ChildNodersNames.size()<minChil)
						rearrange(parent, tree,false, strTableName, col);
					return;
				}
				

			}

		}
		//if the deleted Noder is not a leaf Noder
		else {

			//Noder rightNoder = Noder.Deserialize("data//"+parent.ChildNodersNames.get(rightNoderIndex)+".bin");
			//int maxChil = tree.maxChildrenSize;

//			if(Noder.keys.size()<min) {
//				ArrayList<String> movedChildren = Noder.ChildNodersNames;
//				
//				if(leftNoderIndex>=0) {
//					Noder leftNoder = Noder.Deserialize("data//"+parent.ChildNodersNames.get(leftNoderIndex)+".bin");
//					if(min <= leftNoder.keys.size()-1) {
//						String childToMove =leftNoder.ChildNodersNames.get(leftNoder.ChildNodersNames.size()-1);
//						leftNoder.ChildNodersNames.remove(leftNoder.ChildNodersNames.size()-1);
//						Object keyToMove = leftNoder.keys.get(leftNoder.keys.size()-1);
//						leftNoder.keys.remove(leftNoder.keys.size()-1);
//						Noder.keys.add(0,keyToMove);
//						Noder.ChildNodersNames.add(0, childToMove);
//						Object leastNewLeaf = findLeastLeaf(Noder);
//						parent.keys.remove(leftNoderIndex);
//						parent.keys.add(leftNoderIndex, leastNewLeaf);
//						leftNoder.Serialize("data//"+leftNoder.NoderName+".bin");
//						parent.Serialize("data//"+parent.NoderName+".bin");
//						Noder.Serialize("data//"+Noder.NoderName+".bin");
//						return;
//					}
//					if(leftNoder.numberOfChildren()+movedChildren.size()<=leftNoder.maxChildSize) {
//						leftNoder.ChildNodersNames.addAll(movedChildren);
//						Noder moved = Noder.Deserialize("data//"+movedChildren.get(movedChildren.size()-1)+".bin");
//						leftNoder.keys.add(parent.keys.get(index));
//						leftNoder.keys.addAll(Noder.keys);
//						parent.ChildNodersNames.remove(index);
//						parent.keys.remove(index);
//						File deleteNoder = new File("data//"+Noder.NoderName+".bin");
//						deleteNoder.delete();
//						leftNoder.Serialize("data//"+leftNoder.NoderName+".bin");
//						parent.Serialize("data//"+parent.NoderName+".bin");
//						if(parent.keys.size()<min || parent.ChildNodersNames.size()<minChil)
//							rearrange(parent, tree,false, strTableName, col);
//						return;
//					}
//					
//				}
//				//joining with the right
//				Noder rightNoder = Noder.Deserialize("data//"+parent.ChildNodersNames.get(rightNoderIndex)+".bin");
//				if(min <= rightNoder.keys.size()-1) {
//					String childToMove = rightNoder.ChildNodersNames.get(0);
//					rightNoder.ChildNodersNames.remove(0);
//					Object keyToMove = rightNoder.keys.get(0);
//					Noder.keys.add(parent.keys.get(index));
//					Noder.ChildNodersNames.add(childToMove);
//					parent.keys.remove(index);
//					parent.keys.add(index, keyToMove);
//					rightNoder.Serialize("data//"+rightNoder.NoderName+".bin");
//					parent.Serialize("data//"+parent.NoderName+".bin");
//					Noder.Serialize("data//"+Noder.NoderName+".bin");
//					return;
//				}
//				if(rightNoder.numberOfChildren()+movedChildren.size()<=rightNoder.maxChildSize) {
//					System.out.println("hnaa");
//					rightNoder.ChildNodersNames.addAll(0,movedChildren);
//					//	Noder moved = Noder.Deserialize("data//"+movedChildren.get(0)+".bin");
//					rightNoder.keys.add(0,parent.keys.get(index));
//					rightNoder.keys.addAll(0,Noder.keys);
//					parent.ChildNodersNames.remove(index);
//					parent.keys.remove(index);
//					File deleteNoder = new File("data//"+Noder.NoderName+".bin");
//					deleteNoder.delete();
//					rightNoder.Serialize("data//"+rightNoder.NoderName+".bin");
//					parent.Serialize("data//"+parent.NoderName+".bin");
//					if(parent.keys.size()<min || parent.ChildNodersNames.size()<minChil)
//						rearrange(parent, tree,false, strTableName, col);
//					return;
//				}
//				
//
//			}
			if(Noder.keys.size()<min || Noder.ChildNodersNames.size()<minChil ) {

				if(leftNoderIndex>=0) {
					Noder leftNoder = Noder.Deserialize("data//"+parent.ChildNodersNames.get(leftNoderIndex));
					// borrow from left
					if(leftNoder.ChildNodersNames.size()>minChil && leftNoder.keys.size()>min) {
						Noder.ChildNodersNames.add(0,leftNoder.ChildNodersNames.get(leftNoder.ChildNodersNames.size()-1));
						Noder.keys.add(0,leftNoder.keys.get(leftNoder.keys.size()-1));
						leftNoder.ChildNodersNames.remove(leftNoder.ChildNodersNames.size()-1);
						leftNoder.keys.remove(leftNoder.keys.size()-1);
						Object newLeastLeaf = findLeastLeaf(Noder);
						parent.keys.remove(leftNoderIndex);
						parent.keys.add(leftNoderIndex, newLeastLeaf);
						Noder.Serialize("data//"+Noder.NoderName+".bin");
						leftNoder.Serialize("data//"+leftNoder.NoderName+".bin");
						parent.Serialize("data//"+parent.NoderName+".bin");

						return;
					}
					// merge with left
					else {
						leftNoder.ChildNodersNames.addAll(Noder.ChildNodersNames);
						leftNoder.keys.addAll(Noder.keys);
						parent.ChildNodersNames.remove(index);
						parent.keys.remove(leftNoderIndex);
						if(leftNoder.ChildNodersNames.size()>leftNoder.keys.size()+1) {
							Object key = findMostLeaf(leftNoder);
							leftNoder.keys.add(key);
							}
						File deleteNoder = new File("data//"+Noder.NoderName+".bin");
						deleteNoder.delete();
						parent.Serialize("data//"+parent.NoderName+".bin");
						leftNoder.Serialize("data//"+leftNoder.NoderName+".bin");
						if(parent.keys.size()<min || parent.ChildNodersNames.size()<minChil)
							rearrange(parent, tree, false, strTableName, col);
						return;

					}
				}
				else {
					Noder rightNoder = Noder.Deserialize("data//"+parent.ChildNodersNames.get(rightNoderIndex)+".bin");
					// borrow from right
					if(rightNoder.ChildNodersNames.size()>minChil && rightNoder.keys.size()>min) {
						Noder.ChildNodersNames.add(rightNoder.ChildNodersNames.get(0));
						Noder.keys.add(rightNoder.keys.get(0));
						rightNoder.ChildNodersNames.remove(0);
						rightNoder.keys.remove(0);
						parent.keys.remove(index);
						Object leastNewLeaf = findLeastLeaf(rightNoder);
						parent.keys.add(index,leastNewLeaf);
						Noder.Serialize("data//"+Noder.NoderName+".bin");
						rightNoder.Serialize("data//"+rightNoder.NoderName+".bin");
						parent.Serialize("data//"+parent.NoderName+".bin");
						return;
					}
					// merge with right
					else {
						Noder.ChildNodersNames.addAll(rightNoder.ChildNodersNames);
						Noder.keys.addAll(rightNoder.keys);
						parent.ChildNodersNames.remove(rightNoderIndex);
						parent.keys.remove(index);
						if(Noder.ChildNodersNames.size()>Noder.keys.size()+1) {
							Object key = findLeastLeaf(Noder.Deserialize("data//"+Noder.ChildNodersNames.get(1)+".bin"));
						}
						File deleteNoder = new File("data//"+rightNoder.NoderName+".bin");
						deleteNoder.delete();
						parent.Serialize("data//"+parent.NoderName+".bin");
						Noder.Serialize("data//"+Noder.NoderName+".bin");
						if(parent.keys.size()<min || parent.ChildNodersNames.size()<minChil)
							rearrange(parent, tree, false, strTableName, col);
						return;

					}
				}


			}



		}
	}
	public static ArrayList<Noder> appear(Noder Noder,CompareablePolygons value, ArrayList<Object> array,ArrayList<Noder>acc){
		//ArrayList<Noder> c = new ArrayList<>();
		ArrayList<Object> keys= Noder.keys;
		//Noder Noder= null;
		
		for(int i =0;i<keys.size();i++) {
			if(value.compareTo(keys.get(i))<0 && Noder.ChildNodersNames.size()>0) {
				Noder=Noder.Deserialize("data//"+Noder.ChildNodersNames.get(i)+".bin");
				appear(Noder,value,array,acc);
			}
			if(value.compareTo(keys.get(i))>0 && Noder.ChildNodersNames.size()>0&&i==keys.size()-1) {
				Noder=Noder.Deserialize("data//"+Noder.ChildNodersNames.get(i+1)+".bin");
				appear(Noder,value,array,acc);
			}
			if(value.compareTo(keys.get(i))==0&&Noder.ChildNodersNames.size()==0) {
				//	System.out.println(Noder.p);
				for(int j = 0; j <Noder.p.get(i).v.size(); j++) {
					
					
					if(Noder.p.get(i).v.get(j).equals(array))
						acc.add(Noder);
				}
				return acc;

			}
			if(Noder.ChildNodersNames.size()>0 && value.compareTo(keys.get(i))==0) {
				acc.add(Noder);
				Noder=Noder.Deserialize("data//"+Noder.ChildNodersNames.get(i+1)+".bin");
				appear(Noder,value,array,acc);
			}
		}
		//array.addAll(c) ;
		return acc;
	}
	public static void main (String[] args) throws DBAppException, InterruptedException
	{
//	File deleteNoder = new File("data//ahmedgpaNoder01B2A.bin");
//	deleteNoder.delete();
//	Noder test = Noder.Deserialize("data//ahmedgpaNoder01B2A.bin");
//
//	deleteNoder.delete();

//	Noder Noder9 = Noder.Deserialize("data//ahmedgpaNoder01A2B.bin");
//	System.out.println(Noder9.keys);
//	Noder Noder9parent =Noder9.parent;
//	System.out.println(Noder9parent.keys);
//	System.out.println(Noder9parent.ChildNodersNames);
//	int index = Noder9parent.ChildNodersNames.indexOf(Noder9.NoderName);
//	//Noder9parent.ChildNodersNames.remove(index);
//	System.out.println(Noder9parent.ChildNodersNames);
//
		RTreen t12 = new RTreen();
		
	
		
			ArrayList<Object> a= new ArrayList<Object>();
			a.add(5);
			a.add("kimo");
			a.add("55");
			a.add("2020-21-19 07:21:23");
			
			
	//		t12.delete("ahmed", 5, a, "gpa");
			
		
		
		
		
		t12=Deserialize("data//"+"ahmed"+"RTreen"+"gpa"+".bin");
	
	//	t12.delete("ahmed", 18, c, "gpa");
			Noder n = new Noder();
			
			
		
			System.out.println(t12.root.p.get(0).v);
		
//		print(t12.root);
//		ArrayList<Object> a= new ArrayList<Object>();
//		a.add(1);
//		a.add(2);
//		a.add("kdada"+20);
//		Integer n =20;
//		delete("ahmed",n,a , "gpa");
//		t12=Deserialize("data//"+"ahmed"+"RTreen"+"gpa"+".bin");
//		print(t12.root);
//	Noder hg = Noder.Deserialize("data//ahmedgpaNoder01B.bin");
//	System.out.println(hg.ChildNodersNames);
//	hg.ChildNodersNames.remove(0);
//	//System.out.println(hg.NoderName);
//	hg.Serialize("data//ahmedgpaNoder01B.bin");
//	hg = Noder.Deserialize("data//ahmedgpaNoder01B.bin");
//	System.out.println(hg.ChildNodersNames);
//	Noder Noder = t12.root;
//	print(Noder);
//		System.out.println(Noder.keys);
//		System.out.println(Noder.ChildNodersNames);
//		for(int i=0;i<Noder.numberOfChildren();i++) {
//		Noder print = Noder.Deserialize("data//"+Noder.ChildNodersNames.get(i)+".bin")	;
//		if(print.numberOfChildren()>0) {
//			System.out.println("still more");
//		}
//		System.out.println(print.keys);
//
//	}







	}





	public static void print(Noder Noder) {
		try {
			if(Noder.numberOfChildren()>0)
				//	System.out.println(Noder + "   "+Noder.keys);

				for(int i=0;i<Noder.numberOfChildren();i++) {
					Noder hi = Noder.Deserialize("data//"+Noder.ChildNodersNames.get(i)+".bin")	;
					print(hi);
				}



		}

		catch (Exception e) {
			System.out.println("hh");
		}
	}


//	RTreen t = new RTreen();
//	RTreen q1 = t.Deserialize("data//ahmedRTreengpa.bin");
//	System.out.println(q1.root.keys);
//	System.out.println(q1.root.counter);
//	System.out.println(q1.root.NoderName);
//	System.out.println(q1.root.ChildNodersNames.size());
//
//	for(int i =0;i<q1.root.ChildNodersNames.size();i++)
//	{
//		Noder q =new Noder();
//
//		Noder t1=q.Deserialize("data//"+q1.root.ChildNodersNames.get(i)+".bin");
//		System.out.println("Name:"+t1.NoderName);
//		System.out.println("Parent Name:"+t1.parent.NoderName);
//		System.out.println("Keys:"+t1.keys);
//		System.out.println("count of values inside:"+t1.counter);
//		System.out.println("Child Noders names:"+t1.ChildNodersNames);
//		for(int j =0; j<t1.ChildNodersNames.size();j++)
//		{
//			 Noder t2=q.Deserialize("data//"+t1.ChildNodersNames.get(j)+".bin");
//			System.out.println("CHILDName:"+t2.NoderName);
//			System.out.println("CHILDParent Name:"+t2.parent.NoderName);
//			System.out.println("CHILDKeys:"+t2.keys);
//			System.out.println("CHILDcount of values inside:"+t2.counter);
//			System.out.println("CHILD Child Noders names:"+t2.ChildNodersNames);
//		}
//
//		System.out.println("");
//	}
//
	
	public static ArrayList<Object> eappear(Noder Noder,CompareablePolygons value,ArrayList<Object>acc){
		//ArrayList<Noder> c = new ArrayList<>();
		ArrayList<Object> keys= Noder.keys;
		//Noder Noder= null;
		
		for(int i =0;i<keys.size();i++) {
			if(value.compareTo(keys.get(i))<0 && Noder.ChildNodersNames.size()>0) {
				Noder=Noder.Deserialize("data//"+Noder.ChildNodersNames.get(i)+".bin");
				eappear(Noder,value,acc);
			}
			if(value.compareTo(keys.get(i))>0 && Noder.ChildNodersNames.size()>0&&i==keys.size()-1) {
				Noder=Noder.Deserialize("data//"+Noder.ChildNodersNames.get(i+1)+".bin");
				eappear(Noder,value,acc);
			}
			if(value.compareTo(keys.get(i))==0&&Noder.ChildNodersNames.size()==0) {
				//	System.out.println(Noder.p);
				for(int j = 0; j <Noder.p.get(i).v.size(); j++) {
					
					acc.add(Noder.p.get(i).v);
					
				}
				return acc;

			}
			if(Noder.ChildNodersNames.size()>0 && value.compareTo(keys.get(i))==0) {
				
				Noder=Noder.Deserialize("data//"+Noder.ChildNodersNames.get(i+1)+".bin");
				eappear(Noder,value,acc);
			}
		}
		//array.addAll(c) ;
		return acc;
	}
//
	public static ArrayList<Object> gappear(Noder Noder,CompareablePolygons value,ArrayList<Object>acc){
		//ArrayList<Noder> c = new ArrayList<>();
		ArrayList<Object> keys= Noder.keys;
		//Noder Noder= null;
		
		for(int i =0;i<keys.size();i++) {
			if(value.compareTo(keys.get(i))<0 && Noder.ChildNodersNames.size()>0) {
				if(i==0 || value.compareTo(keys.get(i-1))>0 )
				{
					Noder y=Noder.Deserialize("data//"+Noder.ChildNodersNames.get(i)+".bin");
					gappear(y,value,acc);
				}
				
				Noder x = Noder.Deserialize("data//"+Noder.ChildNodersNames.get(i+1)+".bin");
				gappear(x,value,acc);
			}
			if(value.compareTo(keys.get(i))>0 && Noder.ChildNodersNames.size()>0&&i==keys.size()-1) {
				Noder=Noder.Deserialize("data//"+Noder.ChildNodersNames.get(i+1)+".bin");
				gappear(Noder,value,acc);
			}
			if(value.compareTo(keys.get(i))<0&&Noder.ChildNodersNames.size()==0) {
				for(int j = 0; j <Noder.p.get(i).v.size(); j++) {
					
					acc.add(Noder.p.get(i).v);
					
				}
				

			}
			if(Noder.ChildNodersNames.size()>0 && value.compareTo(keys.get(i))==0) {
				
				Noder=Noder.Deserialize("data//"+Noder.ChildNodersNames.get(i+1)+".bin");
				gappear(Noder,value,acc);
			}
		}
		//array.addAll(c) ;
		return acc;
	}

	public static ArrayList<Object> sappear(Noder Noder,CompareablePolygons value,ArrayList<Object>acc){
		//ArrayList<Noder> c = new ArrayList<>();
		ArrayList<Object> keys= Noder.keys;
		//Noder Noder= null;
		
		for(int i =0;i<keys.size();i++) {
			if(value.compareTo(keys.get(i))<0 && Noder.ChildNodersNames.size()>0) {
				if(i == 0 )
				{
					Noder y = Noder.Deserialize("data//"+Noder.ChildNodersNames.get(i)+".bin");
					sappear(y,value,acc);
				}
			}
			if(value.compareTo(keys.get(i))>0 && Noder.ChildNodersNames.size()>0) {
				if(i == 0 )
				{
					Noder y = Noder.Deserialize("data//"+Noder.ChildNodersNames.get(i)+".bin");
					sappear(y,value,acc);
				}
				Noder x = Noder.Deserialize("data//"+Noder.ChildNodersNames.get(i+1)+".bin");
				sappear(x,value,acc);
			}
			if(value.compareTo(keys.get(i))>0&&Noder.ChildNodersNames.size()==0) {
				for(int j = 0; j <Noder.p.get(i).v.size(); j++) {
					
					acc.add(Noder.p.get(i).v);
					
				}
				

			}
			if(Noder.ChildNodersNames.size()<0 && value.compareTo(keys.get(i))==0) {
				
				Noder=Noder.Deserialize("data//"+Noder.ChildNodersNames.get(i+1)+".bin");
				sappear(Noder,value,acc);
			}
		}
		//array.addAll(c) ;
		return acc;
	}
}
