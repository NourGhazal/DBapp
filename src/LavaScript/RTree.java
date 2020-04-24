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

public class RTree implements Serializable {
	private static final long serialVersionUID = 6141452897052782725L;
	RTree t;


	int minKeySize = 0;
	int minChildrenSize = 0;
	int maxKeySize = 0;
	int maxChildrenSize = 0;

	Node root = null;
 
	int size = 0;

	
	
	
	public  ArrayList<Pointer> search (String tabN,String colN,String operator , String value )
	{
		
		
		
		
		return null ;
	}


	public RTree()
	{



		Properties prop=new Properties();
		try {
			FileInputStream ip= new FileInputStream("config//DBApp.properties");
			try {
				prop.load(ip);
				this.maxKeySize=Integer.parseInt(prop.getProperty("NodeSize"));
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




	public static  RTree Deserialize(String Path) {
		try {

			ObjectInputStream in = new ObjectInputStream(new FileInputStream(Path));
			RTree p = (RTree) in.readObject();
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


	public boolean add(Polygon value,String TableName,String ColName,ArrayList<Object> array) throws DBAppException {
		CompareablePolygons cp =new CompareablePolygons(value);
		return this.addRTree(cp, TableName, ColName, array);
	}

	public boolean addRTree(CompareablePolygons str,String TableName,String ColName,ArrayList<Object> array) throws DBAppException {
		CompareablePolygons value=(CompareablePolygons) str;
		t =Deserialize("data//"+TableName+"RTree"+ColName+".bin");
		CompareablePolygons value1 = (CompareablePolygons) value;

//System.out.println();
		if (null==t.root) {
			t.root = new Node();
			t.root.addKeyr(value);
			t.root.NodeName= TableName+ColName+"Node0";
			t.root.counter.add(1);
			Pointer p = new Pointer();

			p.v=new ArrayList<Object>();
			p.v.add(array);
			t.root.p.add(p);

			t.Serialize("data//"+TableName+"RTree"+ColName+".bin");

		} else {
			Node node = t.root;
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
							node.Serialize("data//"+node.NodeName+".bin");

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



				CompareablePolygons lesser = (CompareablePolygons) node.getKey(0);
				if (value1.compareTo(lesser) <= 0) {
					node = node.getChild(0);
					continue;
				}


				int numberOfKeys = node.numberOfKeys();
				int last = numberOfKeys - 1;
				CompareablePolygons greater = (CompareablePolygons) node.getKey(last);
				if (value1.compareTo(greater) > 0) {
					node = node.getChild(numberOfKeys);

					continue;
				}


				for (int i = 1; i < node.numberOfKeys(); i++) {
					CompareablePolygons prev = (CompareablePolygons) node.getKey(i - 1);
					CompareablePolygons next = (CompareablePolygons) node.getKey(i);
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


	private void split(Node nodeToSplit) throws DBAppException {


		Node node = nodeToSplit;
		int numberOfKeys = node.numberOfKeys();


		int medianIndex = numberOfKeys / 2;
		CompareablePolygons medianValue = (CompareablePolygons) node.getKey(medianIndex);

		Node left = new Node();

		for (int i = 0; i < medianIndex; i++) {
			left.addKeySpliterr((CompareablePolygons)node.getKey(i));
			left.counter.add(node.counter.get(i));

			if(!node.p.isEmpty())
			{
				left.p.add(node.p.get(i));
			}
		}
		if (node.numberOfChildren() > 0) {
			for (int j = 0; j <= medianIndex; j++) {
				Node c = node.getChild(j);

				left.addChild(c);
			}
		}

		Node right = new Node();
		if(node.ChildNodesNames.size()<2)
		{
			for (int i = medianIndex ; i < numberOfKeys; i++) {
				right.addKeySpliterr((CompareablePolygons)node.getKey(i));
				right.counter.add(node.counter.get(i));
				if(!node.p.isEmpty())
				{
					right.p.add(node.p.get(i));
				}
			}
		}
		else
		{
			for (int i = medianIndex+1 ; i < numberOfKeys; i++) {
				right.addKeySpliterr((CompareablePolygons)node.getKey(i));
				right.counter.add(node.counter.get(i));
				if(!node.p.isEmpty())
				{
					right.p.add(node.p.get(i));
				}
			}
		}




		if (node.numberOfChildren() > 0) {
			for (int j = medianIndex + 1; j < node.numberOfChildren(); j++) {
				Node c = node.getChild(j);
				right.addChild(c);

			}
		}
		if (!node.p.isEmpty())
		{
			node.p.clear();
			node.counter.clear();
		}



		if (node.parent == null) {

			// new root, height of tree is increased
			Node newRoot = new Node();
			newRoot.addKeyr(medianValue);
			newRoot.NodeName=t.root.NodeName;
			node.parent = newRoot;
			t.root = newRoot;


			left.parent=t.root;
			right.parent=t.root;
			int level = Integer.parseInt((nodeToSplit.NodeName.split("Node"))[1])+1;
			String alpha= t.root.getChildNumber();
			left.NodeName= t.root.NodeName+level+alpha;
			Node faker = new Node();
			faker.NodeName="";
			t.root.addChild(faker);
			alpha= t.root.getChildNumber();
			right.NodeName= t.root.NodeName+level+alpha;
			t.root.ChildNodesNames.remove(t.root.ChildNodesNames.size()-1);

			if(left.ChildNodesNames.size()!=0)
			{
				String[] leveler=left.NodeName.split("Node")[1].split("");


				level =  level = Integer.parseInt(leveler[leveler.length-2])+1;
				for(int i =0; i<left.ChildNodesNames.size();i++)
				{ alpha= right.getChildNumber2(i);
					Node n = new Node();
					String oldname= "data//"+left.ChildNodesNames.get(i)+".bin";
					Node n1= n.Deserialize(oldname);
					String newName = left.NodeName+level+alpha;
					n1.NodeName=newName;
					n1.parent=left;

					left.ChildNodesNames.add(i,newName);
					left.ChildNodesNames.remove(i+1);
					File deletePage = new File(oldname);
					deletePage.delete();
					n1.changenameofotherchilds();


				}

			}
			if(right.ChildNodesNames.size()!=0)
			{
				String[] leveler=right.NodeName.split("Node")[1].split("");



				level =  level = Integer.parseInt(leveler[leveler.length-2])+1;
				for(int i =0; i<right.ChildNodesNames.size();i++)
				{	 alpha= right.getChildNumber2(i);
					Node n = new Node();
					String oldname= "data//"+right.ChildNodesNames.get(i)+".bin";
					Node n1= n.Deserialize(oldname);
					String newName = right.NodeName+level+alpha;
					n1.NodeName=newName;
					n1.parent=right;
					right.ChildNodesNames.add(i,newName);
					right.ChildNodesNames.remove(i+1);
					File deletePage = new File(oldname);
					deletePage.delete();
					n1.changenameofotherchilds();


				}

			}

			t.root.addChild(left);
			t.root.addChild(right);

			right.Serialize("data//"+right.NodeName+".bin");
			left.Serialize("data//"+left.NodeName+".bin");
		} else {

			Node parent = node.parent;

			boolean kofta= false;


			if(parent.NodeName.equals(t.root.NodeName))
			{
				parent=t.root;
				kofta=true;
			}
			else
			{
				parent=node.Deserialize("data//"+node.parent.NodeName+".bin");
			}
			parent.addKeyr(medianValue);

			String nodels=node.NodeName;


			int index= parent.removeChild(node);
			left.parent=parent;
			left.NodeName=nodels;
			String[] leveler=parent.NodeName.split("Node")[1].split("");
			int level =0;
			if(kofta)
			{
				level = Integer.parseInt(leveler[leveler.length-1])+1;
			}
			else
			{
				level = Integer.parseInt(leveler[leveler.length-2])+1;
			}
			Node faker = new Node();
			faker.NodeName="";
			parent.addChild(faker);
			String alpha= parent.getChildNumber();
			right.parent=parent;
			right.NodeName=parent.NodeName+level+alpha;
			parent.ChildNodesNames.remove(parent.ChildNodesNames.size()-1);



			if(left.ChildNodesNames.size()!=0)
			{
				leveler=left.NodeName.split("Node")[1].split("");


				level =  level = Integer.parseInt(leveler[leveler.length-2])+1;
				for(int i =0; i<left.ChildNodesNames.size();i++)
				{
					alpha= left.getChildNumber2(i);

					Node n = new Node();
					String oldname= "data//"+left.ChildNodesNames.get(i)+".bin";
					Node n1= n.Deserialize(oldname);
					n1.parent=left;

					String newName = left.NodeName+level+alpha;
					n1.NodeName=newName;
					left.ChildNodesNames.add(i,newName);
					left.ChildNodesNames.remove(i+1);
					File deletePage = new File(oldname);
					deletePage.delete();
					n1.changenameofotherchilds();


				}

			}

			if(right.ChildNodesNames.size()!=0)
			{
				leveler=right.NodeName.split("Node")[1].split("");


				level =  level = Integer.parseInt(leveler[leveler.length-2])+1;
				for(int i =0; i<right.ChildNodesNames.size();i++)
				{	 alpha= right.getChildNumber2(i);
					Node n = new Node();
					String oldname= "data//"+right.ChildNodesNames.get(i)+".bin";
					Node n1= n.Deserialize(oldname);
					String newName = right.NodeName+level+alpha;
					n1.NodeName=newName;
					n1.parent=right;

					right.ChildNodesNames.add(i,newName);
					right.ChildNodesNames.remove(i+1);
					File deletePage = new File(oldname);
					deletePage.delete();

					n1.changenameofotherchilds();



				}

			}




			parent.addChild(left,index);

			parent.addChild(right,index+1);

			right.Serialize("data//"+right.NodeName+".bin");
			left.Serialize("data//"+left.NodeName+".bin");
			if(!kofta)
			{
				parent.Serialize("data//"+parent.NodeName+".bin");
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
	public static void removeKeyNonleaf(Node del,String strTableName,CompareablePolygons value,int min,String col)  {
		RTree tree = Deserialize("data//"+strTableName+"RTree"+col+".bin");
		int index= del.keys.indexOf(value);
		if(index>=0) {

			del.keys.remove(value);
			//del.counter.remove(index);
			String leastgetter = del.ChildNodesNames.get(index+1);
			Node least = Node.Deserialize("data//"+leastgetter+".bin");
			Object key = findLeastLeaf(least);
			del.keys.add(index,key);
			if(least.ChildNodesNames.size()>0) {
				removeKeyNonleaf(least,strTableName, value,min,col);
				if(least.parent!=null)
					least.Serialize("data//"+leastgetter+".bin");
				else
				{
					RTree temp = Deserialize("data//"+strTableName+"RTree"+col+".bin");
					temp.root = del;
					temp.Serialize("data//"+strTableName+"RTree"+col+".bin");
				}
				if(least.keys.size()<min);
				rearrange(least, tree, false, strTableName, col);
			}
		}
	}
	public static void renameTree(Node node,String strTableName,String col,int level) {
		if(level ==0) {
		
		ArrayList<String> children = node.ChildNodesNames;
		for(int i=0;i<children.size();i++) {
			Node nodeToChandge = Node.Deserialize("data//"+children.get(i)+".bin");
			
			renameTree(nodeToChandge,strTableName, col, level+1 );
		}
			
		}
		else {
			File renameNode = new File("data//"+node.NodeName +".bin");
			Node parent=null;
			if(level>1) {
			RTree temp = Deserialize("data//"+strTableName+"RTree"+col+".bin");
			parent = temp.root;
			
			}
			else
				parent = Node.Deserialize("data//"+node.parent.NodeName+".bin");
			int index = parent.ChildNodesNames.indexOf(node.NodeName);
			String a = getEquivlant(index);
			File newName = new File("data//"+node.parent.NodeName+level+a+".bin");
			renameNode.renameTo(newName);
			parent.ChildNodesNames.remove(index);
			parent.ChildNodesNames.add(index,"data//"+node.parent.NodeName+level+a+".bin");
			if(level>1) {
				RTree temp = Deserialize("data//"+strTableName+"RTree"+col+".bin");
				temp.root = parent;
				temp.Serialize("data//"+strTableName+"RTree"+col+".bin");
			}
			else
				parent.Serialize("data//"+node.parent.NodeName+".bin");
			ArrayList<String> children = node.ChildNodesNames;
			for(int i=0;i<children.size();i++) {
				Node nodeToChandge = Node.Deserialize("data//"+children.get(i)+".bin");
				
				renameTree(nodeToChandge,strTableName, col, level+1 );
			}

		}

	}
	public static void delete(String strTableName,CompareablePolygons value,ArrayList<Object> array,String col)  {
		//System.out.println(strTableName+"RTree"+col+".bin");
		RTree tree = Deserialize("data//"+strTableName+"RTree"+col+".bin");
		//TODO: add when children less than min
		//TODO: finish handelling the root

		Node root = tree.root;
	//	print(root);
//    	ArrayList<Object> keys= root.keys;
//    	Node node= null;
//    	for(int i =0;i<keys.size();i++) {
//    		if(value.compareTo(keys.get(i))<0) {
//    			node=Node.Deserialize("data//"+root.ChildNodesNames.get(i)+".bin");
//    		}
//    	}
		ArrayList<Node> acc = new ArrayList<>();
	//	System.out.println(value);
		ArrayList<Node> todelete = appear(root,(CompareablePolygons)value,array,acc);
	//	System.out.println(todelete);
//    	for(int i=0 ; i<todelete.size();i++) {
//        	System.out.println(todelete.get(i).keys);
//        	}
		for(int j = todelete.size()-1;j>=0;j--) {
			Node delgetter = todelete.get(j);
			Node del =null;
			if(delgetter.parent!=null)
				del=	Node.Deserialize("data//"+delgetter.NodeName+".bin");
			else {
				RTree temp = Deserialize("data//"+strTableName+"RTree"+col+".bin") ;
				del = temp.root;
			}
 
			boolean leaf =del.ChildNodesNames.size()==0;
			//System.out.println(del.keys);
			
			
			int index = -1;
			for(int i=0;i<del.keys.size();i++) {
				try {
				 if(value.compareTo(del.keys.get(i))==0) {
					 index =i; 
				 	 break;
				 }}
			catch(Exception e) {
				e.printStackTrace();
					 
			}}
		//	System.out.println(index);
			if(index>-1) {

				//System.out.println(del.keys.remove(value));



//					System.out.println(del.ChildNodesNames);
//					System.out.println(del.ChildNodesNames.size());
				if(leaf) {
					if(del.counter.get(index)==1) {
						del.p.remove(index);
					//	System.out.println(del.keys);
						del.keys.remove(value);
						del.counter.remove(index);
					}
					else {
						del.p.get(index).v.remove(array);
						del.counter.add(index,del.counter.get(index)-1 );
						del.counter.remove(index+1);
						del.Serialize("data//"+del.NodeName+".bin");
					}

				}
				else {
					removeKeyNonleaf(del, strTableName, value, tree.minKeySize, col);
				}
		//		System.out.println(del.keys);
				if(del.parent!=null)
					del.Serialize("data//"+del.NodeName+".bin");
				else {
					
					RTree temp = Deserialize("data//"+strTableName+"RTree"+col+".bin") ;
					if(del.keys.size()>0) {
					temp.root=del;}
					else
						temp.root=null;
					temp.Serialize("data//"+strTableName+"RTree"+col+".bin");

				}
				//del.counter.add(index, 0);
		//		System.out.println(tree.minChildrenSize);
				//	System.out.println(del.parent.NodeName);
				if(tree.minKeySize >del.keys.size()||(!leaf &&del.ChildNodesNames.size()<tree.minChildrenSize)) {
			//		System.out.println("HI");
				//	System.out.println(del.NodeName);
					rearrange(del,tree,leaf, strTableName, col);

				}


			}

			else {
				if(del.parent!=null)
					del.Serialize("data//"+del.NodeName+".bin");
				else {
					RTree temp = Deserialize("data//"+strTableName+"RTree"+col+".bin") ;
					temp.root=del;
					temp.Serialize("data//"+strTableName+"RTree"+col+".bin");
				}

				if(tree.minKeySize >del.keys.size()||(!leaf &&del.ChildNodesNames.size()<tree.minChildrenSize) ) {

					rearrange(del,tree,del.ChildNodesNames.size()==0, strTableName, col);

				}
			}
		}

	}
	public static void rootHandl(Node root,String strTableName,String col) {
		if(root.keys.size()==0&&root.ChildNodesNames.size()==0)
			return ;
	//	System.out.println("Root handel");
	//	System.out.println(root.keys);
		if(root.keys.size()==0) {
			if(root.ChildNodesNames.size()==0) {
				return;
			}
			if(root.ChildNodesNames.size()==1) {
				//TODO: rename w n5er el level
				RTree temp = Deserialize("data//"+strTableName+"RTree"+col+".bin");
				Node newRoot = Node.Deserialize("data//"+root.ChildNodesNames.get(0)+".bin");
				File deleteNode = new File("data//"+newRoot.NodeName+".bin");
				deleteNode.delete();
				newRoot.parent=null;
				newRoot.NodeName=strTableName+col+"Node0";
				temp.root=newRoot;
	//			System.out.println("root handl tany");
	//			System.out.println(newRoot.ChildNodesNames);
				temp.Serialize("data//"+strTableName+"RTree"+col+".bin");
				renameTree(newRoot, strTableName, col, 0);
			}
			else {
				Node rightNode = Node.Deserialize("data//"+root.ChildNodesNames.get(1)+".bin");
				Object newRootKey = findLeastLeaf(rightNode);
				root.keys.add(newRootKey);
			}
		}
		else
		{
			return;
		}
	}
	public static Object findLeastLeaf(Node node) {
		if(node.ChildNodesNames.size()>0) {
			Node least = Node.Deserialize("data//"+node.ChildNodesNames.get(0)+".bin");
			return findLeastLeaf(least);
		}
		else
			return node.keys.get(0);

	}
	public static Object findMostLeaf(Node node) {
		if(node.ChildNodesNames.size()>0) {
			Node most = Node.Deserialize("data//"+node.ChildNodesNames.get(node.ChildNodesNames.size()-1)+".bin");
			return findMostLeaf(most);
		}
		else
			return node.keys.get(0);

	}
	public static void rearrange(Node node,RTree tree,boolean leaf,String strTableName,String col) {
		//TODO: handling root on the recursion step
		//TODO: case parent b key wa7ed w 2 pointers mohem t3mlha trace
		int max = tree.maxKeySize;
		int min =tree.minKeySize;
		int minChil= tree.minChildrenSize;
		Node parentgeter = node.parent;
		if(parentgeter ==null) {

			rootHandl(node,strTableName,col);
			return;
		}
		Node parent =null;
		if(parentgeter.NodeName.equals(strTableName+col+"Node0")) {
			RTree temp = Deserialize("data//"+strTableName+"RTree"+col+".bin");
			parent = temp.root;
		}
		else {
			parent = Node.Deserialize("data//"+parentgeter.NodeName+".bin");
		}

		
		int index =-1;
		for(int i=0;i<parent.ChildNodesNames.size();i++) {
			if(parent.ChildNodesNames.get(i).equals(node.NodeName))
			{
				index=i;
				break;
			}
		}
		int leftNodeIndex = index-1;
		boolean canMoveLeft= leftNodeIndex>=0;
		int rightNodeIndex = index +1;
		//check if the deleted node is a leaf node
		if(leaf) {

			if(canMoveLeft ) {
				Node leftNode = Node.Deserialize("data//"+parent.ChildNodesNames.get(leftNodeIndex)+".bin");
				
				if(min <= leftNode.keys.size()-1) {
					Object movedKey = leftNode.keys.get(leftNode.keys.size()-1);
					node.p.add(0,leftNode.p.get(leftNode.keys.size()-1));
					node.counter.add(0,leftNode.counter.get(leftNode.keys.size()-1));
					node.keys.add(0, movedKey);
					leftNode.p.remove(leftNode.keys.size()-1);
					leftNode.counter.remove(leftNode.keys.size()-1);
					leftNode.keys.remove(leftNode.keys.size()-1);
					parent.keys.remove(leftNodeIndex);
					parent.keys.add(leftNodeIndex, movedKey);
					parent.Serialize("data//"+parent.NodeName+".bin");
					node.Serialize("data//"+node.NodeName+".bin");
					leftNode.Serialize("data//"+leftNode.NodeName+".bin");
					if(parent.keys.size()<min||parent.ChildNodesNames.size()<minChil)
						rearrange(parent, tree,false, strTableName, col);
					return;
				}
				if(max>=leftNode.keys.size()+node.keys.size() && leftNode.keys.size()<max) {
					leftNode.keys.addAll(node.keys);
					leftNode.counter.addAll(node.counter);
					leftNode.p.addAll(node.p);
					leftNode.Serialize("data//"+leftNode.NodeName+".bin");
					File deleteNode = new File("data//"+node.NodeName+".bin");
					deleteNode.delete();
					parent.keys.remove(leftNodeIndex);
					parent.counter.remove(leftNodeIndex);
					parent.ChildNodesNames.remove(node.NodeName);
					parent.Serialize("data//"+parent.NodeName+".bin");
					parent = Node.Deserialize("data//"+parent.NodeName+".bin");
					if(parent.keys.size()<min || parent.ChildNodesNames.size()<minChil)
						rearrange(parent, tree,false, strTableName, col);
					return;
				}
				//this if condition can be replaced with else as it's never the case that the 2 conditions would be false
			

			}
			else {
				Node rightNode = Node.Deserialize("data//"+parent.ChildNodesNames.get(rightNodeIndex)+".bin");
				if(min <= rightNode.keys.size()-1) {
					Object movedKey = rightNode.keys.get(0);
					node.keys.add(movedKey);
					node.p.add(rightNode.p.get(0));
					node.counter.add(rightNode.counter.get(0));
					rightNode.keys.remove(0);
					rightNode.p.remove(0);
					rightNode.counter.remove(0);
					parent.keys.remove(index);
					parent.keys.add(index, findLeastLeaf(node));
					parent.Serialize("data//"+parent.NodeName+".bin");
					node.Serialize("data//"+node.NodeName+".bin");
					rightNode.Serialize("data//"+rightNode.NodeName+".bin");
					if(parent.keys.size()<min || parent.ChildNodesNames.size()<minChil)
						rearrange(parent, tree,false, strTableName, col);
					return;
				}

				if(node.keys.size()+rightNode.keys.size()<= max) {
					rightNode.keys.addAll(0, node.keys);
					rightNode.p.addAll(0,node.p);
					rightNode.counter.addAll(0,node.counter);
					rightNode.Serialize("data//"+rightNode.NodeName+".bin");
					File deleteNode = new File("data//"+node.NodeName+".bin");
					deleteNode.delete();
					parent.keys.remove(index);
					parent.counter.remove(index);
					parent.ChildNodesNames.remove(node.NodeName);
					parent.Serialize("data//"+parent.NodeName+".bin");
					if(parent.keys.size()<min || parent.ChildNodesNames.size()<minChil)
						rearrange(parent, tree,false, strTableName, col);
					return;
				}
				

			}

		}
		//if the deleted node is not a leaf node
		else {

			//Node rightNode = Node.Deserialize("data//"+parent.ChildNodesNames.get(rightNodeIndex)+".bin");
			//int maxChil = tree.maxChildrenSize;

//			if(node.keys.size()<min) {
//				ArrayList<String> movedChildren = node.ChildNodesNames;
//				
//				if(leftNodeIndex>=0) {
//					Node leftNode = Node.Deserialize("data//"+parent.ChildNodesNames.get(leftNodeIndex)+".bin");
//					if(min <= leftNode.keys.size()-1) {
//						String childToMove =leftNode.ChildNodesNames.get(leftNode.ChildNodesNames.size()-1);
//						leftNode.ChildNodesNames.remove(leftNode.ChildNodesNames.size()-1);
//						Object keyToMove = leftNode.keys.get(leftNode.keys.size()-1);
//						leftNode.keys.remove(leftNode.keys.size()-1);
//						node.keys.add(0,keyToMove);
//						node.ChildNodesNames.add(0, childToMove);
//						Object leastNewLeaf = findLeastLeaf(node);
//						parent.keys.remove(leftNodeIndex);
//						parent.keys.add(leftNodeIndex, leastNewLeaf);
//						leftNode.Serialize("data//"+leftNode.NodeName+".bin");
//						parent.Serialize("data//"+parent.NodeName+".bin");
//						node.Serialize("data//"+node.NodeName+".bin");
//						return;
//					}
//					if(leftNode.numberOfChildren()+movedChildren.size()<=leftNode.maxChildSize) {
//						leftNode.ChildNodesNames.addAll(movedChildren);
//						Node moved = Node.Deserialize("data//"+movedChildren.get(movedChildren.size()-1)+".bin");
//						leftNode.keys.add(parent.keys.get(index));
//						leftNode.keys.addAll(node.keys);
//						parent.ChildNodesNames.remove(index);
//						parent.keys.remove(index);
//						File deleteNode = new File("data//"+node.NodeName+".bin");
//						deleteNode.delete();
//						leftNode.Serialize("data//"+leftNode.NodeName+".bin");
//						parent.Serialize("data//"+parent.NodeName+".bin");
//						if(parent.keys.size()<min || parent.ChildNodesNames.size()<minChil)
//							rearrange(parent, tree,false, strTableName, col);
//						return;
//					}
//					
//				}
//				//joining with the right
//				Node rightNode = Node.Deserialize("data//"+parent.ChildNodesNames.get(rightNodeIndex)+".bin");
//				if(min <= rightNode.keys.size()-1) {
//					String childToMove = rightNode.ChildNodesNames.get(0);
//					rightNode.ChildNodesNames.remove(0);
//					Object keyToMove = rightNode.keys.get(0);
//					node.keys.add(parent.keys.get(index));
//					node.ChildNodesNames.add(childToMove);
//					parent.keys.remove(index);
//					parent.keys.add(index, keyToMove);
//					rightNode.Serialize("data//"+rightNode.NodeName+".bin");
//					parent.Serialize("data//"+parent.NodeName+".bin");
//					node.Serialize("data//"+node.NodeName+".bin");
//					return;
//				}
//				if(rightNode.numberOfChildren()+movedChildren.size()<=rightNode.maxChildSize) {
//					System.out.println("hnaa");
//					rightNode.ChildNodesNames.addAll(0,movedChildren);
//					//	Node moved = Node.Deserialize("data//"+movedChildren.get(0)+".bin");
//					rightNode.keys.add(0,parent.keys.get(index));
//					rightNode.keys.addAll(0,node.keys);
//					parent.ChildNodesNames.remove(index);
//					parent.keys.remove(index);
//					File deleteNode = new File("data//"+node.NodeName+".bin");
//					deleteNode.delete();
//					rightNode.Serialize("data//"+rightNode.NodeName+".bin");
//					parent.Serialize("data//"+parent.NodeName+".bin");
//					if(parent.keys.size()<min || parent.ChildNodesNames.size()<minChil)
//						rearrange(parent, tree,false, strTableName, col);
//					return;
//				}
//				
//
//			}
			if(node.keys.size()<min || node.ChildNodesNames.size()<minChil ) {

				if(leftNodeIndex>=0) {
					Node leftNode = Node.Deserialize("data//"+parent.ChildNodesNames.get(leftNodeIndex));
					// borrow from left
					if(leftNode.ChildNodesNames.size()>minChil && leftNode.keys.size()>min) {
						node.ChildNodesNames.add(0,leftNode.ChildNodesNames.get(leftNode.ChildNodesNames.size()-1));
						node.keys.add(0,leftNode.keys.get(leftNode.keys.size()-1));
						leftNode.ChildNodesNames.remove(leftNode.ChildNodesNames.size()-1);
						leftNode.keys.remove(leftNode.keys.size()-1);
						Object newLeastLeaf = findLeastLeaf(node);
						parent.keys.remove(leftNodeIndex);
						parent.keys.add(leftNodeIndex, newLeastLeaf);
						node.Serialize("data//"+node.NodeName+".bin");
						leftNode.Serialize("data//"+leftNode.NodeName+".bin");
						parent.Serialize("data//"+parent.NodeName+".bin");

						return;
					}
					// merge with left
					else {
						leftNode.ChildNodesNames.addAll(node.ChildNodesNames);
						leftNode.keys.addAll(node.keys);
						parent.ChildNodesNames.remove(index);
						parent.keys.remove(leftNodeIndex);
						if(leftNode.ChildNodesNames.size()>leftNode.keys.size()+1) {
							Object key = findMostLeaf(leftNode);
							leftNode.keys.add(key);
							}
						File deleteNode = new File("data//"+node.NodeName+".bin");
						deleteNode.delete();
						parent.Serialize("data//"+parent.NodeName+".bin");
						leftNode.Serialize("data//"+leftNode.NodeName+".bin");
						if(parent.keys.size()<min || parent.ChildNodesNames.size()<minChil)
							rearrange(parent, tree, false, strTableName, col);
						return;

					}
				}
				else {
					Node rightNode = Node.Deserialize("data//"+parent.ChildNodesNames.get(rightNodeIndex)+".bin");
					// borrow from right
					if(rightNode.ChildNodesNames.size()>minChil && rightNode.keys.size()>min) {
						node.ChildNodesNames.add(rightNode.ChildNodesNames.get(0));
						node.keys.add(rightNode.keys.get(0));
						rightNode.ChildNodesNames.remove(0);
						rightNode.keys.remove(0);
						parent.keys.remove(index);
						Object leastNewLeaf = findLeastLeaf(rightNode);
						parent.keys.add(index,leastNewLeaf);
						node.Serialize("data//"+node.NodeName+".bin");
						rightNode.Serialize("data//"+rightNode.NodeName+".bin");
						parent.Serialize("data//"+parent.NodeName+".bin");
						return;
					}
					// merge with right
					else {
						node.ChildNodesNames.addAll(rightNode.ChildNodesNames);
						node.keys.addAll(rightNode.keys);
						parent.ChildNodesNames.remove(rightNodeIndex);
						parent.keys.remove(index);
						if(node.ChildNodesNames.size()>node.keys.size()+1) {
							Object key = findLeastLeaf(Node.Deserialize("data//"+node.ChildNodesNames.get(1)+".bin"));
						}
						File deleteNode = new File("data//"+rightNode.NodeName+".bin");
						deleteNode.delete();
						parent.Serialize("data//"+parent.NodeName+".bin");
						node.Serialize("data//"+node.NodeName+".bin");
						if(parent.keys.size()<min || parent.ChildNodesNames.size()<minChil)
							rearrange(parent, tree, false, strTableName, col);
						return;

					}
				}


			}



		}
	}
	@SuppressWarnings("rawtypes")
	public static ArrayList<Node> appear(Node node,CompareablePolygons value, ArrayList<Object> array,ArrayList<Node>acc){
		//ArrayList<Node> c = new ArrayList<>();
		ArrayList<Object> keys= node.keys;
		//Node node= null;
		
		for(int i =0;i<keys.size();i++) {
	//		System.out.println(value);
		//.out.println((CompareablePolygons)keys.get(i));
		//	System.out.println(node.ChildNodesNames.size());
			if(value.compareTo((CompareablePolygons)keys.get(i))<0 && node.ChildNodesNames.size()>0) {
				node=Node.Deserialize("data//"+node.ChildNodesNames.get(i)+".bin");
				appear(node,value,array,acc);
			}
			if(value.compareTo((CompareablePolygons)keys.get(i))>0 && node.ChildNodesNames.size()>0&&i==keys.size()-1) {
				node=Node.Deserialize("data//"+node.ChildNodesNames.get(i+1)+".bin");
				appear(node,value,array,acc);
			}
			if(value.compareTo((CompareablePolygons)keys.get(i))==0&&node.ChildNodesNames.size()==0) {
				//	System.out.println(node.p);
			//	System.out.println(node.p.get(i).v.size());
			//	System.out.println(node.p.get(0).v.size());
			//	System.out.println(node.p.get(i).v);
				//System.out.println(array);
				for(int j = 0; j <node.p.get(i).v.size(); j++) {
					boolean ignore=true;

					boolean flag =true;
					for(int fal=0;fal<array.size();fal++) {
						 ignore=true;
						for(int karma=0;karma<( (ArrayList) node.p.get(i).v.get(j)).size();karma++) {
						if(( (ArrayList) node.p.get(i).v.get(j)).get(karma) instanceof Polygon) {
							  ArrayList<Object>  helpme = ( (ArrayList<Object>) node.p.get(i).v.get(j));
						Polygon t = (Polygon) helpme.get(karma);
						CompareablePolygons cpp =new CompareablePolygons(t);
						
						if(cpp.compareTo(array.get(karma))==0) {
							ignore =false ;
						} 
						
						}
						}
		//				System.out.println(!( (ArrayList) node.p.get(i).v.get(j)).contains(array.get(fal)));
						if(!( (ArrayList) node.p.get(i).v.get(j)).contains(array.get(fal)))
							flag=false;
					}
					
					if(flag||!ignore)
						acc.add(node); 
					
	//				System.out.println(acc);
					
					
				}
				return acc;

			}
			if(node.ChildNodesNames.size()>0 && value.compareTo(keys.get(i))==0) {
				acc.add(node);
				node=Node.Deserialize("data//"+node.ChildNodesNames.get(i+1)+".bin");
				appear(node,value,array,acc);
			}
		}
		//array.addAll(c) ;
		return acc;
	}
	public static void main (String[] args) throws DBAppException, InterruptedException
	{}




	public static void print(Node node) {
        try {
            if(node.numberOfChildren()>0)
                //    System.out.println(node + "   "+node.keys);

                for(int i=0;i<node.numberOfChildren();i++) {
                    Node hi = Node.Deserialize("data//"+node.ChildNodesNames.get(i)+".bin")    ;
                    print(hi);
                }
        //    System.out.print(node.ChildNodesNames);
         //   System.out.println(node.keys+"   "+node.counter);



        }

        catch (NullPointerException e) {
            e.printStackTrace();
        }
    }


//	RTree t = new RTree();
//	RTree q1 = t.Deserialize("data//ahmedRTreegpa.bin");
//	System.out.println(q1.root.keys);
//	System.out.println(q1.root.counter);
//	System.out.println(q1.root.NodeName);
//	System.out.println(q1.root.ChildNodesNames.size());
//
//	for(int i =0;i<q1.root.ChildNodesNames.size();i++)
//	{
//		Node q =new Node();
//
//		Node t1=q.Deserialize("data//"+q1.root.ChildNodesNames.get(i)+".bin");
//		System.out.println("Name:"+t1.NodeName);
//		System.out.println("Parent Name:"+t1.parent.NodeName);
//		System.out.println("Keys:"+t1.keys);
//		System.out.println("count of values inside:"+t1.counter);
//		System.out.println("Child nodes names:"+t1.ChildNodesNames);
//		for(int j =0; j<t1.ChildNodesNames.size();j++)
//		{
//			 Node t2=q.Deserialize("data//"+t1.ChildNodesNames.get(j)+".bin");
//			System.out.println("CHILDName:"+t2.NodeName);
//			System.out.println("CHILDParent Name:"+t2.parent.NodeName);
//			System.out.println("CHILDKeys:"+t2.keys);
//			System.out.println("CHILDcount of values inside:"+t2.counter);
//			System.out.println("CHILD Child nodes names:"+t2.ChildNodesNames);
//		}
//
//		System.out.println("");
//	}
//
		public static ArrayList<Object> eappear(Node node,Comparable value,ArrayList<Object>acc){
		//ArrayList<Node> c = new ArrayList<>();
		ArrayList<Object> keys= node.keys;
		//Node node= null;
		
		for(int i =0;i<keys.size();i++) {
			if(value.compareTo(keys.get(i))<0 && node.ChildNodesNames.size()>0) {
				node=Node.Deserialize("data//"+node.ChildNodesNames.get(i)+".bin");
				return eappear(node,value,acc);


			}
			if(value.compareTo(keys.get(i))>0 && node.ChildNodesNames.size()>0&&i==keys.size()-1) {
				node=Node.Deserialize("data//"+node.ChildNodesNames.get(i+1)+".bin");
				return eappear(node,value,acc);

			}
			if(value.compareTo(keys.get(i))==0&&node.ChildNodesNames.size()==0) {
				//	System.out.println(node.p);
				for(int j = 0; j <node.p.get(i).v.size(); j++) {
					
					acc.add(node.p.get(i).v.get(j));
					
				}
				return acc;

			}
			if(node.ChildNodesNames.size()>0 && value.compareTo(keys.get(i))==0) {
				
				node=Node.Deserialize("data//"+node.ChildNodesNames.get(i+1)+".bin");
				return eappear(node,value,acc);
			}
		}
		//array.addAll(c) ;
		return acc;
	}
//
	public static ArrayList<Object> gappear(Node node,Comparable value,ArrayList<Object>acc){
		//ArrayList<Node> c = new ArrayList<>();
		ArrayList<Object> keys= node.keys;
		//Node node= null;
		
		for(int i =0;i<keys.size();i++) {
			if(value.compareTo(keys.get(i))<0 && node.ChildNodesNames.size()>0) {
				if(i==0 || value.compareTo(keys.get(i-1))>0 )
				{
					Node y=Node.Deserialize("data//"+node.ChildNodesNames.get(i)+".bin");
					gappear(y,value,acc);
				}
				
				Node x = Node.Deserialize("data//"+node.ChildNodesNames.get(i+1)+".bin");
				gappear(x,value,acc);
			}
			if(value.compareTo(keys.get(i))>0 && node.ChildNodesNames.size()>0&&i==keys.size()-1) {
				node=Node.Deserialize("data//"+node.ChildNodesNames.get(i+1)+".bin");
				gappear(node,value,acc);
			}
			if(value.compareTo(keys.get(i))<0&&node.ChildNodesNames.size()==0) {
				for(int j = 0; j <node.p.get(i).v.size(); j++) {
					
					acc.add(node.p.get(i).v.get(j));
					
				}
				

			}
			if(node.ChildNodesNames.size()>0 && value.compareTo(keys.get(i))==0) {
				
				node=Node.Deserialize("data//"+node.ChildNodesNames.get(i+1)+".bin");
				gappear(node,value,acc);
			}
		}
		//array.addAll(c) ;
		return acc;
	}

	public static ArrayList<Object> sappear(Node node,Comparable value,ArrayList<Object>acc){
		//ArrayList<Node> c = new ArrayList<>();
		ArrayList<Object> keys= node.keys;
		//Node node= null;
		
		for(int i =0;i<keys.size();i++) {
			if(value.compareTo(keys.get(i))<0 && node.ChildNodesNames.size()>0) {
				if(i == 0 )
				{
					Node y = Node.Deserialize("data//"+node.ChildNodesNames.get(i)+".bin");
					sappear(y,value,acc);
				}
			}
			if(value.compareTo(keys.get(i))>0 && node.ChildNodesNames.size()>0) {
				if(i == 0 )
				{
					Node y = Node.Deserialize("data//"+node.ChildNodesNames.get(i)+".bin");
					sappear(y,value,acc);
				}
				Node x = Node.Deserialize("data//"+node.ChildNodesNames.get(i+1)+".bin");
				sappear(x,value,acc);
			}
			if(value.compareTo(keys.get(i))>0&&node.ChildNodesNames.size()==0) {
				for(int j = 0; j <node.p.get(i).v.size(); j++) {
					
					acc.add(node.p.get(i).v.get(j));
					
				}
				

			}
			if(node.ChildNodesNames.size()<0 && value.compareTo(keys.get(i))==0) {
				
				node=Node.Deserialize("data//"+node.ChildNodesNames.get(i+1)+".bin");
				sappear(node,value,acc);
			}
		}
		//array.addAll(c) ;
		return acc;
	}
}
