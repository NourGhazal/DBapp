package LavaScript;

import java.awt.Polygon;
import java.awt.Rectangle;
import java.io.Serializable;

public class CompareablePolygons extends Polygon implements Comparable,Serializable {
	Polygon p;
	
	
	public CompareablePolygons(Object O) {
		
		Polygon p = (Polygon) O;
		
		this.p=p;
	}
	
	
public String toString() {
	return p.getBoundingBox().height*p.getBoundingBox().width+"  multiply ya kimo" ;
}
	public int compareTo(Object O2) {
		Polygon P2 = (Polygon) O2;

		Rectangle R1 =	getBoundingBox();
		Rectangle R2 = P2.getBoundingBox();
        double Area1 = R1.getWidth()* R1.getHeight()	;
        double Area2 = R2.getWidth()* R2.getHeight()	;
        
        if(Area1 == Area2) return 0;
        if(Area1 < Area2) return -1;
        else {return 1;} 
        
		
		
	}
	 
 public boolean equals(Comparable x) {
	 return (this.compareTo(x)==0) ;
 }
	public int compareTo(CompareablePolygons O2) {
		return this.compareTo(O2.p);
		
	}


	public Rectangle getBoundingBox() {
		
		Rectangle R = this.p.getBoundingBox();
		
		return R;
	}

	
	
	
	
	
	
		
		
		public static void main (String[] args) {
			
			
			
			Object p=new  Polygon();
			System.out.println(p.getClass().toString());
			
		}
		
		
		
	
		
		

	
	
	

}
