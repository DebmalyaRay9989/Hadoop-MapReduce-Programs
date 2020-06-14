import java.util.ArrayList;
import java.util.Iterator;

public class ListDemo
{
	public static void main(String[] args)
	{
		ArrayList<Integer> list=new ArrayList<>();
		
		list.add(23);
		list.add(42);
		list.add(32);
		list.add(90);
		list.add(12);
		
		
		System.out.println(list);
		
		System.out.println("Printing using the iterator..");
		int sum=0;
		int index=0;
		Iterator<Integer> iterator=list.iterator();
		
		while(iterator.hasNext())
		{
			System.out.println("Index-"+index);
			int i=iterator.next();
			if(index%2==0)
			{
			
				sum=sum+i;
				System.out.println("Value at that index was-->"+i);
			}
			
			index++;
			
		}
		
		System.out.println("Sum of even index-->"+sum);
		
		
	}
}
