import java.util.LinkedHashSet;
import java.util.LinkedList;

public class LinedListDuplicates {
	
	public static void main(String[] args)
	{
		LinkedList<String> list=new LinkedList<>();
		
		list.add("A");
		list.add("N");
		list.add("N");
		list.add("A");
		list.add("B");
		list.add("E");
		list.add("L");
		list.add("L");
		list.add("A");
		
		System.out.println("List-->"+list);
		
		LinkedHashSet<String> set=new LinkedHashSet<>();
		
		for(String s:list)
		{
			set.add(s);
		}
		
		
		System.out.println("Set-->"+set);
		
	}

}
