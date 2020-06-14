
public class RecursiveMethod
{
	int counter=0;
	
	public void square(long x)
	{
		
		if(counter<4)
		{
			long answer=x*x;
			System.out.println("Square -->"+answer);
			counter++;
			System.out.println("Counter--"+counter);
			square(answer);
			
		}
		else
		{
			System.exit(0);
		}
		
		
	}
	
	public static void main(String[] args)
	{
		RecursiveMethod rm=new RecursiveMethod();
		rm.square(2);
	}

}
