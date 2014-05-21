import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;


public class DataTypeHandler_cx implements DataTypeHandlerInterface
{

	@Override
	public long writeInfo(RandomAccessFile targetheapfile,long heapfileptr,String st, int size) throws IOException
	{	
		targetheapfile.seek(heapfileptr);
		
		byte[] tobytes = st.getBytes();
		byte nullbyte = 0;
		
		for(int i=0;i<size;i++)
		{
			if(i<tobytes.length)
				targetheapfile.write(tobytes[i]);
			else
				targetheapfile.write(nullbyte);
		}
		//System.out.println("Length of file:" + targetheapfile.length());
		return(targetheapfile.length());
	}
	
	@Override
	public String readInfo(RandomAccessFile sourceheapfile,long heapfileptr,int size) throws IOException
	{	
		sourceheapfile.seek(heapfileptr);
		byte[] b = new byte[size];
		sourceheapfile.read(b);
		String st = new String(b).trim();
		return st;
	}
	@Override
	public int compare(String field, String value)
	{
		int x = field.compareTo(value);
		
		if(x > 0)
			return 1;
		
		else if(x < 0)
			return -1;
		
		return x;
	}

	@Override
	public boolean validateSelection(String field, Integer columnNumber, ArrayList<ArrayList<String>> selects)
	{
		boolean returnvalue = true;
		
		Iterator<	ArrayList<String>	> itr = selects.iterator();
	    
		while (itr.hasNext())
	    {
	    	ArrayList<String> element = itr.next();
	    	
	    	/*
	    	 * result		Operators that should return true
	    	 * 	-1					1 , 2 , 6
	    	 * 	0					2 , 3 , 4
	    	 * 	1					4 , 5 , 6
	    	 * */
	    	
	    	if(Integer.parseInt(element.get(0)) == columnNumber)
	    	{
	    		int operator = Integer.parseInt(element.get(1));
	    		
	    		int result = compare(field, element.get(2));	//result is 0 or 1 or -1
	    		
	    		//System.out.println(field + "vs." + element.get(2) + ":" + result);
	    		
	    		if(result == -1)
	    		{
	    			if( (operator == 1) || (operator == 2) || (operator == 6) )
	    			{
	    				returnvalue = true;
	    			}
	    			else
	    			{
	    				return false;
	    			}
	    		}
	    		else if(result == 0)
	    		{
	    			if( (operator == 2) || (operator == 3) || (operator == 4) )
	    			{
	    				returnvalue = true;
	    			}
	    			else
	    			{
	    				return false;
	    			}
	    		}
	    		else if(result == 1)
	    		{
	    			if( (operator == 4) || (operator == 5) || (operator == 6) )
	    			{
	    				returnvalue = true;
	    			}
	    			else
	    			{
	    				return false;
	    			}
	    		}
	    	}
	    	
	    }
	    
	    return returnvalue;
	}
	
	

}
