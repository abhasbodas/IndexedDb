import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;


public class DataTypeHandler_r4 implements DataTypeHandlerInterface
{

	@Override
	public long writeInfo(RandomAccessFile targetheapfile,long heapfileptr,String st, int size) throws IOException
	{	
		float d= new Float(st);
		targetheapfile.seek(heapfileptr);
		targetheapfile.writeFloat(d);
		//System.out.println("Length of file:" + targetheapfile.length());
		return(targetheapfile.length());
	}
	
	@Override
	public String readInfo(RandomAccessFile sourceheapfile, long heapfileptr,int size) throws IOException
	{
		sourceheapfile.seek(heapfileptr);
		Float i = sourceheapfile.readFloat();
		String st = i.toString();
		return st;
	}
	@Override
	public int compare(String field, String value)
	{
		if(Float.parseFloat(field) == Float.parseFloat(value))
			return 0;
		
		else if(Float.parseFloat(field) >= Float.parseFloat(value))
			return 1;
		
		return -1;
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

	@Override
	public byte[] GeByteArr(String st, int size)
	{
		float d= new Float(st);
		byte[] b = ConversionClass.toByta(d);
		return b;
	}
	
	@Override
	public String fromByteArr(byte[] b)
	{
		Float d = ConversionClass.toFloat(b);
		
		return d.toString();
	}
	

}
