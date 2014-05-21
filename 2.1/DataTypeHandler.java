import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;


public interface DataTypeHandler 
{
	public long writeInfo(RandomAccessFile targetheapfile,long heapfileptr,String st, int size) throws IOException;
	
	public String readInfo(RandomAccessFile sourceheapfile,long heapfileptr, int size) throws IOException;
	
	public int compare(String field, String value);
	
	public boolean validateSelection(String field, Integer columnNumber, ArrayList<ArrayList<String>> selects);
	
}