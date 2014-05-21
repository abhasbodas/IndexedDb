import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class InputModeClass
{	
	public static void readFile(File inputFile,String targetheapfilepath, ArrayList<String> myMap) throws Exception
	{	
		long heapfileptr = 0;
		
		BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFile));
		
		String headerstr;
		
		//Arraylist to store mapped schema of csv, each element in list has 2 integer fields, the number corresponding to the data type and the size of the data type
		
		ArrayList<Integer[]> csvMap = new ArrayList<Integer[]>();
		
		/**********Read header of csv file and map it to defined numbers for each data type**************/
		
		if((headerstr=bufferedReader.readLine())!=null)
		{
			StringTokenizer stk = new StringTokenizer(headerstr,",");
			
			while(stk.hasMoreTokens())		//loop read tokens from the header string of the csv file
			{
				String tempstring = stk.nextToken();
				
				if(myMap.indexOf(tempstring)>-1)		//Map schema information of csv to numbers from 0 to 6 from myMap arraylist indexes
				{
					Integer[] temparr = { myMap.indexOf(tempstring) , Integer.parseInt( ( myMap.get( myMap.indexOf(tempstring ) ) ).substring(1) )};  
					csvMap.add(	temparr );		
				}
				else
				{
					Integer[] temparr = { 6 , Integer.parseInt(tempstring.substring(1)) }; 
					csvMap.add(temparr);
				}
		}
		}
		else
		{
			throw new IOException("input file does not exist or is empty!!");
		}
		
		RandomAccessFile targetheapfile = null;
		
		//checking if file exists if not create it
		
		 File file = new File(targetheapfilepath);
		 
		 /**********initialize write function classes**************/
		 
		 DataTypeHandlerInterface[] write_function = new DataTypeHandlerInterface[7];
		 write_function[0] = new DataTypeHandler_i1();
		 write_function[1] = new DataTypeHandler_i2();
		 write_function[2] = new DataTypeHandler_i4();
		 write_function[3] = new DataTypeHandler_i8();
		 write_function[4] = new DataTypeHandler_r4();
		 write_function[5] = new DataTypeHandler_r8();
		 write_function[6] = new DataTypeHandler_cx();
		 
		 if(!checkExists(file))
		 {
			 targetheapfile = new RandomAccessFile(file,"rw"); 
			 heapfileptr=0;
			 
			/*Code to write heap file schema in header*/
			
			String tempheaderstr = headerstr.concat("\n");
			heapfileptr = write_function[6].writeInfo(targetheapfile,heapfileptr,tempheaderstr,tempheaderstr.getBytes().length);
		 }
		 
		 else
		 {
			 targetheapfile = new RandomAccessFile(file, "rw");
			 
			 /*Code to match heap file schema from heap file header*/
			 
			 targetheapfile.seek(0);
			 String heapheader = targetheapfile.readLine();
			 
			 if( !heapheader.equals(headerstr) )
			 {
				 throw new IOException("Schema Mismatch!");
			 }
			 
			 heapfileptr=targetheapfile.length();
		}
		
		
		 /**********Read body of csv file**************/
		
		int t;
		
		while((headerstr=bufferedReader.readLine())!=null)	//loop to read file data line-by-line.
		{
			t=0;
			StringTokenizer stk = new StringTokenizer(headerstr,",");
			
			for(int columnnumber=1;stk.hasMoreTokens();columnnumber++)					//Inner loop to read tokens in a given line of the csv file
			{
				String tempstring = stk.nextToken();
			
				heapfileptr=write_function[ (csvMap.get(t))[0] ].writeInfo(targetheapfile, heapfileptr, tempstring, (csvMap.get(t))[1]);
				
				/*
				 * Maintain a counter to keep track of column number
				 * 
				 * If index is available on column, also perform insertion in the hash table file and if needed, the overflow file
				 * 
				 * If overflow occurs, trigger a split for the bucket pointed by "next"
				 * 
				 * 	
				 *Functions we have thought of:
				 * 		
				 *Input									Output/function			
				 *	
				 *-										Get depth/number of buckets in hash table
				 *
				 *value of field						hash value(bucket number), based on number of buckets in current hash table
				 *
				 *address of hash table bucket			insertion in bucket
				 *
				 *address of hash table bucket			rehashing of the main and overflow buckets with allocation of a new bucket(from free list if available)
				 *
				 *RID of record in heap file			returns the entire record/tuple
				 *
				 *				 
				 * */
				
				t++;
			
			}
		}
		
		targetheapfile.close();
	}
	
	
	// Writing the information to a random access file
	
	static boolean checkExists(File fileWritten)
	{	if(!(fileWritten.exists()))
			return(false);
		else
			return(true);
	
	}
}