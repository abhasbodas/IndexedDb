import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.StringTokenizer;


public class OutputModeClass
{
	static String[] args;
	
	public static void writeToFile(String[] arg, ArrayList<String> myMap) throws Exception
	{
		args=arg;
		File inputFile = new File(args[0]);
		
		//System.out.println("------Output Mode-----");
		
		RandomAccessFile sourceheapfile = new RandomAccessFile(inputFile, "rw");
		
		/**********Read header of input heap file**************/
		
		//Arraylist to store mapped schema of heap file, each element in list has 2 integer fields, the number corresponding to the data type and the size of the data type
		ArrayList<Integer[]> hfMap = getSchema(sourceheapfile, myMap);
		
		selectQualifyingRecords(sourceheapfile, hfMap);
		
		sourceheapfile.close();
	}
	
	@SuppressWarnings("rawtypes")
	public static void selectQualifyingRecords(RandomAccessFile sourceheapfile, ArrayList<Integer[]> hfMap) throws IOException, InterruptedException
	{	
		
		DataTypeHandlerInterface[] compare_function = new DataTypeHandlerInterface[7];
		 compare_function[0] = new DataTypeHandler_i1();
		 compare_function[1] = new DataTypeHandler_i2();
		 compare_function[2] = new DataTypeHandler_i4();
		 compare_function[3] = new DataTypeHandler_i8();
		 compare_function[4] = new DataTypeHandler_r4();
		 compare_function[5] = new DataTypeHandler_r8();
		 compare_function[6] = new DataTypeHandler_cx();
		 
		 DataTypeHandlerInterface[] read_function = new DataTypeHandlerInterface[7];
		 read_function[0] = new DataTypeHandler_i1();
		 read_function[1] = new DataTypeHandler_i2();
		 read_function[2] = new DataTypeHandler_i4();
		 read_function[3] = new DataTypeHandler_i8();
		 read_function[4] = new DataTypeHandler_r4();
		 read_function[5] = new DataTypeHandler_r8();
		 read_function[6] = new DataTypeHandler_cx();
		 
		/*What this code does:
		 * 
		 * Position cursor at end of the header
		 * 
		 * outer loop that runs till end of file
		 * 
		 * 		inner loop that runs number of times equal to number of columns in schema, and retrieves each field
		 * 
		 *		inner loop also calls method to project the field that was just read it the row qualifies
		 */
		
		////System.out.println(	"Line after header:" + sourceheapfile.readLine()	);	//just checking if file ptr is in place
		
		ArrayList<ArrayList<String>> selects = SelectionMappingClass.getSelectionMapping( args );
		
		ArrayList projectioncolumns = ProjectionClass.getProjectionMapping(args, hfMap);
		
		long heapfileptr = sourceheapfile.getFilePointer();
		
		while(heapfileptr<sourceheapfile.length())		//outer loop to read heap file
		{
			StringBuilder temprow = new StringBuilder();
			
			boolean rowqualifies = true;
			
			for( int i=0; i<hfMap.size(); i++	)			//inner loop to read a record, one field at a time
			{
				String field = read_function[ (hfMap.get(i))[0] ].readInfo(sourceheapfile, heapfileptr, (hfMap.get(i))[1] );
				
				//append only if column is to be projected
				if( projectioncolumns.indexOf(i+1) > -1)
				{
					temprow.append(field);
					
					if( projectioncolumns.indexOf(i+1) != (projectioncolumns.size()-1))
					{
						temprow.append(",");
					}
				}
				if(rowqualifies!=false)
				{
					rowqualifies = compare_function[ (hfMap.get(i))[0] ].validateSelection(field, i+1, selects);
				}
				
				heapfileptr = heapfileptr + (hfMap.get(i))[1];	//increment file pointer
								
				
			}	//end of this loop means a record has been read
			
			if(rowqualifies == true)
			{	
				ProjectionClass.projectrow(new String(temprow), args);
			}
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static ArrayList<Integer[]> getSchema(RandomAccessFile sourceheapfile, ArrayList<String> myMap) throws IOException, InterruptedException
	{
		String headerstr = sourceheapfile.readLine();
		
		StringTokenizer stk = new StringTokenizer(headerstr,",");
		ArrayList<Integer[]> countN = new ArrayList<Integer[]>(); 
		
		while(stk.hasMoreTokens())		//Inner loop read tokens of the header of the csv file
		{
			String tempstring = stk.nextToken();
			
			if(myMap.indexOf(tempstring)>-1)		//Map schema information of csv to numbers from 0 to 6 from myMap arraylist indexes
			{
				Integer[] temparr = { myMap.indexOf(tempstring) , Integer.parseInt( ( myMap.get( myMap.indexOf(tempstring ) ) ).substring(1) )};  
				countN.add(	temparr );	
			}
			else
			{
				Integer[] temparr = { 6 , Integer.parseInt(tempstring.substring(1)) }; 
				countN.add(temparr);
			}
		}
		
		ArrayList projectioncolumns = ProjectionClass.getProjectionMapping(args, countN);
		
		stk = new StringTokenizer(headerstr,",");
		
		int columncounter = 1;
		
		StringBuilder temprow = new StringBuilder();
		
		while(stk.hasMoreTokens())		//Inner loop read tokens of the header of the csv file
		{
			
			String field = stk.nextToken();
			
			//append only if column is to be projected
			if( projectioncolumns.indexOf(columncounter) > -1)
			{
				temprow.append(field);
				
				if( projectioncolumns.indexOf(columncounter) != (projectioncolumns.size()-1))
				{
					temprow.append(",");
				}
			}
			columncounter++;
		}
			
		String projectionheader = new String(temprow);
		
		//check file exists
		ProjectionClass.checkFileExists(args);
		
		//call to project the header
		ProjectionClass.projectrow(projectionheader, args);
		
		return countN;
	}
}
