import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;


public class OutputModeClass
{
	static String[] args;
	
	public static void writeToFile(String[] arg, ArrayList<String> myMap) throws Exception
	{
		args=arg;
		File inputFile = new File(args[0]);
		
		
		//Build index if arguments of the form -b are encountered
		
		ArrayList<Integer> indexedColumnList= new ArrayList<Integer>();
		
		for(int i=0;i<args.length;i++)
		{	
			if(args[i].contains("-b"))
			{	
				indexedColumnList.add(Integer.parseInt(	args[i].substring(2)));
			}
		}

		
		RandomAccessFile sourceheapfile = new RandomAccessFile(inputFile, "rw");
		
		/**********Read header of input heap file**************/
		
		//Arraylist to store mapped schema of heap file, each element in list has 2 integer fields, the number corresponding to the data type and the size of the data type
		
		ArrayList<Integer[]> hfMap = getSchema(sourceheapfile, myMap);
		
		
		if(indexedColumnList.size() != 0)
		{
			
			buildIndices(inputFile, sourceheapfile, hfMap, indexedColumnList, myMap);
		}
		else		//if we remove this else condition, the following code i.e selection and projection can run after indices are built
		{
			////System.out.println("------Output Mode-----");

			ArrayList<Integer> indexedColumns = getIndexedColumns(inputFile, hfMap);
			
			////System.out.println("\nIndexes are available on columns:" + indexedColumns);
			
			ArrayList<ArrayList<String>> selects = SelectionMappingClass.getSelectionMapping( args );
			
			boolean useindexes = false;
			
			Iterator selectitr = selects.iterator();
			
			while(selectitr.hasNext())
			{
				ArrayList<String> condition = (ArrayList<String>) selectitr.next();
				
				Integer c = Integer.parseInt( condition.get(0) );
				
				Integer operator = Integer.parseInt( condition.get(1) );
				
				if( (indexedColumns.indexOf(c) > -1) && (operator == 3))
				{
					useindexes = true;
				}
			}
			
			if(useindexes == true)//indexedColumns.size()!=0)	//selection and projection based on Linear Hash Indexed Columns
			{
				////System.out.println("Indexes will be used for this query.");
				
				Set<Long> rids = getQualifyingRids(indexedColumns, hfMap, inputFile);
				
				////System.out.println("\nRID's of Qualifying Records are:" + rids);
				
				selectQualifyingRecords(sourceheapfile, hfMap, rids);
			}
			else	//selection and projection based on heap file scan
			{
				////System.out.println("Indexes will be not used for this query.");
				
				selectQualifyingRecords(sourceheapfile, hfMap);
			}
			
			sourceheapfile.close();
		}

	}
	
	public static Set<Long> getQualifyingRids(ArrayList<Integer> indexedColumns, ArrayList<Integer[]> hfMap, File inputFile) throws Exception
	{
		Iterator indexedcolumnitr = indexedColumns.iterator();
		
		Set<Long> rids = new HashSet<Long>();
		
		ArrayList<ArrayList<String>> selects = SelectionMappingClass.getSelectionMapping( args );
		
		ArrayList projectioncolumns = ProjectionClass.getProjectionMapping(args, hfMap);				
		
		while(indexedcolumnitr.hasNext())
		{
			Integer column = (Integer) indexedcolumnitr.next();
			
			Iterator selectiterator = selects.iterator();
			
			ArrayList<Long> singlecolumnrids = new ArrayList<Long>();
			
			while(selectiterator.hasNext())
			{	
				ArrayList<String> condition = (ArrayList<String>) selectiterator.next(); 
				
				Integer conditioncolumn = Integer.parseInt( condition.get(0) );
				
				Integer conditionoperator =  Integer.parseInt( condition.get(1) );
				
				String comparisonvalue = condition.get(2);
				
				if((conditioncolumn == column)	&&	conditionoperator == 3)		//3 is mapped for equality
				{
					String indexfilename = new String(inputFile.getName() + "." + column.toString() + ".lht");
		 
					File indexfile = new File(indexfilename);
		
					String overflowfilename = new String(inputFile.getName() + "." + column.toString() + ".lho");
		 
					File overflowfile = new File(overflowfilename);
					
					////System.out.println("\nIndex File:"+indexfilename+"\nOverflow File:"+overflowfilename);
					
					singlecolumnrids = LinearHash.getqualifyingrecordids(indexfile, overflowfile, comparisonvalue, column, hfMap);
				}
			}
		
			Iterator<Long> it = singlecolumnrids.iterator();
		
			while(it.hasNext())
			{
				rids.add(it.next());
			}
		}
		
		return rids;
	}
	
	
	public static ArrayList<Integer> getIndexedColumns(File inputFile, ArrayList<Integer[]> hfMap)
	{
		ArrayList<Integer> cols = new ArrayList<Integer>(); 
		
		for(Integer i=1;i<=hfMap.size();i++)
		 {
			String indexfilename = new String(inputFile.getName() + "." + i.toString() + ".lht");
		 
			File indexfile = new File(indexfilename);
				
			if(InputModeClass.checkExists(indexfile))
			{
				cols.add(i);
			}
		}
		
		return cols;
	}
	
	public static void buildIndices(File inputFile, RandomAccessFile sourceheapfile, ArrayList<Integer[]> hfMap, ArrayList<Integer> indexedColumnList, ArrayList<String> myMap) throws IOException, InterruptedException
	{
		 DataTypeHandlerInterface[] read_function = new DataTypeHandlerInterface[7];
		 read_function[0] = new DataTypeHandler_i1();
		 read_function[1] = new DataTypeHandler_i2();
		 read_function[2] = new DataTypeHandler_i4();
		 read_function[3] = new DataTypeHandler_i8();
		 read_function[4] = new DataTypeHandler_r4();
		 read_function[5] = new DataTypeHandler_r8();
		 read_function[6] = new DataTypeHandler_cx();
		 
		 Iterator<Integer> itr = indexedColumnList.iterator();
		 
		 while(itr.hasNext())
		 {
			 Integer c = itr.next();
			 
			 String indexfilename = new String(inputFile.getName() + "." + c.toString() + ".lht");
			 
			 String overflowfilename = new String(inputFile.getName() + "." + c.toString() + ".lho");
				
			File indexfile = new File(indexfilename);
				
			File overflowfile = new File(overflowfilename);
			
			if(InputModeClass.checkExists(indexfile))
			{
				indexfile.delete();
				overflowfile.delete();
				
				//System.out.println("Sleep of 5 seconds to ensure that existing index file is deleted before rebuilding the new one.....");
				
				Thread.sleep(5000);
			}
		 }
		 
		sourceheapfile.seek(0);
			
		String headerstr = sourceheapfile.readLine();
		 
		 long heapfileptr = sourceheapfile.getFilePointer();
		 
		 while(heapfileptr<sourceheapfile.length())		//outer loop to read heap file
			{
				StringBuilder temprow = new StringBuilder();
				
				boolean rowqualifies = true;
				
				long recordptr = heapfileptr;
				
				for( int i=0; i<hfMap.size(); i++	)			//inner loop to read a record, one field at a time
				{
					int columnnumber = i+1;
					
					String field = read_function[ (hfMap.get(i))[0] ].readInfo(sourceheapfile, heapfileptr, (hfMap.get(i))[1] );
					
					//size of data in column is (hfMap.get(i))[1]
					
					if(indexedColumnList.indexOf(columnnumber) > -1)
					{
						//insert entry in index
						
						int sizeofcolumn = (hfMap.get(i))[1];
						
						int handlertype = (hfMap.get(i))[0];
						
						
						LinearHash.insertEntry(inputFile, recordptr, field, sizeofcolumn, handlertype, columnnumber, true, myMap, args);
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
		
		sourceheapfile.seek(0);
		
		String headerstr = sourceheapfile.readLine();
		
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
		sourceheapfile.close();
	}
	
	
	public static void selectQualifyingRecords(RandomAccessFile sourceheapfile, ArrayList<Integer[]> hfMap, Set<Long> rids) throws IOException, InterruptedException
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
		
		long heapfileptr = 0;
		
		Iterator itr = rids.iterator();
		
		while(itr.hasNext() )		//outer loop to read heap file
		{
			heapfileptr = (Long) itr.next();
			
			if(!(heapfileptr<sourceheapfile.length()) )
			{
				throw new IOException("Error in reading indexed records from heap file....retry with a clean index build....");
			}
			
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
			
			//rowqualifies = true;	/*REOMVE BEFORE SUBMISSION, FOR TESTING ONLY!!*/
			
			if(rowqualifies == true)
			{	
				ProjectionClass.projectrow(new String(temprow), args);
			}
		}
		
	}
	
	@SuppressWarnings("rawtypes")
	public static ArrayList<Integer[]> getSchema(RandomAccessFile sourceheapfile, ArrayList<String> myMap) throws IOException, InterruptedException
	{
		sourceheapfile.seek(0);
		
		String headerstr = sourceheapfile.readLine();
		
		////System.out.println("Size of Header:" + sourceheapfile.getFilePointer());
		
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
	
	public static ArrayList<Integer[]> getSchema(RandomAccessFile sourceheapfile, ArrayList<String> myMap, String[] args) throws IOException, InterruptedException
	{
		sourceheapfile.seek(0);
		
		String headerstr = sourceheapfile.readLine();
		
		////System.out.println("Size of Header:" + sourceheapfile.getFilePointer());
		
		
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
