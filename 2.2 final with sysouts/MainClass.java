import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import javax.swing.JOptionPane;


public class MainClass {

	/**
	 * @param args
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	
	public static void main(String[] args) throws IOException
	{ 
		
		long startTime = System.nanoTime();
		
		try
		{
			//Define a mapping for schema, an index number in array list maps to each data type
			ArrayList indexedColumnList= new ArrayList<Integer>();
			ArrayList mySchemaMap = new ArrayList<String>();
			mySchemaMap.add("i1");
			mySchemaMap.add("i2");
			mySchemaMap.add("i4");
			mySchemaMap.add("i8");
			mySchemaMap.add("r4");
			mySchemaMap.add("r8");
			mySchemaMap.add("cx");
			
			/********* code to check if first two args are '-i' and '<' *********/
			
			if(args.length>1)
			{
				if(	args[1].equals("-i"))		//input mode
				{
					////System.out.println("------Input Mode-----");
					File inputFile = null;
					
					for(int i=0;i<args.length;i++)
					{	
						if(args[i].contains("-b"))
						{	
							indexedColumnList.add(Integer.parseInt(	args[i].substring(2)));
						}
					}
					
					for(int i=1;i<args.length;i++)
					{
						if(	!(	args[i].contains("-")	)	)	//if(	args[i].equals("<")	) was earlier condition, when piping was not assumed
						{	
							inputFile = new File(args[i]);
							String targetheapfilepath = args[0];
							
							if(InputModeClass.checkExists(inputFile))
							{
								InputModeClass.readFile(args, inputFile, targetheapfilepath, mySchemaMap, indexedColumnList);
							}
						}
					}
				}
				else		//output mode, heap file name along with other input arguments is given
				{
					//output mode
					OutputModeClass.writeToFile(args, mySchemaMap);
				}
			}
			
			else		//output mode, only one argument, the heap file name is given as input
			{
				//output mode
				OutputModeClass.writeToFile(args, mySchemaMap);
			}
		}
		catch(Exception e)
		{
			JOptionPane.showMessageDialog(null, "The program malfunctioned, please check input....\nError:\n" + e.toString());
			
			e.printStackTrace();
			
			System.exit(0);
		}
		
		long endTime = System.nanoTime();
		System.out.println("\nThe program took "+(endTime - startTime) + " nanoseconds to run.");		//show run time of program
	}
	

}