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
		
		try
		{
			//Define a mapping for schema, an index number in array list maps to each data type
			ArrayList indexColumns= new ArrayList<String>();
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
							indexColumns.add(args[i].charAt(2));
						
						
						if(	args[i].equals("<")	)
						{	
							inputFile = new File(args[i+1]);
							String targetheapfilepath = args[0];
							InputModeClass.readFile(inputFile, targetheapfilepath, mySchemaMap);
						}
					}
				}
				else
				{
					//output mode
					OutputModeClass.writeToFile(args, mySchemaMap);
				}
			}
			
			else
			{
				//output mode
				OutputModeClass.writeToFile(args, mySchemaMap);
			}
		}
		catch(Exception e)
		{
			JOptionPane.showMessageDialog(null, "The program malfunctioned, please check input....\nError:\n" + e.toString());
			
			System.exit(0);
		}
	}

}
