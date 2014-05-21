import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import javax.swing.JOptionPane;


public class StartClass {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException
	{ 
		
		try
		{
			//Define a mapping for schema, an index number in array list maps to each data type
			
			ArrayList myMap = new ArrayList<String>();
			myMap.add("i1");
			myMap.add("i2");
			myMap.add("i4");
			myMap.add("i8");
			myMap.add("r4");
			myMap.add("r8");
			myMap.add("cx");
			
			/********* code to check if first two args are '-i' and '<' *********/
			
			if(args.length>1)
			{
				if(	args[1].equals("-i")	&&		args[2].equals("<"))		//input mode
				{
					////System.out.println("------Input Mode-----");
				
					File inputFile = new File(args[3]);
					String targetheapfilepath = args[0];
					InputModeClass.readFile(inputFile, targetheapfilepath, myMap);
				}
				else
				{
					//output mode
					OutputModeClass.writeToFile(args, myMap);
				}
			}
			else
			{
				//output mode
				OutputModeClass.writeToFile(args, myMap);
			}
		}
		catch(Exception e)
		{
			JOptionPane.showMessageDialog(null, "The program malfunctioned, please check input....\nError:\n" + e.toString());
			
			//e.printStackTrace();
			
			System.exit(0);
		}
	}

}
