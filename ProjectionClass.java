import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;


public class ProjectionClass
{
	public static ArrayList<Integer> getProjectionMapping(String[] args, ArrayList<Integer[]> countN)
	{
		ArrayList<Integer> projectioncolumns = new ArrayList<Integer>();
		
		boolean b = false;
		
		for(int i=0;i<args.length;i++)
		{
			if(args[i].contains("-p"))
			{
				projectioncolumns.add( Integer.parseInt(args[i].substring(2)) );
				b = true;
			}
		}
		
		if(b==false)
		{
			for(int i=1;i<=countN.size();i++)
			{
				projectioncolumns.add(i);
			}
		}
		
		return projectioncolumns;
	}
	
	public static void projectrow(String row, String[] args) throws IOException, InterruptedException
	{
		int numberofargs = args.length;
		
		if(numberofargs>1)
		{
			if(args[numberofargs-2].contains(">") && !args[numberofargs-3].contains("-s"))
			{
				File outputfile = new File(args[numberofargs-1]);
			
				BufferedWriter bw = new BufferedWriter(new FileWriter(outputfile, true));
				bw.write(row);
				bw.newLine();
				bw.close();
			}
			else
			{
				System.out.println(row);
			}
		}
		else
		{
			System.out.println(row);
		}

	}
	
	public static boolean checkFileExists(String[] args)
	{	 
		int numberofargs = args.length;
		
		if(numberofargs>1)
		{
			if(args[numberofargs-2].equals(">") && !args[numberofargs-3].contains("-s"))
			{
				File outputfile = new File(args[numberofargs-1]);
			
				if(outputfile.exists())			//if file exists, clear the previous contents and project new results
				{
					outputfile.delete();
				}
			
			}
		}
		return(true);
	}
}
