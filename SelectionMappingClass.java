import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;


public class SelectionMappingClass {

public static ArrayList<ArrayList<String>> getSelectionMapping(String[] args) throws IOException
{
	
	StringBuilder tempstr = new StringBuilder();
	for(int i=0;i<args.length;i++)
			{
				tempstr.append(args[i]);
				tempstr.append(" ");
			}
	
	String arguments = new String(tempstr);
	
	
 ArrayList<String> columnnumbers=new ArrayList<String>();
 
 ArrayList<String> operatornumbers=new ArrayList<String>();
 
 ArrayList<String> comparisonvalues=new ArrayList<String>();

StringTokenizer st = new StringTokenizer(arguments, " "); 
st.nextToken();

while(st.hasMoreTokens()) {
	
	String argtoken = st.nextToken();
	
	if(argtoken.contains("-s"))
	{
		columnnumbers.add( (argtoken.substring(2)) );
		argtoken=st.nextToken();
		if(argtoken.contains("<") && !argtoken.contains("<=") && !argtoken.contains("<>"))
			operatornumbers.add("1");
		else if(argtoken.contains("<="))
			operatornumbers.add("2");
		else if(argtoken.contains("=") && !argtoken.contains("<=") && !argtoken.contains(">="))
			operatornumbers.add("3");
		else if(argtoken.contains(">="))
			operatornumbers.add("4");
		else if(argtoken.contains(">") && !argtoken.contains(">=") && !argtoken.contains("<>"))
			operatornumbers.add("5");
		else if(argtoken.contains("<>"))
			operatornumbers.add("6");
		
		argtoken=st.nextToken();
		comparisonvalues.add(argtoken);
		
	}

	// sorting the arraylist of arraylists
	
} 

String temp=null;

for(int x=0; x<columnnumbers.size();x++)
{
	for(int y=x;y<columnnumbers.size();y++)
	{
		
		if((Integer.parseInt(columnnumbers.get(x))>(Integer.parseInt(columnnumbers.get(y)))))
				{
				//list1
				temp=columnnumbers.get(y);
				columnnumbers.set(y, columnnumbers.get(x));
				columnnumbers.set(x,temp);
				//list2
				temp=operatornumbers.get(y);
				operatornumbers.set(y, columnnumbers.get(x));
				operatornumbers.set(x,temp);
				//list3
				temp=comparisonvalues.get(y);
				comparisonvalues.set(y, columnnumbers.get(x));
				comparisonvalues.set(x,temp);
				
				}
		
	}
}

//adding back to the arraylist

ArrayList<ArrayList<String>> allconditions = new ArrayList<ArrayList<String>>();

for(int x=0; x<columnnumbers.size();x++)
{	ArrayList<String> selectioncondition = new ArrayList<String>();
	selectioncondition.add(columnnumbers.get(x));
	selectioncondition.add(operatornumbers.get(x));
	selectioncondition.add(comparisonvalues.get(x));
	
	allconditions.add(selectioncondition);
}

return(allconditions);

}
	
}
