
public class Section
{
	public Long recordptr;
	public String data;
	
	Section()
	{
		recordptr = (long) 0;
		data = null;
	}
	
	Section(Long recordptr, String data)
	{
		this.recordptr = recordptr;
		this.data = data;
	}
}
