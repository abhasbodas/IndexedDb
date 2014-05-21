import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;


public class Block
{

	/*
	 * An object of the bucket class can be created in the form:
	 * 
	 * bucket b = new bucket(bucket_number_in_hash_table_or_overflow_file);
	 * 
	 * 	- all RID's and pointers in this bucket must be stored by the constructor as member variables of this object
	 * 	
	 * 
	 * Doubts: How to implement:
	 *
	 * 	- A pointer 'next' for the hash table split pointer
	 * 	
	 * 	- A count storing the number of buckets in the hash table
	 * 
	 * 
	 * Methods to be implemented:
	 * 
	 * 	- Read an entire bucket and return the record ID's in the bucket as an ArrayList<long>
	 * 
	 * 	- Write a value to a particular bucket
	 * 	
	 * 	- 
	 * */
	
	byte[] blockdata;
	
	public Block(int blocksize)
	{
		blockdata = new byte[blocksize]; 
		
		for(int i=0;i<blocksize;i++)
		{
			blockdata[i] = 0;
		}
	}
	
	public void readBlock(File indexfile, Integer bucketnumber, int blocksize) throws IOException
	{
		byte[] b = new byte[blocksize];
		
		RandomAccessFile rf = new RandomAccessFile(indexfile, "rw");
		
		//rf.read(b, bucketnumber*blocksize, blocksize);		//bucketnumber starts from 0 if we include header bucket in the count
		
		b = readFromFile(rf, bucketnumber*blocksize, blocksize);
		
		blockdata = b;
		
		rf.close();
	}
	
	public void writeBlock(File indexfile, Integer bucketnumber, int blocksize) throws IOException
	{
		RandomAccessFile rf = new RandomAccessFile(indexfile, "rw");
		
		//rf.write(blockdata, bucketnumber*blocksize, blocksize);
		
		writeToFile(rf, blockdata, bucketnumber*blocksize, blocksize);
		
		rf.close();
	}
	
	public void insertData(byte[] data, int offset)	//insert data in form of bytes into a block at the specified offset
	{												//(offsets start from 0, similar to a byte array,
		
		System.out.println("\nInput Data for the Offset:" + LinearHash.toLong(data));
		
		System.out.println("Insert Data on Offest" + offset + " :");
		
		for(int i=0;i<data.length;i++)				// so a block of 32 bytes has the first offset of 0 and last offset of 31)
		{
			//System.out.println(data.length);
			blockdata[offset+i] = data[i];
			//System.out.println("Source Byte:"+ data[i] + "Destination byte:" +blockdata[offset+i]);
		}
		
		byte[] arr = new byte[8];
		
		for(int x=0;x<=LinearHash.blocksize-8;x = x + 8)		//sysout for debugging
		{
			for(int k=0;k<8;k++)
			{
				arr[k] = blockdata[k + x];
			}
		
			System.out.print("Data at Offset " + x + ": " +LinearHash.toLong(arr));
		}
		
	}
	
	public static byte[] readFromFile(RandomAccessFile rf, int off, int blocksize) throws IOException
	{
		rf.seek(off);
		
		System.out.println("\nReading block with offset:" + off);
		
		byte[] b = new byte[blocksize];
		for(int i=0;i<b.length;i++)
		{
			b[i] = rf.readByte();
		}
		
		rf.close();
		
		byte[] arr = new byte[8];
		
		for(int x=0;x<=LinearHash.blocksize-8;x = x + 8)		//sysout for debugging
		{
			for(int k=0;k<8;k++)
			{
				arr[k] = b[k + x];
			}
		
			System.out.print("Data at Offset " + x + ": " +LinearHash.toLong(arr));
		}
		
		return b;
	}
	
	public static void writeToFile(RandomAccessFile rf, byte[] data, int off, int blocksize) throws IOException
	{
		//System.out.println("Writing:" + data + "\tof length:" + len + "\tat offset:" + off);
		
		rf.seek(off);
		
		System.out.println("\nWriting to block with offset:" + off);
		
		for(int i=0;i<blocksize;i++)
		{
			rf.writeByte(data[i]);
		}
		
		byte[] arr = new byte[8];
		
		for(int x=0;x<=LinearHash.blocksize-8;x = x + 8)		//sysout for debugging
		{
			for(int k=0;k<8;k++)
			{
				arr[k] = data[k + x];
			}
		
			System.out.print("Data at Offset " + x + ": " +LinearHash.toLong(arr));
		}
		
		rf.close();
	}
	
}
