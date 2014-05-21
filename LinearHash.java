import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;



public class LinearHash
{

	public static int blocksize = 4096;	//one block is considered to be 4096 bytes or 4KB by default
	
	static int sectionsize;
	
	static int numberofsections;
	
	public static int blockspace;
	
	public static void insertEntry(File HeapFile, long recordptr, String entry, int sizeofentry, int handlertype, int columnnumber, boolean splitallowed, ArrayList<String> myMap, String[] args) throws IOException, InterruptedException
	{
		 
		sectionsize = 8 + sizeofentry;
		
		numberofsections = (int) Math.floor(blocksize/sectionsize);
		
		blockspace = sectionsize*numberofsections;
		
		//System.out.println("\n\nBlock space:" + effectiveblockspace);
		
		////System.out.println("\nBlock Size:"+ blocksize + "\tSection Size:" + sectionsize);
		
		//System.out.println("\nRecord Pointer being written:" + recordptr);
		
		//Thread.sleep(1000);
		
		DataTypeHandlerInterface[] conversion_function = new DataTypeHandlerInterface[7];
		conversion_function[0] = new DataTypeHandler_i1();
		conversion_function[1] = new DataTypeHandler_i2();
		conversion_function[2] = new DataTypeHandler_i4();
		conversion_function[3] = new DataTypeHandler_i8();
		conversion_function[4] = new DataTypeHandler_r4();
		conversion_function[5] = new DataTypeHandler_r8();
		conversion_function[6] = new DataTypeHandler_cx();
		
		Integer tempcolumnnumber = columnnumber;
		
		//check if index already exists
		String indexfilename = new String(HeapFile.getName() + "." + tempcolumnnumber.toString() + ".lht");
		
		String overflowfilename = new String(HeapFile.getName() + "." + tempcolumnnumber.toString() + ".lho");
		
		File indexfile = new File(indexfilename);
		
		File overflowfile = new File(overflowfilename);
		
		
		long numberofbuckets = 2;		//initially there is only 1 bucket in the hash index
										//this number excludes the header bucket
		
		long nextptr = 1;				//first bucket
		
		if(!InputModeClass.checkExists(indexfile))
		{
			//build new index file with next pointer at initial position and only one bucket in the file
			Block bl = new Block(blocksize);
			
			//Write initial data to hash table file
			
			//System.out.println("Initial Bucket:" + toLong(toByta(numberofbuckets)));
			
			bl.insertData(toByta(numberofbuckets), 0);	//insert data and offset
			
			//System.out.println("\nInitial Next pointer:" + toLong(toByta(nextptr)));
			
			bl.insertData(toByta(nextptr), 8);				//next pointer
			
			bl.insertData(toByta(numberofbuckets), 16);		//store the bucket number which ends a round
			
			bl.insertData(toByta(blockspace), 24);		//store block size utilized

			
			bl.writeBlock(indexfile, 0, blocksize);	//write header bucket to hash index
			
			bl = new Block(blocksize);	//make block object empty
			
			//start with 1 bit linear hash, with 2 buckets
			
			bl.writeBlock(indexfile, 1, blocksize);	//write first empty bucket to hash index
			
			bl.writeBlock(indexfile, 2, blocksize);	//write second empty bucket to hash index
			
			
			//write initial data (header + one empty bucket) to overflow file
			
			bl = new Block(blocksize);
			
			bl.insertData(toByta(1), 0);	//initial next free bucket pointer
			
			bl.insertData(toByta(blocksize), 8);	//store the blocksize
			
			
			bl.writeBlock(overflowfile, 0, blocksize);	//write header bucket to overflow file
			
			bl = new Block(blocksize);		//empty block
			
			bl.writeBlock(indexfile, 1, blocksize);	//write first empty bucket to overflow file
			
		}
		
		else	//read value of number of buckets and nextptr
		{
			
			Block bl = new Block(blocksize);
			bl.readBlock(indexfile, 0, blocksize);
			
			byte[] temp1=new byte[8];
			byte[] temp2=new byte[8];
			
			for(int f=0;f<8;f++)
			{
				temp1[f]=bl.blockdata[f];	//number of buckets
				temp2[f]=bl.blockdata[f+8];	//next pointer
			}
		 	
			numberofbuckets=toLong(temp1);
			
			////System.out.println("\n\nTemp1:" + toLong(temp1) );
			
		 	nextptr=toLong(temp2);
		}
		
		
		String hashvalue = getHashValue(entry, numberofbuckets);	//hashvalue+1 is the bucket the recordptr for the entry should be written to
		
		////System.out.println("Hash Value:" + hashvalue);
		
		Integer insertionbucket = Integer.parseInt(hashvalue, 2) + 1;
		
		Block bucket = new Block(blocksize);
		
		//System.out.println("Insertion Bucket:" + insertionbucket + "\tNumber of buckets:" + numberofbuckets);
		
		while(insertionbucket>numberofbuckets)
		{
			hashvalue = hashvalue.substring(1);
			insertionbucket = Integer.parseInt(hashvalue, 2) + 1;
		}
		
		bucket.readBlock(indexfile, insertionbucket, blocksize);
		
		boolean nooverflow = false;
		
		for(int i=0;i<blockspace-sectionsize;i=i+sectionsize)		//check each section in block if it is free
		{
			byte[] temparr = new byte[8];
			 
			for(int j=0;j<8;j++)		//each a section for its data
			{
				temparr[j] = bucket.blockdata[i+j]; 
			}
			
			long l = toLong(temparr);	//data of a section in long
						
			if(l==0)	//if data is 0, write to this section
			{
				//System.out.println("Space found in bucket at offset:" + i);
				
				byte[] recordptrarray = toByta(recordptr);
				for(int k=0;k<8;k++)			//write a long pointer to start of the section
				{
					bucket.blockdata[i+k] = recordptrarray[k];
				}
				
				byte[] dataarr = conversion_function[handlertype].GeByteArr(entry, sizeofentry);
				
				for(int k=0;k<sizeofentry;k++)
				{
					bucket.blockdata[i+8+k] = dataarr[k];
				}
				
				nooverflow = true;
				break;
			}
		}
		
		if(nooverflow == true)	//no overflow
		{
			bucket.writeBlock(indexfile, insertionbucket, blocksize);
			
			//System.out.println("No overflow! Bucket written:" + insertionbucket);
		}
		
		else	//overflow handling
		{
			byte[] temp = new byte[8];		//retrieve overflow pointer of the main bucket
			
			for(int i=0;i<8;i++)
			{
				temp[i] = bucket.blockdata[(blockspace-sectionsize) + i];
			}
			long overflowptr = toLong(temp);
			
			if(overflowptr!=0)// overflow bucket pointer is not null
			{
				insertOverflow(overflowfile, overflowptr, recordptr, entry, sizeofentry, handlertype, sectionsize);
				
				//System.out.println("Overflow goes into bucket:" + overflowptr);
			}
			else	//if main bucket is full but overflow is not yet created
			{
				byte[] newoverflowptr=insertOverflow(overflowfile, recordptr, entry, sizeofentry, handlertype, sectionsize);
				
				//System.out.println("New Overflow Bucket! Overflow goes into bucket:" + toLong(newoverflowptr));
				
				int i=blockspace-sectionsize;
				
				for(int k=0;k<8;k++)	//write overflow pointer in last section
				{
					bucket.blockdata[i+k] = newoverflowptr[k];
				}
				
				bucket.writeBlock(indexfile, insertionbucket, blocksize);
				
				//System.out.println("Overflow bucket linked to Main Bucket Number:" + insertionbucket);
			}
		
			//trigger a split
			if(splitallowed = true)
			{
				//System.out.println("------------------------Triggering a split------------------------");
				
				split(HeapFile, myMap, indexfile, overflowfile, columnnumber, args, sizeofentry, sectionsize, handlertype);
			}
			
		}
	
	}
	
	public static void split(File HeapFile, ArrayList<String> myMap, File indexfile, File overflowfile, int columnnumber, String[] args, int sizeofentry, int sectionsize, int handlertype) throws IOException, InterruptedException
	{
		Block indexheader = new Block(blocksize);
		indexheader.readBlock(indexfile, 0, blocksize);
		
		byte[] temp1=new byte[8];		//read index file info
		byte[] temp2=new byte[8];
		byte[] temp3=new byte[8];
		byte[] temp4=new byte[8];
		
		for(int f=0;f<8;f++)
		{
			temp1[f]=indexheader.blockdata[f];	//number of buckets
			temp2[f]=indexheader.blockdata[f+8];	//next pointer
			temp3[f]=indexheader.blockdata[f+16];	//end of round
		}
	 	
		long numberofbuckets=toLong(temp1);
		
		long nextptr=toLong(temp2);
		
		long endofround=toLong(temp3);
		
		ArrayList<Section> records = getrecordsptrs(indexfile, overflowfile, nextptr, sizeofentry, sectionsize, handlertype);		//nextptr is the pointer to the bucket to be split
		
		//System.out.println("\nRecord Pointers being split:");
		
		Iterator itra = records.iterator();
		
		while(itra.hasNext())
		{
			Section sect = (Section) itra.next();
			//System.out.print("\t'"+sect.recordptr + "'");
		}
		
		//increment next pointer
		
		if(nextptr == endofround)
		{
			nextptr = 1;
			endofround = endofround * 2;
			numberofbuckets = numberofbuckets+1;
		}
		else
		{
			nextptr = nextptr + 1;
			numberofbuckets = numberofbuckets+1;
		}
		
		//update header of hash file
		indexheader.insertData(toByta(numberofbuckets), 0);
		indexheader.insertData(toByta(nextptr), 8);
		indexheader.insertData(toByta(endofround), 16);
		
		indexheader.writeBlock(indexfile, 0, blocksize);
				
		Block bl = new Block(blocksize);
		
		//System.out.println("\nNew bucket added to hash...New Header Information:");
		//System.out.println("Number of Buckets:" + numberofbuckets);
		//System.out.println("Next Pointer:" + nextptr);
		//System.out.println("End of Round:" + endofround);
		
		bl.writeBlock(indexfile, (int)numberofbuckets, blocksize);	//create new empty bucket
		
		//System.out.println("\nNew Bucket Created:" + numberofbuckets);
		
		//re-hash all RID's in arraylist recordptrs
		
		Iterator<Section> record = records.iterator();
		
		while(record.hasNext())
		{
			Section tempsection = record.next();
			
			long address = tempsection.recordptr;
			
			String entry = tempsection.data;
			
			//System.out.println("Temp Section Data:" + entry);
			
			RandomAccessFile rf = new RandomAccessFile(HeapFile, "rw");
			
			
			
			//code to fetch 'columnnumber' entry from record with 'address'
			
			DataTypeHandlerInterface[] read_function = new DataTypeHandlerInterface[7];
			read_function[0] = new DataTypeHandler_i1();
			read_function[1] = new DataTypeHandler_i2();
			read_function[2] = new DataTypeHandler_i4();
			read_function[3] = new DataTypeHandler_i8();
			read_function[4] = new DataTypeHandler_r4();
			read_function[5] = new DataTypeHandler_r8();
			read_function[6] = new DataTypeHandler_cx();
			
			ArrayList<Integer[]> hfMap = OutputModeClass.getSchema(rf, myMap, args);
			
			Iterator<Integer[]> x = hfMap.iterator();		//Heap File Schema Map Iterator, to find size of the entry
			
			long offset = 0;
			long size = 0;
			int type = 0;
			
			for(int i=1;i<=columnnumber;i++)
			{
				Integer[] arr = x.next();
				
				if(i==columnnumber)
				{
					type = arr[0];
					size = arr[1];
				}
				else
				{
					offset = offset + arr[1];
				}
			}
			
			//System.out.println("Type:" + type + "\tSize:" + size + "\tAddress:" + address + "\tOffset:" + offset);
			
			//size of data in column is (int)size
			
			insertEntry(HeapFile, address, entry, (int)size, type, columnnumber, false, myMap, args);
			
			rf.close();
		}
		
	}
	
	public static ArrayList<Section> getrecordsptrs(File indexfile, File overflowfile, long nextptr, int sizeofentry, int sectionsize, int handlertype) throws IOException
	{
		DataTypeHandlerInterface[] conversion_function = new DataTypeHandlerInterface[7];
		conversion_function[0] = new DataTypeHandler_i1();
		conversion_function[1] = new DataTypeHandler_i2();
		conversion_function[2] = new DataTypeHandler_i4();
		conversion_function[3] = new DataTypeHandler_i8();
		conversion_function[4] = new DataTypeHandler_r4();
		conversion_function[5] = new DataTypeHandler_r8();
		conversion_function[6] = new DataTypeHandler_cx();
		
		ArrayList<Section> records = new ArrayList<Section>();
		
		Block bl = new Block(blocksize);
		
		Block newoverflowheader = new Block(blocksize);
		
		newoverflowheader.readBlock(overflowfile, 0, blocksize);
		
		byte[] oldfreeptr = new byte[8];
		
		for(int k=0;k<8;k++)
		{
			oldfreeptr[k] = newoverflowheader.blockdata[blockspace-sectionsize+k];
		}
		
		Block newhashheader = new Block(blocksize);
		
		newhashheader.readBlock(indexfile, 0, blocksize);
		
		bl.readBlock(indexfile, (int)nextptr, blocksize);	//read from main file
		
		for(int k=0;k<blockspace-sectionsize;k=k+sectionsize)
		{
			byte[] dataptr = new byte[8];
			
			byte[] data = new byte[sizeofentry];
			
			for(int j=0;j<8;j++)		//read rid pointer
			{
				dataptr[j] = bl.blockdata[k+j];
			}
			
			Long l = toLong(dataptr);
			
			for(int j=0;j<sizeofentry;j++)		//read entry
			{
				data[j] = bl.blockdata[8+k+j];
			}
			
			String datastring = conversion_function[handlertype].fromByteArr(data);
			
			Section section = new Section(l, datastring);
			
			if(l!=0)
			{
				records.add(section);
			}
		}
		
		byte b[] = new byte[8];
		
		for(int j=0;j<8;j++)
		{
			b[j] = bl.blockdata[blockspace-sectionsize+j];
		}
		
		long overflowptr = toLong(b);
		
		//if overflowptr is not 0, freelist in overflow file should start with this overflowptr
		
		if(overflowptr!=0)
		{
			newoverflowheader.insertData(toByta(overflowptr), 0);
			newoverflowheader.writeBlock(overflowfile, 0, blocksize);		//freelist starts with this list's 1st overflow bucket now
		}
		
		long tempptr = overflowptr;
		
		while(overflowptr!=0)		//read overflow buckets till last bucket in chain
		{
			bl.readBlock(overflowfile, (int)overflowptr, blocksize);
			
			for(int k=0;k<blockspace-sectionsize;k=k+sectionsize)
			{
				byte[] dataptr = new byte[8];
				
				byte[] data = new byte[sizeofentry];
				
				for(int j=0;j<8;j++)
				{
					dataptr[j] = bl.blockdata[k+j];
				}
				
				Long l = toLong(dataptr);
				
				for(int j=0;j<sizeofentry;j++)
				{
					data[j] = bl.blockdata[8+j];
				}
				
				String datastring = conversion_function[handlertype].fromByteArr(data);
				
				Section section = new Section(l, datastring);
				
				if(l!=0)
				{
					records.add(section);
				}
			}
			
			for(int j=0;j<8;j++)
			{
				b[j] = bl.blockdata[blockspace-sectionsize+j];
			}
			
			tempptr = overflowptr;	//last bucket in chain, will connect it to old starting bucket of free list
			
			overflowptr = toLong(b);
		}
		
		if(tempptr!=0)
		{
			bl = new Block(blocksize);
			bl.readBlock(overflowfile, (int)tempptr, blocksize);
			bl.insertData(oldfreeptr, blockspace-sectionsize);
			bl.writeBlock(overflowfile, (int)tempptr, blocksize);
		}
		
		bl = new Block(blocksize);		//empty the main bucket
		
		bl.writeBlock(indexfile, (int)nextptr, blocksize);
		
		return records;
	}
	
	public static ArrayList<Long> getqualifyingrecordids(File indexfile, File overflowfile, String compvalue, int columnnumber, ArrayList<Integer[]> hfMap) throws IOException
	{
		
		Block getheaderinfo = new Block(32);
		
		getheaderinfo.readBlock(indexfile, 0, 32);
		
		byte[] blocksizeinfo = new byte[8];
		
		for(int k=24;k<32;k++)
		{
			blocksizeinfo[k-24] = getheaderinfo.blockdata[k];
		}
		
		blockspace = (int) toLong(blocksizeinfo);		//get effective block size
		
		ArrayList<Long> recordptrs = new ArrayList<Long>();
		
		Block hashheader = new Block(blocksize);
		
		hashheader.readBlock(indexfile, 0, blocksize);
		
		byte[] arr = new byte[8];
		
		for(int k=0;k<8;k++)		//numberofbuckets
		{
			arr[k] = hashheader.blockdata[k];
		}
		
		long numberofbuckets = toLong(arr);
		
		String hashvalue = getHashValue(compvalue, numberofbuckets);
		
		long bucketptr = (Integer.parseInt(hashvalue, 2) + 1);
		
		
		//------------------------------
		Iterator<Integer[]> x = hfMap.iterator();		//Heap File Schema Map Iterator, to find size of the entry
		
		long offset = 0;
		long size = 0;
		int type = 0;
		
		for(int i=1;i<=columnnumber;i++)
		{
			Integer[] array = x.next();
			
			if(i==columnnumber)
			{
				type = array[0];
				size = array[1];
			}
			else
			{
				offset = offset + array[1];
			}
		}
		
		////System.out.println("\tSize:" + size + "\tAddress:" + bucketptr);
		//------------------------------
		
		int sectionsize = (int) (8 + size);
		
		Block bl = new Block(blocksize);
		
		bl.readBlock(indexfile, (int)bucketptr, blocksize);	//read required bucket from main file
		
		for(int k=0;k<blockspace-sectionsize;k=k+sectionsize)
		{
			byte[] b = new byte[8];
			
			for(int j=0;j<8;j++)
			{
				b[j] = bl.blockdata[k+j];
			}
			
			Long l = toLong(b);
			if(l!=0)
			{
				recordptrs.add(l);
			}
		}
		
		byte b[] = new byte[8];
		
		for(int j=0;j<8;j++)
		{
			b[j] = bl.blockdata[blockspace-sectionsize+j];
		}
		
		long overflowptr = toLong(b);
		
		//if overflowptr is not 0
		
		while(overflowptr!=0)		//read overflow buckets till last bucket in chain
		{
			bl.readBlock(overflowfile, (int)overflowptr, blocksize);
			
			for(int k=0;k<blockspace-sectionsize;k=k+sectionsize)
			{
				b = new byte[8];
				
				for(int j=0;j<8;j++)
				{
					b[j] = bl.blockdata[k+j];
				}
				
				Long l = toLong(b);
				if(l!=0)
				{
					recordptrs.add(l);
				}
			}
			
			for(int j=0;j<8;j++)
			{
				b[j] = bl.blockdata[blockspace-sectionsize+j];
			}
			
			overflowptr = toLong(b);
		}
		
		return recordptrs;
	}
	
	public static String getHashValue(String s, long numberofbuckets)
	{
		byte[] bytes = s.getBytes();
		StringBuilder binary = new StringBuilder();
		
		for (byte b : bytes)
		{
			int val = b;
			
			for (int i = 0; i < 8; i++)
			{
				binary.append((val & 128) == 0 ? 0 : 1);
				val <<= 1;
			}
			
			//binary.append(' ');
		}
		
		//System.out.println("\n'" + s + "' to binary: " + binary);
	 
		long hashbits = 1;
		
		if(numberofbuckets!=1)
		{
			hashbits = (long)	( Math.ceil( Math.log((double)numberofbuckets) / Math.log(2) ) 	);
		}
		
		//System.out.println("\nNumber of buckets:" + numberofbuckets + "\tHash Bits:" + hashbits);
		
//		////System.out.println("\nLength of String Builder: "+binary.length());
		
		String hashvalue = new String( binary );
		
		StringBuilder tempBuilder = new StringBuilder("0");
		
		if( hashvalue.length() < hashbits )
		{
			for(	int x=1; x<(hashbits - hashvalue.length())	; x++)
			{
				tempBuilder.append("0");
			}
			
			String tempString = new String(tempBuilder);
			
			////System.out.println("Appending dummy '0' bits to beginning of hash value:" + tempString.length());
			
			hashvalue = tempString.concat(hashvalue);	//add needed number of 0's to beginning of hash value if number of bits in hash value is smaller than needed
		}
		
		hashvalue = hashvalue.substring( (int) (hashvalue.length() - hashbits) );
		
		Integer insertionbucket = Integer.parseInt(hashvalue, 2) + 1;
		
		while(insertionbucket > numberofbuckets)
		{
			hashvalue = hashvalue.substring(hashvalue.length() - 1);
			
			insertionbucket = Integer.parseInt(hashvalue, 2) + 1;
			
			//System.out.println("\nHash Value:" + hashvalue);
		}
		
		return hashvalue;	//get the last 'hashbits' number of bits from the binary representation
	}
	
//	public static byte[] toByta(int i)
//	{
//		return new byte[] {
//				(byte)((i >> 24) & 0xff),
//				(byte)((i >> 16) & 0xff),
//				(byte)((i >> 8) & 0xff),
//				(byte)((i >> 0) & 0xff),
//				};
//	}
	
	public static byte[] toByta(long data)
	{
		return new byte[] {
		(byte)((data >> 56) & 0xff),
		(byte)((data >> 48) & 0xff),
		(byte)((data >> 40) & 0xff),
		(byte)((data >> 32) & 0xff),
		(byte)((data >> 24) & 0xff),
		(byte)((data >> 16) & 0xff),
		(byte)((data >> 8) & 0xff),
		(byte)((data >> 0) & 0xff),
		};
	}
	
	public static long toLong(byte[] data)
	{
		if (data == null || data.length != 8) return 0x0;
		// ----------
		return (long)(
		// (Below) convert to longs before shift because digits
		// are lost with ints beyond the 32-bit limit
		(long)(0xff & data[0]) << 56 |
		(long)(0xff & data[1]) << 48 |
		(long)(0xff & data[2]) << 40 |
		(long)(0xff & data[3]) << 32 |
		(long)(0xff & data[4]) << 24 |
		(long)(0xff & data[5]) << 16 |
		(long)(0xff & data[6]) << 8 |
		(long)(0xff & data[7]) << 0
		);
	}
	
	public static byte[] insertOverflow(File overflowfile, long recordptr, String entry, int sizeofentry, int handlertype, int sectionsize) throws IOException
	{
		DataTypeHandlerInterface[] conversion_function = new DataTypeHandlerInterface[7];
		conversion_function[0] = new DataTypeHandler_i1();
		conversion_function[1] = new DataTypeHandler_i2();
		conversion_function[2] = new DataTypeHandler_i4();
		conversion_function[3] = new DataTypeHandler_i8();
		conversion_function[4] = new DataTypeHandler_r4();
		conversion_function[5] = new DataTypeHandler_r8();
		conversion_function[6] = new DataTypeHandler_cx();
		
		Block headerblock = new Block(blocksize);
		
		headerblock.readBlock(overflowfile, 0, blocksize);
		
		byte[] freebucketaddarr=new byte[8];
				
		for(int f=0;f<8;f++)		//read header of overflow file for the free bucket pointer
		{
			freebucketaddarr[f]=headerblock.blockdata[f];		//free pointer
		}
		
		////System.out.println("Address of first free bucket:" + toLong(freebucketaddarr));
		
		long freebucketaddress = toLong(freebucketaddarr);
		boolean nofreebucket = false;
		//code to check if freepointer !=0, if it is 0, allocate one bucket at end of overflow file
		
		if(freebucketaddress == 0)		//if no free buckets, allocate free pointer at end of file
		{
			freebucketaddress = overflowfile.length()/blocksize;
			freebucketaddarr = toByta(freebucketaddress);
			nofreebucket = true;
		}
		
		//System.out.println("Address of first free bucket:" + toLong(freebucketaddarr));
		
		Block bucket = new Block(blocksize);
		
		if(nofreebucket=false)
		{
			bucket.readBlock(overflowfile, (int)(toLong(freebucketaddarr)), blocksize);	//fetch empty bucket
		}
		
		byte[] nextfreebucket = toByta( Long.parseLong("0") );
		
		for(int k=0;k<8;k++)
		{
			nextfreebucket[k] = bucket.blockdata[(blockspace-sectionsize)+k];
		}
		
		//System.out.println("Next free bucket:" + toLong(nextfreebucket));
		
		headerblock.insertData(nextfreebucket, 0);
		
		headerblock.writeBlock(overflowfile, 0, blocksize);		//write new header(with updated free pointer) to overflow file
		
		//insert code to write nextfreebucket to header

		// adding the recordptr and data to the newly used bucket
		byte[] dataptr = toByta(recordptr);
		
		bucket.insertData(dataptr, 0);
		
		byte[] data = conversion_function[handlertype].GeByteArr(entry, sizeofentry);
		
		bucket.insertData(data, 8);
		
		for(int k=sectionsize;k<=blockspace-sectionsize;k++)		//cleaning of empty bucket starts at offset 8 before it is used
		{
			bucket.insertData(toByta(0), k);		//clean remaining parts of free bucket that is being used
		}
		
		bucket.writeBlock(overflowfile,  (int)freebucketaddress, blocksize);
		
		//System.out.println("Address of new overflow bucket:" + toLong(freebucketaddarr));
		
		return freebucketaddarr;		//return address to be written to parent bucket's next pointer 
		
	}
	
	
	public static void insertOverflow(File overflowfile, long overflowptr, long recordptr ,String entry, int sizeofentry, int handlertype, int sectionsize) throws IOException
	{
		DataTypeHandlerInterface[] conversion_function = new DataTypeHandlerInterface[7];
		conversion_function[0] = new DataTypeHandler_i1();
		conversion_function[1] = new DataTypeHandler_i2();
		conversion_function[2] = new DataTypeHandler_i4();
		conversion_function[3] = new DataTypeHandler_i8();
		conversion_function[4] = new DataTypeHandler_r4();
		conversion_function[5] = new DataTypeHandler_r8();
		conversion_function[6] = new DataTypeHandler_cx();
		
		Block overflowbucket = new Block(blocksize);
		
		int insertionbucket = (int)overflowptr;
		
		overflowbucket.readBlock(overflowfile, insertionbucket, blocksize); 	//read overflow bucket
		
		boolean insertionpossible = false;
		
		for(int i=0;i<blockspace-sectionsize;i=i+sectionsize)		//check each section in overflow block if it is free
		{
			byte[] temparr = new byte[8];
			 
			for(int j=0;j<8;j++)		//each a section for its record pointer
			{
				temparr[j] = overflowbucket.blockdata[i+j]; 
			}
			
			long l = toLong(temparr);	//data of a section in long
			
			////System.out.println("\nLong l:" + l);
			
			if(l==0)	//if data is 0, write to this section
			{
				//System.out.println("Space found in bucket at offset:" + i);
				
				// adding the recordptr and data to the overflow bucket
				byte[] dataptr = toByta(recordptr);
				
				overflowbucket.insertData(dataptr, i);
				
				byte[] data = conversion_function[handlertype].GeByteArr(entry, sizeofentry);
				
				overflowbucket.insertData(data, i+8);
				
				//System.out.println("Record Pointer Written:" + toLong(dataptr));
				insertionpossible = true;	//set flag true if insertion is possible
				break;
			}
		}
		
		if(insertionpossible == true)	//no overflow
		{
			overflowbucket.writeBlock(overflowfile, (int)(overflowptr), blocksize);
		}
		else	//overflow handling
		{
			byte[] temp = new byte[8];		//retrieve overflow pointer of this bucket
			
			for(int i=0;i<8;i++)
			{
				temp[i] = overflowbucket.blockdata[(blockspace-sectionsize) + i];
			}
			
			overflowptr = toLong(temp);
			
			if(overflowptr!=0)// overflow bucket pointer is not null
			{
				insertOverflow(overflowfile, overflowptr, recordptr, entry, sizeofentry, handlertype, sectionsize);
			}
			else	//if main bucket is full but overflow is not yet created
			{
				byte[] newoverflowptr=insertOverflow(overflowfile, recordptr, entry, sizeofentry, handlertype, sectionsize);
				
				int i=blockspace-sectionsize;
				
				for(int k=0;k<8;k++)		//write new overflow pointer
				{
					overflowbucket.blockdata[i+k] = newoverflowptr[k];
				}
				
				overflowbucket.writeBlock(overflowfile, insertionbucket, blocksize);
			}	
		}
	}
		
}