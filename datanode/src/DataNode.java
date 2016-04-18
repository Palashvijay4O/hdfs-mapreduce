import com.example.assignment.assignment.*;

import com.google.protobuf.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.rmi.*;
import java.util.*;
import java.io.*;
 
 
public class DataNode implements DataNodeInt {
	
	public String file = "";
	
	public DataNode() throws RemoteException {
		super();
	}
 
	public void setFile(String f){
		file = f;
	}
	
	public byte[] writeBlock(byte[] inp) throws RemoteException {
		try{		
				System.out.println("This is working fine!!");
				WriteBlockRequest writeblocks = WriteBlockRequest.parseFrom(inp);
			
				ByteString dinwriteblock = ByteString.copyFrom(writeblocks.getDataList());
				
				BlockLocations getinfolistofnn = writeblocks.getBlockInfo();

				int chunkoffileno = getinfolistofnn.getBlockNumber();
				String blocknaam = Integer.toString(chunkoffileno) + ".txt";
				File data = new File(blocknaam);
				boolean blnCreated = false;

	     		try{
					byte[] b = "******************************".getBytes("UTF-8");
					dinwriteblock.copyTo(b,0);
					String stringdata = new String(b,"UTF8");
					stringdata = stringdata.replaceAll("[^\\x20-\\x7e]", "");
		       		blnCreated = data.createNewFile();
					FileWriter fw = new FileWriter(blocknaam,true);
					fw.write(stringdata);
					//fw.flush();			
					fw.close();
					
					FileWriter configwriter = new FileWriter("DataNodeConfig",true);
					configwriter.write("\n");
					configwriter.write(Integer.toString(chunkoffileno));
					configwriter.close();
	     		}
	     		catch(IOException ioe){
	       			System.out.println("Error while creating a new empty file :" + ioe);
	     		}

				int forwardToCount = getinfolistofnn.getLocationsCount();
				int n = forwardToCount;
				while(forwardToCount > 0)
				{
					//System.out.println(getinfolistofnn.getLocations(n-forwardToCount).getIp());
					//System.out.println(getinfolistofnn.getLocations(n-forwardToCount).getPort());
					forwardToCount--;
				}
			
				// @TODO Open and create the file with name of blocknumber coming
				WriteBlockResponse.Builder sendstatus = WriteBlockResponse.newBuilder();
				byte[] returnResponse = sendstatus.setStatus(1).build().toByteArray();
				return returnResponse;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return inp;
	}

	public byte[] readBlock(byte[] inp) throws RemoteException {
	try{
			ReadBlockRequest deser = ReadBlockRequest.parseFrom(inp);
			int recBlocknum = deser.getBlockNumber();
			String blocknaam = Integer.toString(recBlocknum) + ".txt";
			char[] myBuffer = new char[30];
			int bytesRead = 0;
			//try{
			BufferedReader in = new BufferedReader(new FileReader(blocknaam));
			byte[] myByte = new byte[30];		
			while((bytesRead = in.read(myBuffer, 0, 30)) != -1)
			{
				for(int i = 0; i< bytesRead; i++)
				{
					myByte[i] = (byte)myBuffer[i];
				}
			}

			ReadBlockResponse.Builder datacollect = ReadBlockResponse.newBuilder();
			datacollect.setStatus(1);
			datacollect.addData(ByteString.copyFrom(myByte));
			byte[] returner = datacollect.build().toByteArray();
			return returner; 
		}
	catch(Exception e) {
			e.printStackTrace();
		}
	return inp;
	}

}
