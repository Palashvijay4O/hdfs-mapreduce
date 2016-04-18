import com.example.assignment.assignment.*;
import com.google.protobuf.ByteString;
import java.rmi.Naming;
import java.util.*;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import java.io.*;
 
public class StartFileClient {
 
	public static void main(String[] args) {
		try{
			String name = "akshat";
			FileClient c = new FileClient("akshat");
			System.out.println("Ho gaya");

			Registry registry = LocateRegistry.getRegistry(args[0]);
            FileServerInt comp = (FileServerInt) registry.lookup(name);

			System.out.println("Listening.....");			
			Scanner s=new Scanner(System.in);			
			while(true)
			{
				String line=s.nextLine();
				String foo[] = new String[5];
				int count = 0;
				System.out.println(line);
				for(String i: line.split(" "))
				{
					foo[count++] = i;
				}
				if(foo[0].equals("list"))
				{
					ListFilesRequest.Builder bar = ListFilesRequest.newBuilder();
					bar.setDirName("mummy");
					byte[] ser = bar.build().toByteArray();
					byte[] results = comp.list(ser);
					try{
						ListFilesResponse des_results = ListFilesResponse.parseFrom(results);
						int cnt = des_results.getFileNamesCount();
						int n = cnt;
						while(cnt > 0)
						{
							System.out.println(des_results.getFileNames(n-cnt));
							cnt--;	
						}
						
					}
					catch(Exception e)
					{
						e.printStackTrace();
					}
				}
				else if(foo[0].equals("put"))
				{
					char[] myBuffer = new char[30];
					int bytesRead = 0;
					try{
						BufferedReader in = new BufferedReader(new FileReader(foo[1]));

						OpenFileRequest.Builder filname = OpenFileRequest.newBuilder();
						filname.setFileName(foo[1]);
						filname.setForRead(false);
						byte[] putname = filname.build().toByteArray();
						byte[] results_handle = comp.openFile(putname);
						OpenFileResponse put_results = OpenFileResponse.parseFrom(results_handle);
						byte[] ganda = new byte[10240];
						while((bytesRead = in.read(myBuffer, 0, 30)) != -1)
						{
							byte[] myByte = new byte[30];
							for(int i = 0; i< bytesRead; i++)
							{
								myByte[i] = (byte)myBuffer[i];
							}
							AssignBlockRequest.Builder filechunk = AssignBlockRequest.newBuilder();
							filechunk.setHandle(put_results.getHandle());
							byte[] sendchunk = filechunk.build().toByteArray(); 
							byte[] chunkcatch = comp.assignBlock(sendchunk);

							// UNMARSHALLING KIYA YAHAN PEIN //
							AssignBlockResponse chunkdeserial = AssignBlockResponse.parseFrom(chunkcatch);
							//System.out.println(chunkdeserial.getStatus());
							BlockLocations temp = chunkdeserial.getNewBlock();
							//System.out.println(temp.getBlockNumber());
							int cnt = temp.getLocationsCount();
							int n = cnt;
							String nnname;
							while(cnt > 0)
							{
								//System.out.println(temp.getLocations(n-cnt).getIp());
								//System.out.println(temp.getLocations(n-cnt).getPort());
								if(temp.getLocations(n-cnt).getIp().equals("10.0.0.3"))
								{
									nnname = "palashvm";
								}
								else if(temp.getLocations(n-cnt).getIp().equals("10.0.0.4"))
								{
									nnname = "jugnuvm";
								}
								else
								{
									nnname = "jugnu";
								}
								Registry registrynn = LocateRegistry.getRegistry(temp.getLocations(n-cnt).getIp());
            					DataNodeInt compnn = (DataNodeInt) registrynn.lookup(nnname);
	            				WriteBlockRequest.Builder writeblock = WriteBlockRequest.newBuilder();
								writeblock.addData(ByteString.copyFrom(myByte));

								BlockLocations.Builder blckinfo = BlockLocations.newBuilder();
								blckinfo.setBlockNumber(temp.getBlockNumber());
								DataNodeLocation.Builder nnloc = DataNodeLocation.newBuilder();
								nnloc.setIp(temp.getLocations(n-cnt).getIp());
								nnloc.setPort(temp.getLocations(n-cnt).getPort());
								blckinfo.addLocations(nnloc);
								writeblock.setBlockInfo(blckinfo);
								byte[] bhejofile = writeblock.build().toByteArray();
								ganda = compnn.writeBlock(bhejofile);
								cnt--;
							}
						}
						WriteBlockResponse wrtresponse = WriteBlockResponse.parseFrom(ganda);
						if(wrtresponse.getStatus() == 1)
						{
							System.out.println("Sab kuch sahi ho gaya");
							CloseFileRequest.Builder bandfile = CloseFileRequest.newBuilder();
							bandfile.setHandle(put_results.getHandle());
							byte[] closekarna = bandfile.build().toByteArray();
							byte[] closestatus = comp.closeFile(closekarna);

							CloseFileResponse bandresponse = CloseFileResponse.parseFrom(closestatus);
							if(bandresponse.getStatus() == 1)
							{
								System.out.println("YE HOGAYA HOGAYA");
							}

						}
						else
						{
							System.out.println("Kat gaya");
						}
					}				
					catch(Exception e){
						e.printStackTrace();
					}
				}
				else if(foo[0].equals("get"))
				{
					System.out.println("get hua");
					try
					{
						OpenFileRequest.Builder filname = OpenFileRequest.newBuilder();
						filname.setFileName(foo[1]);
						filname.setForRead(true);
						byte[] putname = filname.build().toByteArray();
						byte[] results_handle = comp.openFile(putname);

						OpenFileResponse get_results = OpenFileResponse.parseFrom(results_handle);
						int fileHandle = get_results.getHandle();
						int cnt = get_results.getBlockNumsCount();
						int n = cnt;
						BlockLocationRequest.Builder myLocation = BlockLocationRequest.newBuilder();
						while(cnt > 0)
						{
								//System.out.println(get_results.getBlockNums(n-cnt));
								myLocation.addBlockNums(get_results.getBlockNums(n-cnt));
								cnt--;
						}

						byte[] myLocations = myLocation.build().toByteArray();
						byte[] responseLocations = comp.getBlockLocations(myLocations);
						BlockLocationResponse blckLoc = BlockLocationResponse.parseFrom(responseLocations);
						BlockLocations.Builder bar = BlockLocations.newBuilder();
						//System.out.println("dygahij");
						cnt = (blckLoc.getBlockLocationsCount());
						n = cnt;
						//System.out.println(blckLoc.getBlockLocations(n-cnt).getLocationsCount());

						while(cnt > 0)
						{
							System.out.println(blckLoc.getBlockLocations(n-cnt).getBlockNumber());
							String bars = blckLoc.getBlockLocations(n-cnt).getLocations(0).getIp();
							System.out.println(bars);
							byte[] ganda = new byte[30];
							String nnname;
							if(bars.equals("10.0.0.3") == true)
							{
								nnname = "palashvm";
        					}
							else if(bars.equals("10.0.0.4") == true)
							{
								nnname = "jugnuvm";
        					}
							else
							{
								nnname = "jugnu";
							}
							Registry registrynn = LocateRegistry.getRegistry(bars);
							DataNodeInt compnn = (DataNodeInt) registrynn.lookup(nnname);
        					ReadBlockRequest.Builder readblock = ReadBlockRequest.newBuilder();
        					readblock.setBlockNumber(blckLoc.getBlockLocations(n-cnt).getBlockNumber());
							byte[] bhejofile = readblock.build().toByteArray();
							ganda = compnn.readBlock(bhejofile);

							System.out.println("This Read is is working fine!!");
							ReadBlockResponse readblocks = ReadBlockResponse.parseFrom(ganda);
							ByteString dinreadblock = ByteString.copyFrom(readblocks.getDataList());
							String fillName =  foo[1].replaceAll(".txt","_.txt");
							File data = new File(fillName);
							boolean blnCreated = false;
				     		try
				     		{
								byte[] b = "******************************".getBytes("UTF-8");
								dinreadblock.copyTo(b,0);
								String stringdata = new String(b,"UTF8");
								stringdata = stringdata.replaceAll("[^\\x20-\\x7e]", "");
					       		blnCreated = data.createNewFile();
								FileWriter fw = new FileWriter(fillName, true);
								fw.write(stringdata);
								//fw.flush();			
								fw.close();
				     		}
				     		catch(IOException ioe)
				     		{
				       			System.out.println("Error while creating a new empty file :" + ioe);
				     		}
							cnt--;
						}
					}
					catch(Exception e){
						e.printStackTrace();
					}
				}
			}
		}
			catch(Exception e){
			e.printStackTrace();
		}
	}	
}