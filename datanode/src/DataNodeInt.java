import java.rmi.*;
 
public interface DataNodeInt extends Remote{
 
	public byte[] writeBlock(byte[] inp) throws RemoteException;		
	public byte[] readBlock(byte[] inp) throws RemoteException;
}
