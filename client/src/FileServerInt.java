import java.rmi.*;
 
public interface FileServerInt extends Remote{
 
		byte[] list(byte[] inp ) throws RemoteException;
		byte[] openFile(byte[] inp) throws RemoteException;
		byte[] assignBlock(byte[] inp ) throws RemoteException;
		byte[] getBlockLocations(byte[] inp ) throws RemoteException;
		byte[] closeFile(byte[] inp ) throws RemoteException;
}	