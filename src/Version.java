import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;

/*
	REQUIRES JAVA 7
*/

class Version {
	public static HashMap<String, ArrayList<String>> versionTable = new HashMap<String, ArrayList<String>>();
	public final static String versionFilePath = "..\\versions\\";
	
	public static void init() {
		try{
			readVersionInfo();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public static void checkpoint(String ver) throws IOException{	
		ArrayList<String> chkpts = versionTable.get(DB.currentFileName);
		try{
			if(!chkpts.contains(ver)){
				chkpts.add(ver);
				File sourceDir = new File(DB.dataFilePath);
				File sinkDir;
				File[] directoryListing = sourceDir.listFiles();
				if (directoryListing != null) {
					String original =  DB.currentFileName+".?";
					String regex = original.replace("?", ".*?");
					String newName = "";
					File path;
					for (File child : directoryListing) {
						// Do something with child
						if(child.getName().matches(regex)){
							newName = ver+"."+child.getName();
							sinkDir = new File(versionFilePath+newName);
							Files.copy(child.toPath(), sinkDir.toPath(), StandardCopyOption.REPLACE_EXISTING);
						}
					}
					System.out.println("Version "+ver+" written to backup.");
				}
			} else {
				System.out.println("Version "+ver+" already exists.");
			}
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public static void rollback(String ver) throws IOException{
		ArrayList<String> chkpts = versionTable.get(DB.currentFileName);
		try{
			if(chkpts.contains(ver)){
				File sourceDir = new File(versionFilePath);
				File sinkDir;
				File[] directoryListing = sourceDir.listFiles();
				if (directoryListing != null) {
					String original =  ver+"."+DB.currentFileName+".?";
					String regex = original.replace("?", ".*?");
					String newName = "";
					File path;
					for (File child : directoryListing) {
						// Do something with child
						if(child.getName().matches(regex)){
							newName = child.getName().replace(ver+".","");
							sinkDir = new File(DB.dataFilePath+newName);
							Files.copy(child.toPath(), sinkDir.toPath(), StandardCopyOption.REPLACE_EXISTING);
						}
					}
					System.out.println("Version "+ver+" copied from backup.");
				}
			} else {
				System.out.println("Version "+ver+" doesn't exist.");
			}
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public static void delete(String ver) throws IOException{
		ArrayList<String> chkpts = versionTable.get(DB.currentFileName);
		try{
			if(chkpts.contains(ver)){
				File sourceDir = new File(versionFilePath);
				File[] directoryListing = sourceDir.listFiles();
				if (directoryListing != null) {
					String original =  ver+"."+DB.currentFileName+".?";
					String regex = original.replace("?", ".*?");
					for (File child : directoryListing) {
						// Do something with child
						if(child.getName().matches(regex)){
							//Files.deleteIfExists(child.toPath());
							child.delete();
						}
					}
					System.out.println("Version "+ver+" deleted from backup.");
					chkpts.remove(ver);
					writeVersionInfo();
				}
			} else {
				System.out.println("Version "+ver+" doesn't exists.");
			}
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public static void listCheckpoints(){
		if(versionTable.containsKey(DB.currentFileName)){
			ArrayList<String> checkpoints = versionTable.get(DB.currentFileName);
			for(String ck:checkpoints){
				System.out.println(ck);
			}
		} else {
			System.out.println(DB.currentFileName+" has no backup versions.");
		}
	}
	
	public static void readVersionInfo()throws IOException {
		String fullPath = versionFilePath + DB.currentFileName +".versionInfo";
		try{
			if( !(new File(fullPath).isFile()) ) {
				FileOutputStream fos = new FileOutputStream(fullPath);
				ObjectOutputStream oos = new ObjectOutputStream(fos);
				try{
					oos.writeObject(new HashMap<String, ArrayList<String>>());
					oos.close();
					fos.close();
				}catch(IOException e){
					e.printStackTrace();
				}
			}
			FileInputStream fis = new FileInputStream(fullPath);
			ObjectInputStream ois = new ObjectInputStream(fis);
			try {
				versionTable = (HashMap<String, ArrayList<String>>) ois.readObject();
				if(!versionTable.containsKey(DB.currentFileName)){
					versionTable.put(DB.currentFileName, new ArrayList<String>());
				}
			} catch (ClassNotFoundException cnfe) {
				cnfe.printStackTrace();
			}
			ois.close();
			fis.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public static void writeVersionInfo()throws IOException {
		String fullPath = versionFilePath + DB.currentFileName +".versionInfo";
		FileOutputStream fos = new FileOutputStream(fullPath);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		try{
			oos.writeObject(versionTable);
			oos.close();
			fos.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
}