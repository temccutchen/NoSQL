import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

// version 3

class DB {
	public final static String fileLabel = "fileName", userLabel = "user";
	public final static String dataFilePath = "..\\files\\";
	public final static String DBSumFilePath = "..\\filesAttributes\\";
	public final static String DBSumFileName = "DBSumFile";
	public static HashMap<String,HashMap<String, String>> DBSumFileTable = new HashMap<String,HashMap<String, String>>();
	public final static String DBUserFilePath = "..\\users\\";
	public final static String DBUserFileName = "DBUserFile";
	public static HashMap<String,HashMap<String, String>> DBUserFileTable = new HashMap<String,HashMap<String, String>>();
	public static String owner, currentUser, user, currentFileName, visible, modifiable, limitedAccess,highestKey,record_num;
	public static DataFile newDataFile; // =  new DataFile(currentFileName);
	public static boolean safe=false; // exit or switch file, need this boolean to change to true
	public static int recordsPerPartition = 50; // number of records per partition file


	//verify user
	public static boolean userExists(String user) throws IOException{
		readSumFile(DBUserFileName, DBUserFilePath,  DBUserFileTable);
		if(DBUserFileTable.containsKey(user)){
			return true;
		}
		//System.out.println("The user " + user + " does not exist");
		return false;
	}
	//verfify user and user's password
	public static boolean userVerified(String user, String pass) throws IOException{
		if(userExists(user)&&DBUserFileTable.get(user).get("passWord").equals(pass)){
			return true;
		}
		System.out.println("The passWord does not match");
		return false;
	}

	//to use a current file to insert or search...., the user will be first verified
	public static void useFile(String fileName, String sumFile, String dir,HashMap<String,HashMap<String, String>> table) throws IOException{
		if(getFileNameFromDBSumFile(fileName, sumFile, dir, table)==true){
			//verify the user is listed for this file
			if(DBSumFileTable.get(fileName).get("user").equals("null")|| DB.DBSumFileTable.get(fileName).get("user").contains(currentUser)){
				//System.out.println("The user " + currentUser + " is allowed to use the file " + fileName);
				System.out.println("Now use file " + fileName);
				setCurrentFileName(fileName);
				newDataFile =  new DataFile(fileName);//yilongs, 04/12
			}else{
				System.out.println("The user " + currentUser + " is not allowed to use the file " + fileName);
				return;
			}
		}else{
			System.out.println("The file " + fileName + " does not exist");
			return;
		}
	}

	//save All the filenames plus attributes in the DBFilesTable to a single file "filesTable"
	//after any file updated or a new file added, all the contents need to be re-wrote to the file again
	//this method only create a file name with certain attributes in the DBSumfile. To create the real data file, you need to use this file and write the data into this file.
	public static void createNewFile(String label, String singleFile, String sumFile, String filePath, HashMap<String,HashMap<String, String>> table) throws IOException{
		if(getFileNameFromDBSumFile(singleFile,sumFile,filePath,table)==true){
			System.out.println("The file " + singleFile + " already exisits");
			return;
		}

		readSumFile(sumFile, filePath, table);
		setOwner(currentUser);
		addUser(currentUser);
		//setLimitedAccess("Y");
		table.put(singleFile,createFileSecurityInfo()); //owner, visible, modifiable, user, limitedAccess
		writeSumFile(label, sumFile, filePath, table);
	}

	//create user account which can be used to log in or operate files
	public static void createNewUser(String label, String singleFile, String sumFile, String filePath, HashMap<String,HashMap<String, String>> table) throws IOException{
		readSumFile(sumFile, filePath, table);
		table.put(singleFile,setDefaultUserInfo());
		writeSumFile(label, sumFile, filePath, table);
	}

	//update files(users)' attributes, if old, update the value, if not exist, add as new attribute====>then writeback to the file immediately
	public static void updateInfo(String label, String file, String att, String value, String sumFile, String filePath, HashMap<String,HashMap<String, String>> table) throws IOException{
		readSumFile(sumFile,filePath,table);
		if(table.containsKey(file)){
			table.get(file).put(att, value);
			writeSumFile(label, sumFile, filePath, table); //write to the file to update it
		}else{
			System.out.println("The " + label + " does not exist");
			return;
		}
	}


	public static void deleteRecord(String label,String singleFile,String sumFile, String filePath, HashMap<String,HashMap<String, String>> table ) throws IOException{
		readSumFile(sumFile, filePath,table);
		if(table.isEmpty() || !table.containsKey(singleFile)){
			System.out.println("There is no " + label + " exsisting in the database for deletion");
			return;
		}else{
			table.remove(singleFile);
			writeSumFile(label, sumFile, filePath,table);
		}
	}

	//write all the contents in DBFilesTable out to a file
	public static void writeSumFile(String label,String outFile,String dir, HashMap<String,HashMap<String, String>> table)throws IOException {
		File out_File = new File(dir+outFile);
		try{
			if(!out_File.exists()){
	 			out_File.createNewFile();
	 		}
			//true = append file
			FileWriter fileWritter = new FileWriter(out_File.getPath(),false); //must use getAbsolutepath() to write to the right folder
			//if use "new FileWriter(out_File.getName(),false)" ---the real data will be write to relative path(java src folder) and an empty file will be in the target folder
			BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
			if(!table.isEmpty()&&table !=null){
				Set<String> file_keys = table.keySet();
				for(String i : file_keys){
					HashMap<String,String> each_map=table.get(i);
					Set<String> map_keys = each_map.keySet();
					StringBuilder line = new StringBuilder();
					line.append(label +"="+i+";	 "); //file name
					for(String key : map_keys){
						line.append(key+"="+each_map.get(key)+";	 ");
					}
					bufferWritter.write(line.toString());
					//write to next line!
					bufferWritter.newLine();
				}
			}
			bufferWritter.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}

//read the saved filesTable into the hashTable
	public static void  readSumFile(String sumFile,String dir,HashMap<String,HashMap<String, String>> table)throws IOException {
		table.clear();
		//to build the file path automatically
		File in_file = new File(dir+sumFile);
		//if not exist, create a new DBSumFile
		if(!in_file.exists()){
			in_file.createNewFile();
		}
		FileReader f = new FileReader(in_file);
		BufferedReader in = new BufferedReader(f);
		String line = in.readLine();
		while (line != null && !line.trim().equals("") && line.trim() !=null) { //add the line.trim()==null to prevent empty line
			String line_split[] = line.split("\\s*;\\s*");
			int len = line_split.length;
			String singleFileName = (line_split[0].trim().split("\\s*=\\s*"))[1].trim();
			if (len > 1) {
				HashMap<String, String> tempAtt = new HashMap<String, String>();
				for (int i = 1; i < len; i++) {
					String att_split [] = line_split[i].trim().split("\\s*=\\s*");
					int att_len = att_split.length;
					String att = att_split[0].trim();
					String value = "";
					if(att_len>1){
						value = att_split[1].trim();
					}
					tempAtt.put(att, value);
				}
				table.put(singleFileName, tempAtt); // add each file's infor to the table
			} else {
				table.put(singleFileName, null); // add each file's infor into the table
			}
			line = in.readLine(); // must have this inside
		}
		f.close();
	}

	//create File security infor for each file
	public static HashMap<String,String> createFileSecurityInfo()throws IOException {
		HashMap<String,String> result = new HashMap<String,String>();
		result.put("owner", getOwner());
		result.put("user", getUser());
		result.put("visible", "Y");
		result.put("modifiable", "Y");
		result.put("limitedAccess", "N");
		result.put("highestKey", "0");
		result.put("record_num", "0");
		return result;
	}


	public static HashMap<String, String> setDefaultUserInfo(){
		HashMap<String, String> user_map = new HashMap<String, String>();
		user_map.put("passWord", "cs609"); //each user's default password is cs609
		user_map.put("isSuperUser", "N");
		return user_map;
	}

	//get a file(or a user)'s all attributes
	public static HashMap<String, String> getAllInfo(String label, String file, String sumFile, String filePath, HashMap<String,HashMap<String, String>> table) throws IOException{
		readSumFile(sumFile, filePath,table);
		if(table.containsKey(file)){
			HashMap<String, String> result = table.get(file);
			return result;
		}else{
			System.out.println("This "+label+ " does not exist");
			return null;
		}
	}

	//get a single attribute from a file (or a user)
	public static String getSingleInfo(String label, String file, String att,String sumFile, String filePath, HashMap<String,HashMap<String, String>> table) throws IOException{
		HashMap<String, String> result=getAllInfo(label,file, sumFile,filePath,table);
		if(result == null){
			System.out.println("The " + label +" has no attributes set up");
			return "";
		}
		if(result.containsKey(att)){
			String value = result.get(att);
			return value;
		}else{
			System.out.println("The " + file + " has no content of "+att);
			return "";
		}
	}

	//print all attributes of a file
	public static void printAllInfo(String label, String file, String sumFile, String filePath, HashMap<String,HashMap<String, String>> table) throws IOException{
		HashMap<String, String> result=getAllInfo(label,file, sumFile,filePath,table);
		if(result.isEmpty() || result == null){
			System.out.println("No record found");
			return;
		}
		Set<String> keySet = result.keySet();
		StringBuilder att = new StringBuilder();
		att.append(label+"="+file+";	 ");
		for(String key:keySet){
			att.append(key+"="+result.get(key)+";	 ");
		}
		System.out.println(att);
	}

	//print a single attribute of a file
	public static void printSingleInfo(String label, String file, String att, String sumFile, String filePath, HashMap<String,HashMap<String, String>> table) throws IOException{
		if(getSingleInfo(label,file,att, sumFile,filePath,table).equals("null")||getSingleInfo(label,file,att, sumFile,filePath,table)==null){
			return;
		}
		System.out.println(label+"=" + file + ";	 "+ att + "=" + getSingleInfo(label,file,att, sumFile,filePath,table));
	}

	//get all the real filename (has the real data) existing in the "files" folder
	public static String[] getDBFiles(String dir) {
		File folder = new File(dir);
		File[] fileList = folder.listFiles();
		if(fileList !=null || fileList.length>0){
			String[] fileName = new String[fileList.length];

			for (int i = 0; i < fileList.length; i++) {
				fileName[i] = fileList[i].getName();
		}
		return fileName;
		}else{
			System.out.println("no file exists");
			return null;
			//System.exit(0);
		}
	}

	//print all the real filename (has the real data) existing in the "files" folder
	public static void printAllDBFilesName(String [] files){
		if(files==null ||files.length==0){
			System.out.println("No file exists");
			return;
		}
		for(String file : files){
			System.out.println(file);
		}
	}

	public static boolean getFileNameFromDBSumFile(String fileName,String sumFile, String dir,HashMap<String,HashMap<String, String>> table) throws IOException{
		readSumFile(sumFile,dir,table);
		if (DBSumFileTable.containsKey(fileName)){
			return true;
		}
		return false;
	}

	public static String getOwner(){
		return owner;
	}

	public static void setOwner(String newOwner){
		owner=newOwner;
	}

	public static String getCurrentUser(){
		return currentUser;
	}

	public static void setCurrentUser(String newUser){
		currentUser=newUser;
	}

	public static String getVisible(){
		return visible;
	}

	public static void setVisible(String boo){
		visible=boo;
	}

	public static String getModifiable(){
		return modifiable;
	}

	public static void setModifiable(String boo){
		modifiable=boo;
	}

	public static String getLimitedAccess(){
		return limitedAccess;
	}

	public static void setLimitedAccess(String boo){
		limitedAccess=boo;
	}

	public static String getCurrentFileName(){
		return currentFileName;
	}

	public static void setCurrentFileName(String newFileName){
		currentFileName = newFileName;
	}

	public static String getUser(){
		return currentUser;
	}

	public static void setUser(String usr){
		currentUser = usr;
	}

	//to get the all users as a string for a file
	public static String getFileAllUsers(String file) throws IOException{ //yilongs, 04/18
		readSumFile(DBSumFileName, DBSumFilePath, DBSumFileTable);
		return DBSumFileTable.get(file).get("user");
		}

	public static void setFileAllUsersUser(String file, String usr) throws IOException{//yilongs, 04/18
		readSumFile(DBSumFileName, DBSumFilePath, DBSumFileTable);
		DBSumFileTable.get(file).put("user",usr);
	}

	public static int getHighestKey(){
		return Integer.parseInt(highestKey);
	}

	public static void setHighestKey(int hk){
		highestKey = ""+hk;
	}

	public static void addUser(String newUser) throws IOException{
		if(fileUserFound(newUser,getCurrentFileName())){
			System.out.println("The user " + newUser + " already exists.");
			return;
		}
		if(newUser !=null){
		String temp_usr = "";
		if(getUser()==null || getUser().length()==0){
			temp_usr = newUser+",";
		}else{
			temp_usr = getUser().concat(newUser+",");
		}
		//setUser(temp_usr);
		createNewUser(userLabel, temp_usr, DBUserFileName, DBUserFilePath, DBUserFileTable); // add this new user to the userfile
		System.out.println("The user " + newUser + " was successfully added to the user List of this file.");
		}
	}

	public static void addFileUser(String newUser) throws IOException{
		if(fileUserFound(newUser,getCurrentFileName())){
			System.out.println("The user " + newUser + " already exists.");
			return;
		}
		if(newUser !=null){
		   String temp_usr = getFileAllUsers(getCurrentFileName())+","+newUser;
		   updateInfo(fileLabel, getCurrentFileName(), "user", temp_usr, DBSumFileName, DBSumFilePath, DBSumFileTable);
		//(userLabel, temp_usr, DBUserFileName, DBUserFilePath, DBUserFileTable); // add this new user to the userfile
		   System.out.println("The user " + newUser + " was successfully added to the user List of this file.");
		}
	}

	//to delete a file or a user from the DBSumFile or the DBUserFile
	public static void deleteFileUserRecord(String label,String userName, String file,String sumFile, String filePath, HashMap<String,HashMap<String, String>> table ) throws IOException{
		readSumFile(sumFile, filePath,table);
		if(fileUserFound(file,userName)){
		   deleteFileUser(file,userName);
		}else{
		System.out.println("There is no " + label + " exsisting in the database for deletion");
			return;
			}

	}

	public static void deleteFileUser(String file,String oldUser) throws IOException{//yilongs,04/17
      String allUser = getFileAllUsers(file).trim();
      System.out.println(allUser);
	if(allUser!=null || !allUser.equals("")||allUser.split(",").length > 0){
	      ArrayList<String> usr_arr=new ArrayList<String>();
	        if(!allUser.contains(",")){
			   usr_arr.add(allUser);
			}else{
			    String usr_each[] =allUser.split(",");
				for(String str:usr_each){
				System.out.println(str);
				   usr_arr.add(str);
				}
			}
			//System.out.println(usr_each[0]);
			StringBuilder result_usr = new StringBuilder();
			int count=0;
			for (String str : usr_arr){
				if(!str.equals(oldUser)){
					result_usr.append(str+",");
				}else{
					count++;
				}
			}

			String final_usr=result_usr.toString();
			if(final_usr.contains(",")){
				final_usr=final_usr.substring(0,final_usr.length()-1);
			}
			if(count > 0){
				updateInfo(fileLabel, file, "user", final_usr, DBSumFileName, DBSumFilePath, DBSumFileTable);
            	System.out.println("The user " + oldUser + " was successfully removed");
			}else{
				System.out.println("The user " + oldUser + " was not found");
			}
		}else{
			System.out.println("No users existing for this file");
			return;
		}
}

public static boolean fileUserFound(String file, String usr) throws IOException{//yilongs,04/17
	readSumFile(DBSumFileName, DBSumFilePath, DBSumFileTable);

	if(DBSumFileTable.containsKey(file)){
		String usr_each = DBSumFileTable.get(file).get("user"); //.trim().split("(\\s*)(+)(\\s*)");
		if (usr_each.contains(usr)){
			//if(str.equals(usr)){
				System.out.println("The user "+ usr + " exists");
				return true;
			//}
		}
		return false;
	}else{
		//System.out.println("The file " + file + " does not exist");
		return false;
	}

}

}
