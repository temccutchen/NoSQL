
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;


public class Logs {
	public final static String logFilePath = "..\\logs\\";
	public final static String logFileName = "logFile";
	public static ArrayList<String> logTimeList = new ArrayList<String>();
	public static HashMap<String, HashMap<String,String>> allLog = new HashMap<String,HashMap<String,String>>();
	public static String rowID, operation, fieldName, oldVal, newVal;


	public static void writeLogToFile(String log, String outFile,String dir)throws IOException {
		File out_File = new File(dir+outFile);
		try{
			if(!out_File.exists()){
	 		  out_File.createNewFile();
	 		}
	  		//true = append file
			FileWriter fileWritter = new FileWriter(out_File.getPath(),true); //must use getAbsolutepath() to write to the right folder
			//if use "new FileWriter(out_File.getName(),false)" ---the real data will be write to relative path(java src folder) and an empty file will be in the target folder
	        BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
	        if(!log.trim().equals("") || log !=null){
	        	   bufferWritter.write(log);
				        //write to next line!
				   bufferWritter.newLine();
	        }
		    bufferWritter.close();
		}catch(IOException e){
			e.printStackTrace();
	  }

	}

//todd's suggested method

	public static void recordInsertUpdateLog(String op, ArrayList<String> row, ArrayList<String> fields, ArrayList<String> values) throws IOException{

		for (String aRow:row){
			int len=fields.size();
			java.util.Date date;date= new java.util.Date();
			String time=new Timestamp(date.getTime()).toString();
			StringBuilder common = new StringBuilder();
			//new_result.append("<<new>>----   ");
			common.append(""+time +";   ");
			common.append(op+";   ");
			common.append("rowID="+aRow +";   ");
			String commonInfo=common.toString();

			StringBuilder new_result = new StringBuilder();
			for(int i=0;i<len; i++){
				new_result.append(fields.get(i)+"="+values.get(i)+";   ");
			}

			StringBuilder old_result = new StringBuilder();
			if(op.equals("insert".toLowerCase())||op.equals("insertbykey".toLowerCase())){
				for(int i=0;i<len; i++){
					old_result.append(fields.get(i)+"="+";   ");

				}
			}else if(op.equals("updatebykeys".toLowerCase())){
                	old_result.append(getSelectedRowData(aRow,fields));
			}

           writeLogToFile("<<old records>>--- "+commonInfo+old_result.toString(),logFileName,logFilePath);
           writeLogToFile("<<new records>>--- "+commonInfo+new_result.toString(),logFileName,logFilePath);

		    logTimeList.add(time);
		}
}

	public static void recordDeleteLog(String op, ArrayList<String> row)throws IOException{

		for (String aRow:row){
			java.util.Date date;date= new java.util.Date();
			String time=new Timestamp(date.getTime()).toString();
			StringBuilder common = new StringBuilder();
			common.append(""+time +";   ");
			common.append(op+";   ");
			common.append("rowID="+aRow +";   ");
			String commonInfo=common.toString();


			String oldV="";
			String fieldsList="";
			if(op.equals("deletebykeys".toLowerCase())) {//||op.equals("deletebyfieldsvalues".toLowerCase())){
                 oldV =getWholeRowData(aRow)[0]; //old value
                 fieldsList=getWholeRowData(aRow)[1];
			}else{

			}

			String[] single_field=fieldsList.split(";");
			StringBuilder new_result=new StringBuilder();
			for(String fil:single_field){
				new_result.append(fil + "=;   ");
			}

             writeLogToFile("<<old records>>--- "+commonInfo+oldV,logFileName,logFilePath);
             writeLogToFile("<<new records>>--- "+commonInfo+new_result.toString(),logFileName,logFilePath);

		    logTimeList.add(time);
		}
}

	public static void recordDeleteFVLog(String op, ArrayList<String> fields, ArrayList<String> values)throws IOException{
            Set<Integer> keys = DB.newDataFile.searchSpace.keySet();

		for (Integer aRow:keys){
			java.util.Date date;date= new java.util.Date();
			String time=new Timestamp(date.getTime()).toString();
			StringBuilder common = new StringBuilder();
			common.append(""+time +";   ");
			common.append(op+";   ");
			common.append("rowID="+aRow +";   ");
			String commonInfo=common.toString();

			String oldV="";
			String fieldsList="";
			if(op.equals("deletebyfieldsvalues".toLowerCase())){
                 oldV =getWholeRowData(""+aRow)[0]; //old value
                 fieldsList=getWholeRowData(""+aRow)[1];
			}

			String[] single_field=fieldsList.split(";");
			StringBuilder new_result=new StringBuilder();
			for(String fil:single_field){
				new_result.append(fil + "=;   ");
			}

           writeLogToFile("<<old records>>--- "+commonInfo+oldV,logFileName,logFilePath);
           writeLogToFile("<<new records>>--- "+commonInfo+new_result.toString(),logFileName,logFilePath);

		    logTimeList.add(time);
		}
}


	public static String getSelectedRowData(String ID, ArrayList<String> fields){
		HashMap<String, String> result_map=DB.newDataFile.searchSpace.get(Integer.parseInt(ID));
		StringBuilder result=new StringBuilder();
		int len=fields.size();
		for(int i=0; i<len; i++){
			if(result_map.containsKey(fields.get(i))){
				result.append(fields.get(i)+"="+result_map.get(fields.get(i)) + ";   ");
			}else{
				result.append(fields.get(i)+"="+";   ");
			}
		}
        return result.toString();

	}

	public static String[] getWholeRowData(String ID){
		String temp[] =new String[2];
		HashMap<String, String> result_map=DB.newDataFile.searchSpace.get(Integer.parseInt(ID));
		StringBuilder result=new StringBuilder();
		StringBuilder fields=new StringBuilder();
		for(String str : result_map.keySet()){
				result.append(str+"="+result_map.get(str) + ";   ");
				fields.append(str+";");
		}
		temp[0]=result.toString();
		temp[1]=fields.toString();

        return temp;

	}

	///


	public static void printMessage(String message){
		System.out.println(message);
	}


public static void clearLog(){
	if(!allLog.isEmpty()){
		allLog.clear();
	}else{
		System.out.println("The log is empty");
	}
}


}
