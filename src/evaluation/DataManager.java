package evaluation;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class DataManager {	
	public static List<String> readFile(String path) {		
		List<String> documents = new ArrayList<String>();
		try {
			FileInputStream fstream = new FileInputStream(path);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			while ((strLine = br.readLine()) != null) {
			    documents.add(strLine);
			}
			in.close();
		} catch(Exception e) {
            e.getStackTrace();
		}
		return documents;
	}

	public static void writeFile(String string, String path) {
		try {
		    File f = new File(path);
		    if(f.exists()) {
		        System.out.println("File already exists");
			} else {
			    f.createNewFile();
			}			 
		    
		    try {
		    	FileWriter fout = new FileWriter(path,true);
		        PrintWriter fileout = new PrintWriter(fout,true);
		        fileout.println(string);
		        fileout.flush();
		        fileout.close();
		    } catch(IOException e) {	
		        e.getStackTrace();
		    }
		} catch(Exception e) {
			  e.getStackTrace();
		}
	}
}
