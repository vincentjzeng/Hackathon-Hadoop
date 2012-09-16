package org.springframework.data.hadoop.cache;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class FileUnits {

	public static boolean isFileExist(String fileName){
		File f = new File(fileName);
		return f.exists();
	}
	
	public static void saveToDisk(String fileName, Object o){
		try {
			FileOutputStream fs = new FileOutputStream(fileName);
			ObjectOutputStream oos = new ObjectOutputStream(fs);
			oos.writeObject(o);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static Object readFileToCache(String fileName){
		try {
			FileInputStream fin = new FileInputStream(fileName);
			ObjectInputStream ois = new ObjectInputStream(fin);
			return ois.readObject();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}
