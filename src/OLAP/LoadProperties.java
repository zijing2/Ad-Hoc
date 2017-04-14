package OLAP;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class LoadProperties {
	public static String[] load_properties() {
		/*
		 * 
		 * @param null
		 * @return:
		 * 		String array with length is 3. First one is username, second is password, third is URL
		 * 		
		 * 		Description:
		 * 			A string array of username, password and url, which consist of a connection
		 * 	    	string to database. By default, the config file is config.properties.
		 * 		
		 */

		Properties prop = new Properties();
		InputStream input = null;
		String user = null;
		String password = null;
		String url = null;
		try {

			input = new FileInputStream("src\\OLAP\\config.properties");

			// load a properties file
			prop.load(input);

			// get the property value and print it out
			System.out.println("User:" + prop.getProperty("USER"));
			user = prop.getProperty("USER");
			
			System.out.println("Password:" + prop.getProperty("PASSWORD"));
			password = prop.getProperty("PASSWORD");
			
			System.out.println("URL:" + prop.getProperty("URL"));
			url = prop.getProperty("URL");

		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			
		}
		return new String[]{user, password, url};

	  }

}
