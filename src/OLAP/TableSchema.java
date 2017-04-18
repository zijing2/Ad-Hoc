package OLAP;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/*
 * this class is for storing the structure of table
 */
public class TableSchema {

	public static HashMap<String, String> map;
	
	public static void InitTableSchema(){
		map = new HashMap<String, String>();//attr,type
		ResultSet rs = Data.getInformationSchema();
		
		try {
			while(rs.next()){
				map.put(rs.getString("column_name"), rs.getString("data_type"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		//System.out.println(map);
	}
	
	public static boolean isTableAttribute(String s){
		Iterator iter = map.entrySet().iterator();
		String key;
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry)iter.next();
			key = (String)entry.getKey();
			if(key.equalsIgnoreCase(s)){
				return true;	
			}
		}
		return false;
	}
	
	/*
	 * return all table attributes except for grouping attributes
	 */
	public static String[] getAllTableAttributeExceptGA(){
		ArrayList<String> list = new ArrayList<String>();
		Iterator iter = map.entrySet().iterator();
		String key;
	outter:	while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry)iter.next();
			key = (String)entry.getKey();
			for(int i=0;i<MFConfig.V.length;i++){
				if(key.equalsIgnoreCase(MFConfig.V[i])){
					continue outter;
				}
			}
			list.add(key);
		}
		
		return (String[])list.toArray(new String[list.size()]);
	}
	
	/*
	 * return all table attributes
	 */
	public static String[] getAllTableAttributes(){
		ArrayList<String> list = new ArrayList<String>();
		Iterator iter = map.entrySet().iterator();
		String key;
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry)iter.next();
			key = (String)entry.getKey();
			list.add(key);
		}
		
		return (String[])list.toArray(new String[list.size()]);
	}
	
	public static boolean isAttributeInt(String attr){
		String data_type = map.get(attr);
		if(data_type.equals("integer")){
			return true;
		}else{
			return false;
		}
	}
	
}
