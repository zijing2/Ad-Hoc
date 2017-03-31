package OLAP;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.script.*;

public class Main {
	
	public static HashMap<String, HashMap<String, Integer>> H;

	public static void main(String[] args) {
		MFConfig.initConfig(
				//S
				"cust, 1_sum_quant, 2_sum_quant, 3_sum_quant",
				//N
				"3",
				//V
				"cust,prod",
				//F
				"1_sum_quant, 1_avg_quant, 2_sum_quant, 2_avg_quant, 3_sum_quant, 3_avg_quant", 
				//O
				"state='NY',state='NJ',state='CT'",
				//G
				"1_sum_quant > 2 * 2_sum_quant or 1_avg_quant > 3_avg_quant");
		
		//prompt();
		evaluate();
		render();
	}
	
	public static void prompt(){
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		try {
			System.out.println("SELECT ATTRIBUTE(S):");
			String pr = br.readLine();
			System.out.println("NUMBER OF GROUPING VARIABLES(n):");
			String gv = br.readLine();
			System.out.println("GROUPING ATTRIBUTES(V):");
			String ga = br.readLine();
			System.out.println("F-VECT([F]):");
			String af = br.readLine();
			System.out.println("SELECT CONDITION-VECT([σ]):");
			String pd = br.readLine();
			System.out.println("HAVING_CONDITION(G):");
			String hg = br.readLine();
			
			MFConfig.initConfig(pr, gv, ga, af, pd, hg);
			
			System.out.println(MFConfig.S[0]);

		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	public static void evaluate(){
		Statement stmtInsetance = Driver.getConnectionSingleton();
		ResultSet rs;
		ScriptEngineManager manager = new ScriptEngineManager();  
        ScriptEngine engine = manager.getEngineByName("js");
		H  = new HashMap<String, HashMap<String, Integer>>();
		
		//go through each grouping variable (vertical)
		for(int i=0;i<=MFConfig.N;i++){
			//System.out.println("-----------");
			try {
				rs = stmtInsetance.executeQuery("SELECT * FROM Sales");
				while (rs.next())
				{
					ArrayList<String> ga_list = new ArrayList<String>();
					for(int k=0;k<MFConfig.V.length;k++){
						ga_list.add(rs.getString(MFConfig.V[k]));
					}
					String ga = getGA(ga_list);
					//String ga = rs.getString("cust");
					//denote grouping attribute
					if(i==0){
						if(H.get(ga)==null){
							HashMap<String, Integer> map =  new HashMap<String, Integer>();
							for(int j=0;j<MFConfig.F.length;j++){
								//if there is avg, then change to count + sum
								if(MFConfig.F[j].trim().indexOf("avg")>=0){
									map.put(MFConfig.F[j].trim().replace("avg", "sum"), null);
									map.put(MFConfig.F[j].trim().replace("avg", "count"), null);
								}else{
									map.put(MFConfig.F[j].trim(), null);
								}
								
							}
							H.put(ga, map);
						}else{
							continue;
						}
					}else{
						//traverse each grouping variable
						//range of grouping variable
						String rgv = MFConfig.O[i-1].replace("=", "==");
						String column = "state";
						String state = rs.getString(column);
						engine.put("state", state);
						//String str = "(state == 'NY')";
						String str = "("+rgv+")";
						Object result = null;
						try {
							result = engine.eval(str);
						} catch (ScriptException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} 
						//System.out.println(rs.getString("state")+"|"+result);
						if((boolean)result){
						//if(rs.getString("state").equals("NY")){
							HashMap<String, Integer> map = H.get(ga);
							//walk though hashmap keys and find related F
							Iterator iter = map.entrySet().iterator();
							while(iter.hasNext()) {
						           Map.Entry entry = (Map.Entry)iter.next();
						           String[] temp = entry.getKey().toString().split("_");
						           if(Integer.parseInt(temp[0])==i){
						        	   if(temp[1].equals("sum")){
						        		   if(H.get(ga).get(entry.getKey().toString())==null){
						        			   H.get(ga).put(entry.getKey().toString(), Integer.parseInt(rs.getString("quant")));
						        		   }else{
						        			   H.get(ga).put(entry.getKey().toString(), H.get(ga).get(entry.getKey().toString())+Integer.parseInt(rs.getString("quant")));
						        		   }
						        	   }else if(temp[1].equals("count")){
						        		   if(H.get(ga).get(entry.getKey().toString())==null){
						        			   H.get(ga).put(entry.getKey().toString(), 1);
						        		   }else{
						        			   H.get(ga).put(entry.getKey().toString(), H.get(ga).get(entry.getKey().toString())+1);
						        		   }
						        	   }else if (temp[1].equals("max")){
						        		   
						        	   }else if (temp[1].equals("min")){
						        		   
						        	   }
						           }
//						           if(Integer.parseInt(entry.getKey().toString().substring(0,1))==i){
//						        	   System.out.println(ga +"|" + entry.getKey() +" - "+entry.getValue());
//						           }
							}
						}
						
						
					}
					//System.out.println(rs.getString("cust"));
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		System.out.println(H);
//		Statement stmtInsetance = Driver.getConnectionSingleton();
//		ResultSet rs;
//		try {
//			rs = stmtInsetance.executeQuery("SELECT * FROM Sales");
//			while (rs.next())
//			{
//				System.out.println(rs.getString("cust"));
//				
//			}
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}
	
	private static void render(){
		
	}
	
	private static String getGA(ArrayList list){
		StringBuffer buff = new StringBuffer();
		buff.append(list.get(0));
		
		for(int i=1;i<list.size();i++){
			buff.append("-");
			buff.append(list.get(i));
			i++;
		}
		return buff.toString();
	}

}
