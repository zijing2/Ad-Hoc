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
	
	public static HashMap<String, HashMap<String, String>> H;
	
	public static Statement stmtInsetance;
	
	public static ScriptEngine engine;

	public static void main(String[] args) {
		MFConfig.initConfig(
				//S
				"cust,prod,1_sum_quant,2_count_quant,3_sum_quant",
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
		
//		try{
			init();
			//prompt();
			evaluate();
			render();
//		}catch(endProgramExeption e){
//			System.out.print(e.message);
//		}
	}
	
	public static void init(){
		TableSchema.InitTableSchema();
		ScriptEngineManager manager = new ScriptEngineManager();
		engine = manager.getEngineByName("js");
		//suchThatClauseParser("1.month > month and 1.state = 'NY'");
	}
	
	public static void prompt() throws endProgramExeption{
		try{
			IO.prompt();
		}catch(endProgramExeption e){
			throw new endProgramExeption();
		}
	}
	
	public static void evaluate(){
		  
        
		H  = new HashMap<String, HashMap<String, String>>();
		
		//go through each grouping variable (vertical)
		for(int i=0;i<=MFConfig.N;i++){
			try {
				ResultSet rs = Data.getSalesRow();
				while (rs.next())
				{
					//denote grouping attribute
					if(i==0){
						initX0(rs);
					}else{
						ArrayList<String> ga_list = new ArrayList<String>();
						for(int k=0;k<MFConfig.V.length;k++){
							ga_list.add(rs.getString(MFConfig.V[k]));
						}
						String ga = MFConfig.bindGA(ga_list);
						//traverse each grouping variable 
						//and each row compare with all grouping attributes and grouping variables
						//range of grouping variable
						if(suchThat(MFConfig.O[i-1], rs)){
							HashMap<String, String> map = H.get(ga);
							//walk though hashmap keys and find related F
							Iterator iter = map.entrySet().iterator();
							while(iter.hasNext()) {
						           Map.Entry entry = (Map.Entry)iter.next();
						           String key = entry.getKey().toString();
						           aggregateFunction(ga,key,i,rs);
							}
						}
					}
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/*
	 * render result base on MFStructure meaning the Hashmap above
	 */
	private static void render(){
		IO.render(H);
	}
	
	/*
	 * initial hashmap as well as X0
	 */
	private static void initX0(ResultSet rs){
		ArrayList<String> ga_list = new ArrayList<String>();
		for(int k=0;k<MFConfig.V.length;k++){
			try {
				ga_list.add(rs.getString(MFConfig.V[k]));
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		String ga = MFConfig.bindGA(ga_list);
		if(H.get(ga)==null){
			//add X1~Xn structure to hashmap
			HashMap<String, String> map =  new HashMap<String, String>();
			for(int j=0;j<MFConfig.F.length;j++){
				//if there is avg, then change to count + sum
				if(MFConfig.F[j].trim().indexOf("avg")>=0){
					map.put(MFConfig.F[j].trim().replace("avg", "sum"), null);
					map.put(MFConfig.F[j].trim().replace("avg", "count"), null);
				}else{
					map.put(MFConfig.F[j].trim(), null);
				}
			}
			//add X0 (all attributes except grouping attributes)
			String[] attr_except_ga = TableSchema.getAllTableAttributeExceptGA();
			for(int i=0;i<attr_except_ga.length;i++){
				try{
					if(TableSchema.isAttributeInt(attr_except_ga[i])){
						map.put("0_sum_"+attr_except_ga[i], String.valueOf(Integer.parseInt(rs.getString(attr_except_ga[i]))));
					}
					map.put("0_count_"+attr_except_ga[i], "1");
					map.put("0_max_"+attr_except_ga[i], rs.getString(attr_except_ga[i]));
					map.put("0_min_"+attr_except_ga[i], rs.getString(attr_except_ga[i]));
				}catch(SQLException e){
					
				}
			}
			H.put(ga, map);
		}else{
			String[] attr_except_ga = TableSchema.getAllTableAttributeExceptGA();
			for(int i=0;i<attr_except_ga.length;i++){
				try {
					aggregateFunction(ga,"0_sum_"+attr_except_ga[i],0,rs);
					aggregateFunction(ga,"0_count_"+attr_except_ga[i],0,rs);
					aggregateFunction(ga,"0_max_"+attr_except_ga[i],0,rs);
					aggregateFunction(ga,"0_min_"+attr_except_ga[i],0,rs);
				} catch (NumberFormatException e) {
					e.printStackTrace();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/*
	 * ga : grouping attribute (after binded)
	 * key : aggregate function like 1_sum_quant
	 * i : denote the grouping variable
	 * rs : a row of data 
	 */
	private static void aggregateFunction(String ga, String key, int i, ResultSet rs) throws SQLException{
		String[] temp = key.split("_");
		try{
	        if(Integer.parseInt(temp[0])==i){
	     	   if(temp[1].equals("sum")){
	     		   if(TableSchema.isAttributeInt(temp[2])){
	     			  if(H.get(ga).get(key)==null){
		     			   H.get(ga).put(key, rs.getString(temp[2]));
		     		   }else{
		     			   H.get(ga).put(key, String.valueOf(Integer.parseInt(H.get(ga).get(key))+Integer.parseInt(rs.getString(temp[2]))));
		     		   }
	     		   }
	     	   }else if(temp[1].equals("count")){
	     		   if(H.get(ga).get(key)==null){
	     			   H.get(ga).put(key, "1");
	     		   }else{
	     			   H.get(ga).put(key, String.valueOf(Integer.parseInt(H.get(ga).get(key))+1));
	     		   }
	     	   }else if (temp[1].equals("max")){
	     		   if(H.get(ga).get(key)==null){
	     			   H.get(ga).put(key, rs.getString(temp[2]));
	     		   }else{
	     			  if(TableSchema.isAttributeInt(temp[2])){
	     				  H.get(ga).put(key, String.valueOf(Math.max(Integer.parseInt(rs.getString(temp[2])), Integer.parseInt(H.get(ga).get(key)))));
	     			  }else{
	     				 if(rs.getString(temp[2]).compareTo(H.get(ga).get(key))>0){
							H.get(ga).put(key,rs.getString(temp[2]));
						}
	     			  }
	     		   }
	     	   }else if (temp[1].equals("min")){
	     		   if(H.get(ga).get(key)==null){
	     			   H.get(ga).put(key, rs.getString(temp[2]));
	     		   }else{
	     			  if(TableSchema.isAttributeInt(temp[2])){
	     				  H.get(ga).put(key, String.valueOf(Math.min(Integer.parseInt(rs.getString(temp[2])), Integer.parseInt(H.get(ga).get(key)))));
	     			  }else{
	     				 if(rs.getString(temp[2]).compareTo(H.get(ga).get(key))<0){
	     					 H.get(ga).put(key,rs.getString(temp[2]));
	     				 }
	     			  }
	     		   }
	     	   }
	        }
		}catch(SQLException e){
			throw new SQLException(e);
		}
	}
	
	/*
	 * 能得出两个结论：
	 * 1.这条数据库数据符不符合grouping variable的要求，比如x1要求1.state="NY",但这条是1.state="NJ"就可以过掉这条
	 * 2.这条数据的更新范围：一般来说如果such that里面的判断是和grouping attribute是相等关系，
	 * 更新范围就是grouping attribute为key里面的数据。
	 * 当grouping attribute中有>或<时就不是更新特定一个hashmap，而是一组数据。比如1.month>month,
	 * 这时更新的就是hashmap里面month<=现在这条数据month的key的hash值。
	 */
	private static boolean suchThat(String clause, ResultSet rs){
		//find expressions which not belong to grouping attribute
		Object result = null;
		try {
			//for example (1.month > month and 1.state = "NY") to (month_1 > month and state_1 == "NY") 
			String rgv = suchThatClauseParser(clause);
			String[] attrs = TableSchema.getAllTableAttributes();
			for(int i=0;i<attrs.length;i++){
				engine.put(attrs[i], rs.getString(attrs[i]));
			}
			//String state = rs.getString("state");
			//engine.put("state", state);
			String str = "("+rgv+")";
			//String str = "(state=='NY')";
			result = engine.eval(str);
		} catch (ScriptException e) {
			e.printStackTrace();
		} catch (SQLException e){
			e.printStackTrace();
		}
		return (boolean)result;
	}
	
	/*
	 * change such that clause to proper form
	 * for example (1.month > month and 1.state = "NY") to (_1month > month and _1state == "NY") 
	 */
	private static String suchThatClauseParser(String clause){
		char[] arr = clause.toCharArray();

		for(int i=0;i<arr.length;i++){
			if((""+arr[i]).equals(".") && Character.isLetter(arr[i+1])){
				arr[i] = arr[i-1];
				arr[i-1] = '_';
			}
		}
		
		clause = String.valueOf(arr);
		clause = clause.replaceAll("=", "==").replaceAll("and", "&&").replaceAll("or", "||");
		//System.out.println(clause);
		return clause;
	}
	
	private static boolean isOperator(Character c){
		if(c.equals("=") || c.equals(">") || c.equals("<")){
			return true;
		}
		return false;
	}

}
