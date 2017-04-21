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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.*;

public class Main {
	
	public static HashMap<String, HashMap<String, String>> H;
	
	public static Statement stmtInsetance;
	
	public static ScriptEngine engine;

	public static void main(String[] args) {
		MFConfig.initConfig(
				//S
				"prod,month,1_avg_quant,2_count_quant,3_avg_quant",
				//N
				"3",
				//V
				"prod,month",
				//F
				"1_avg_quant,2_count_quant,3_avg_quant", 
				//O
				"1.prod=prod and 1.month=month-1, 2.prod=prod and 2.month = month and 2.quant>avg( 1.quant ) and 2.quant<avg(3.quant),3.prod=prod and 3.month=month+1",
				//G
				"3_avg_quant>1_avg_quant");
		
		try{
			init();
			//prompt();
			topologicalSort();
			evaluate();
			render();
		}catch(endProgramExeption e){
			System.out.println(e.message);
		}catch(Exception e){
			System.out.println(e);
		}
		
	}
	
	public static void init(){
		TableSchema.InitTableSchema();
		ScriptEngineManager manager = new ScriptEngineManager();
		engine = manager.getEngineByName("javascript");
		//suchThatClauseParser("1.month > month and 1.state = 'NY'");
	}
	
	public static void prompt() throws endProgramExeption{
		try{
			IO.prompt();
			System.out.println("please wait a second...");
		}catch(endProgramExeption e){
			throw new endProgramExeption();
		}
	}
	
	/*
	 * topologicalSort base on such that dependency, result store on Graph.seq_arr
	 */
	public static void topologicalSort() throws Exception{
		Graph.initG();
		Graph.topoligicalSort();
//		for(int i=0;i<Graph.seq_arr.length;i++){
//			System.out.println(Graph.seq_arr[i]);
//		}
	}
	
	public static void evaluate() throws ScriptException{
		  
		H  = new HashMap<String, HashMap<String, String>>();
		
		//go through each grouping variable (vertical)
		for(int r=0;r<=MFConfig.N;r++){
			int i = Integer.parseInt(Graph.seq_arr[r]);
			try {
				ResultSet rs = Data.getSalesRow();
				while (rs.next())
				{
					//denote grouping attribute
					if(i==0){
						initX0(rs);
					}else{
						//traverse each grouping variable 
						//and each row compare with all grouping attributes and grouping variables
						//range of grouping variable
						Iterator iter_outter = H.entrySet().iterator();
						while(iter_outter.hasNext()){
							Map.Entry entry_outter = (Map.Entry)iter_outter.next();
							String ga = entry_outter.getKey().toString();
							String[]ga_list = MFConfig.debindGA(ga);
							if(suchThat(MFConfig.O[i-1], rs, i, ga_list)){
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
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		//execute having clause
		having();
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
				//if there is "avg", then change to "count" + "sum"
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
	 * 
	 * clause : such that clause of particular grouping variable
	 * rs : a row of data
	 * round_num : denote which grouping variable we are processings
	 * ga : grouping attribute for this time, for example ["Bloom", "Pepsi"]
	 */
	private static boolean suchThat(String clause, ResultSet rs, int round_num, String[] ga){
		//find expressions which not belong to grouping attribute
		Object result = null;
		try {
			//for example (1.month > month and 1.state = "NY") to (month_1 > month and state_1 == "NY") 
			//extra credit:(2.quant > avg(1.quant)) to (_2quant > 1_avg_quant)
			String rgv = suchThatClauseParser(clause);
			String[] attrs = TableSchema.getAllTableAttributes();
			for(int i=0;i<attrs.length;i++){
				if(TableSchema.isAttributeInt(attrs[i])){
					engine.put("_"+round_num+attrs[i], Integer.parseInt(rs.getString(attrs[i])));
				}else{
					engine.put("_"+round_num+attrs[i], rs.getString(attrs[i]));
				}
			}
			for(int i=0;i<ga.length;i++){
				if(TableSchema.isAttributeInt(MFConfig.V[i])){
					engine.put(MFConfig.V[i], Integer.parseInt(ga[i]));
				}else{
					if(ga[i]==null){
						engine.put(MFConfig.V[i], "=false");
					}else{
						engine.put(MFConfig.V[i], ga[i]);
					}
				}
			}
			
			//if such that contains dependents
			if(rgv.indexOf("avg(")!=-1 || rgv.indexOf("sum(")!=-1 ||  rgv.indexOf("count(")!=-1 || rgv.indexOf("max(")!=-1 || rgv.indexOf("min(")!=-1){
				HashMap<String, String> res = suchThatClauseDependentsParser(rgv);
				Iterator iter = res.entrySet().iterator();
				String ga_list = MFConfig.bindGA(ga);
				while(iter.hasNext()){
					Map.Entry entry_outter = (Map.Entry)iter.next();
					String key = entry_outter.getKey().toString();
					if(key.equals("rgv")){
						rgv = res.get(key);
					}else{
						//compute avg
						if(key.indexOf("avg")!=-1){
							String value_temp;
							String sum_temp = res.get(key).replace("avg", "sum");
							String count_temp = res.get(key).replace("avg", "count");
							if(H.get(ga_list).get(sum_temp)==null||H.get(ga_list).get(count_temp)==null){
								value_temp = "=false";
							}else{
								value_temp = String.valueOf(Integer.parseInt(H.get(ga_list).get(sum_temp))/Integer.parseInt(H.get(ga_list).get(count_temp)));
							}
							engine.put(key, value_temp);
						}else{
							if(H.get(ga_list).get(res.get(key))==null){
								engine.put(key, "=false");
							}else{
								engine.put(key, H.get(ga_list).get(res.get(key)));
							}
						}
					}
				}
			}
			String str = "("+rgv+")";
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
		clause = clause.replaceAll("=", "==").replaceAll("and", "&&").replaceAll("or", "||").replaceAll("<>", "!=");
		//System.out.println(clause);
		return clause;
	}
	
	/*
	 * return 2 things:
	 * 1.such that clause after parse, for example {"clasue": "_1quant>_2quant_avg && _1quant<_3quant_avg"}
	 * 2.dependent elements mapping, like {"_2quant_avg": "2_avg_quant"}
	 */
	private static HashMap<String,String> suchThatClauseDependentsParser(String clause){
		HashMap<String,String> result = new HashMap<String,String>();
		StringBuffer temp;
		String s = clause.replaceAll("avg\\(\\s*", "_avg").replaceAll("sum\\(\\s*", "_sum").replaceAll("count\\(\\s*", "_count").replaceAll("max\\(\\s*", "_max").replaceAll("min\\(\\s*", "_min").replaceAll("\\)", "");
		
		//avg
		Pattern p = Pattern.compile("_avg_(\\w+)\\s*");
		Matcher m = p.matcher(s);
		while(m.find()){
			temp = new StringBuffer();
			//System.out.println(m.group(0));
			for(int i=0;i<m.group(0).length();i++){
				char c = m.group(0).charAt(i);
				try{
					Integer.parseInt(""+c);
					temp.insert(0, c);
				}catch(NumberFormatException e){
					temp.append(c);
				}
			}
			result.put(m.group(0).trim(), temp.toString().trim());
		}
		
		//sum
		p = Pattern.compile("_sum_(\\w+)\\s*");
		m = p.matcher(s);
		while(m.find()){
			temp = new StringBuffer();
			//System.out.println(m.group(0));
			for(int i=0;i<m.group(0).length();i++){
				char c = m.group(0).charAt(i);
				try{
					Integer.parseInt(""+c);
					temp.insert(0, c);
				}catch(NumberFormatException e){
					temp.append(c);
				}
			}
			result.put(m.group(0).trim(), temp.toString().trim());
		}
		
		//max
		p = Pattern.compile("_max_(\\w+)\\s*");
		m = p.matcher(s);
		while(m.find()){
			temp = new StringBuffer();
			for(int i=0;i<m.group(0).length();i++){
				char c = m.group(0).charAt(i);
				try{
					Integer.parseInt(""+c);
					temp.insert(0, c);
				}catch(NumberFormatException e){
					temp.append(c);
				}
			}
			result.put(m.group(0).trim(), temp.toString().trim());
		}
		
		//min
		p = Pattern.compile("_min_(\\w+)\\s*");
		m = p.matcher(s);
		while(m.find()){
			temp = new StringBuffer();
			for(int i=0;i<m.group(0).length();i++){
				char c = m.group(0).charAt(i);
				try{
					Integer.parseInt(""+c);
					temp.insert(0, c);
				}catch(NumberFormatException e){
					temp.append(c);
				}
			}
			result.put(m.group(0).trim(), temp.toString().trim());
		}
		
		result.put("rgv", s);
		return result;
	}
	
	/*
	 * filter result base on having clause
	 */
	private static void having() throws ScriptException{
		String having_clause = havingParser(MFConfig.G);
		System.out.println(having_clause);
		Iterator iter1 = H.entrySet().iterator();
		while(iter1.hasNext()){
			Map.Entry entry1 = (Map.Entry)iter1.next();
			String key1 = entry1.getKey().toString();
			HashMap<String,String> inner_value = H.get(key1);
			Iterator iter2 = inner_value.entrySet().iterator();
			while(iter2.hasNext()){
				Map.Entry entry2 = (Map.Entry)iter2.next();
				String key2 = entry2.getKey().toString();
					
				//generate avg
				String[] temp1 = key2.split("_");
				String key3 = temp1[0]+"_avg_"+temp1[2];
				char[] char_arr = key2.toCharArray();
				char temp = char_arr[0];
				char_arr[0] = char_arr[1];
				char_arr[1] = temp;
				
				char[] char_arr1 = key3.toCharArray();
				char temp2 = char_arr1[0];
				char_arr1[0] = char_arr1[1];
				char_arr1[1] = temp2;
				
				String sum = inner_value.get(temp1[0]+"_sum_"+temp1[2]);
				String count = inner_value.get(temp1[0]+"_count_"+temp1[2]);
				if(sum!=null && count!=null){
					engine.put(String.valueOf(char_arr1), Integer.parseInt(sum)/Integer.parseInt(count));
				}
				if(inner_value.get(key2)==null){
					engine.put(String.valueOf(char_arr), "=false");
				}else{
					engine.put(String.valueOf(char_arr), inner_value.get(key2));
				}
			}
			String str = "("+having_clause+")";
			Object result = engine.eval(str);
			if((boolean)result==false){
				iter1.remove();
			}
		}
	}
	
	/*
	 * change having clause to a proper javascript expression(the first letter can not be a number)
	 * 1_avg_quant > 2_avg_quant => _1avg_quant > _2avg_quant
	 */
	private static String havingParser(String having){
		char[] char_arr = having.toCharArray();
		for(int i=0;i<char_arr.length;i++){
			if((""+char_arr[i]).equals("_")){
				try{
					Integer.parseInt(""+char_arr[i-1]);
					char temp = char_arr[i];
					char_arr[i] = char_arr[i-1];
					char_arr[i-1] = temp;
				}catch(NumberFormatException e){
					
				}
			}
		}
		String s = String.valueOf(char_arr);
		return s;
	}
	
	private static boolean isOperator(Character c){
		if(c.equals("=") || c.equals(">") || c.equals("<")){
			return true;
		}
		return false;
	}

}
