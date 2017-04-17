package OLAP;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.*;

public class Validation {
	//List of projected attributes for the query output
		private ArrayList<String> S;
		
		//Number of grouping variables
		private Integer N;
		
		//List of grouping attributes
		private ArrayList<String> V;
		
		//{F0, F1, ..., Fn}, list of sets of aggregate functions. Fi represents a list of aggregate functions for each grouping variable
		private ArrayList<String> F;
		
		//{σ0, σ1, ..., σn}, list of predicates to define the ranges for the grouping variables.
		private ArrayList<String> O;
		
		//Predicate for the having clause
		private String G;
		
		private HashMap<String, HashMap<String, Integer>> outermap = null;
		
		private static HashSet<String> aggregate_set = new HashSet<String>(){{
																		add("sum");
																		add("avg");
																		add("max");
																		add("min");
																		add("count");
																}};
		
																
																
		public Validation(){
			
			this.S = new ArrayList<String>(Arrays.asList(MFConfig.S)); // select
			this.N = MFConfig.N;
			this.V = new ArrayList<String>(Arrays.asList(MFConfig.V)); // group by
			this.F = new ArrayList<String>(Arrays.asList(MFConfig.F)); // function
			this.O = new ArrayList<String>(Arrays.asList(MFConfig.O)); // such that
			this.G = MFConfig.G;
			
		}
		
		
		public boolean isValid() throws Exception{
			try {
				isValidSelect();
				isValidFunctionVariables();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				throw new Exception(e.getMessage());
			}
			
			return true;
		}
		
		
		private boolean isValidSelect() throws Exception{
			int max_gv = -1;
			for(String str : S){
				if(!V.contains(str) && !F.contains(str)){
					throw new Exception("Illegal attribute in select statement ");
				}
				
				if(F.contains(str)){
					String group_ID = str.split("_")[0];
					if(Pattern.matches("[0-9]+", group_ID)){
						max_gv = Math.max(max_gv, Integer.parseInt(group_ID));
					}
				}
			}
			
			if(max_gv > N)
				throw new Exception("In your functions, there is no grouping variable " + max_gv);
			
			return true;
		}
		
		private boolean isValidFunctionVariables() throws Exception{
			
			//Traverse the Functions, check if all the aggregate functions are valid. Find the maximum ID of grouping variables
			int max_gv = -1;
			for(String func : F){
				String[] attr = func.split("_");
				if(Pattern.matches("[0-9]+", attr[0])){
					max_gv = Math.max(max_gv, Integer.parseInt(attr[0]));
					if(! aggregate_set.contains(attr[1])){
						throw new Exception("Wrong aggregate function in" + func);
					}
				}else{
					if(! aggregate_set.contains(attr[0])){
						throw new Exception("Wrong aggregate function in" + func);
					}
				}
			}
			
			//If the maximum ID of grouping variable exceeds N, throw an exception
			if(max_gv > N)
				throw new Exception("In your functions, there is no grouping variable " + max_gv);
			
			return true;
		}
}
