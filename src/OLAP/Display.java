package OLAP;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.*;

public class Display {
	
	
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
	
	public Display(HashMap<String, HashMap<String, Integer>> outermap){
		
		this.S = new ArrayList<String>(Arrays.asList(MFConfig.S)); // select
		this.N = MFConfig.N;
		this.V = new ArrayList<String>(Arrays.asList(MFConfig.V)); // group by
		this.F = new ArrayList<String>(Arrays.asList(MFConfig.F)); // function
		this.O = new ArrayList<String>(Arrays.asList(MFConfig.O)); // such that
		this.G = MFConfig.G;
		this.outermap = outermap;
	}
	
	
	
	
	/*
	 * @Param: null
	 * @Return: null
	 * @Function: Display the attributes in the order specified in select statement. The output format is justified.
	 * 
	 */
	
	public void display(){
		
		// Create a mapping array on the select statement. For each item in select statement, find the corresponding index inside group by attributes
		// If this item is not inside group by attribute, it has to be a function, we put a -1 into this mapping array.
		int[] mapping = new int[S.size()];
		
		for(int i = 0; i < mapping.length; i++){
			mapping[i] = V.indexOf(S.get(i));
		}
		
		
		//A string buffer to store the formatted string for each row inside outer hashmap.
		StringBuffer sb = new StringBuffer();
		
		//For each row in outer hashmap, we split the key into array of group by attributes.
		// Then iterate the mapping array. If this item is a group by attribute, look up in the above array, print out the formatted group by attributes
		// Otherwise use this function name in the select statement, look up the value in the inner hashmap.
		for(Map.Entry<String, HashMap<String, Integer>> entry : outermap.entrySet()){
			String[] groupbys = entry.getKey().split("-");
			
			//Scan the mapping array, if -1 then it is a function in hash map, otherwise it is in groupbys
			for(int i = 0; i < mapping.length; i++){
				if(mapping[i] != -1){
					sb.append(formatString(groupbys[mapping[i]])).append("|     ");
				}else{
					
					if (entry.getValue().get(S.get(i)) != null){
						sb.append(formatString(Integer.toString(entry.getValue().get(S.get(i))))).append("|     ");
					}else{
						sb.append(formatString("null")).append("|     ");
					}
				}
			}
			System.out.println(sb.toString());
			sb.setLength(0);
		}

	}
	
	
	private String formatString(String str){
		if(Pattern.matches("[0-9]+", str) || str.equals("null")){
			return String.format("%12s", str);
		}else{
			return String.format("%-12s", str);
		}
	}

}
