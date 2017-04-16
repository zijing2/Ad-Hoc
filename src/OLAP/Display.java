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
	
	
	public void display(){
		int[] mapping = new int[S.size()];
		
		for(int i = 0; i < mapping.length; i++){
			mapping[i] = V.indexOf(S.get(i));
		}
		
		StringBuffer sb = new StringBuffer();
		
		for(Map.Entry<String, HashMap<String, Integer>> entry : outermap.entrySet()){
			String[] groupbys = entry.getKey().split("-");
			
			//Scan the mapping array, if -1 then it is a function in hash map, otherwise it is in groupbys
			for(int i = 0; i < mapping.length; i++){
				if(mapping[i] != -1){
					sb.append(formatString(groupbys[mapping[i]])).append("     ");
				}else{
					
					if (entry.getValue().get(S.get(i)) != null){
						sb.append(formatString(Integer.toString(entry.getValue().get(S.get(i))))).append("     ");
					}else{
						sb.append(formatString("null")).append("     ");
					}
				}
			}
			System.out.println(sb.toString());
			sb.setLength(0);
		}

	}
	
	
	private String formatString(String str){
		if(Pattern.matches("[0-9]+", str) || str.equals("null")){
			return String.format("%-12s", str);
		}else{
			return String.format("%12s", str);
		}
	}

}
