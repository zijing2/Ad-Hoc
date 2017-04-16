package OLAP;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

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
		
		public Validation(){
			
			this.S = new ArrayList<String>(Arrays.asList(MFConfig.S)); // select
			this.N = MFConfig.N;
			this.V = new ArrayList<String>(Arrays.asList(MFConfig.V)); // group by
			this.F = new ArrayList<String>(Arrays.asList(MFConfig.F)); // function
			this.O = new ArrayList<String>(Arrays.asList(MFConfig.O)); // such that
			this.G = MFConfig.G;
			
		}
}
