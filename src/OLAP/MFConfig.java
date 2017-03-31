package OLAP;

import java.util.ArrayList;

public class MFConfig {
	//List of projected attributes for the query output
	public static String[] S;
	//Number of grouping variables
	public static Integer N;
	//List of grouping attributes
	public static String[] V;
	//{F0, F1, ..., Fn}, list of sets of aggregate functions. Fi represents a list of aggregate functions for each grouping variable
	public static String[] F;
	//{σ0, σ1, ..., σn}, list of predicates to define the ranges for the grouping variables.
	public static String[] O;
	//Predicate for the having clause
	public static String G;
	
	public static void initConfig(String pr, String gv, String ga, String af, String pd, String hg){
		validation(pr, gv, ga, af, pd, hg);
		S = pr.split(",");
		N = Integer.parseInt(gv);
		V = ga.split(",");
		F = af.split(",");
		O = pd.split(",");
		G = hg;
	}
	
	public static void validation(String pr, String gv, String ga, String af, String pd, String hg){
		//TODO argument validation
	}
	
}
