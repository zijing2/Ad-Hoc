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
		S = pr.replaceAll("\\s", "").split(",");
		N = Integer.parseInt(gv.replaceAll("\\s", ""));
		V = ga.replaceAll("\\s", "").split(",");
		F = af.replaceAll("\\s", "").split(",");
		O = pd.replaceAll("\\s", "").split(",");
		G = hg.replaceAll("\\s", "");
	}
	
	public static void validation(String pr, String gv, String ga, String af, String pd, String hg){
		//TODO argument validation
	}
	
}
