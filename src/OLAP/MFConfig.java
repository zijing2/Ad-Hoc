package OLAP;

import java.util.ArrayList;

public class MFConfig {
	//List of projected attributes for the query output
	public static String[] S = null;
	//Number of grouping variables
	public static Integer N = null;
	//List of grouping attributes
	public static String[] V = null;
	//{F0, F1, ..., Fn}, list of sets of aggregate functions. Fi represents a list of aggregate functions for each grouping variable
	public static String[] F = null;
	//{σ0, σ1, ..., σn}, list of predicates to define the ranges for the grouping variables.
	public static String[] O = null;
	//Predicate for the having clause
	public static String G = null;
	
	/*
	 * initial configuration of MFStruture
	 */
	public static void initConfig(String pr, String gv, String ga, String af, String pd, String hg){
		S = pr.split(",");
		N = Integer.parseInt(gv);
		V = ga.split(",");
		F = af.split(",");
		O = pd.split(",");
		G = hg;
	}
	
	public static void initS(String pr){
		S = pr.split(",");
		for(int i=0;i<S.length;i++){
			S[i] = S[i].trim();
		}
	}
	
	public static void initN(String gv){
		N = Integer.parseInt(gv);
	}
	
	public static void initV(String ga){
		V = ga.split(",");
		for(int i=0;i<V.length;i++){
			V[i] = V[i].trim();
		}
	}
	
	public static void initF(String af){
		F = af.split(",");
		for(int i=0;i<F.length;i++){
			F[i] = F[i].trim();
		}
	}
	
	public static void initO(String pd){
		O = pd.split(",");
		for(int i=0;i<O.length;i++){
			O[i] = O[i].trim();
		}
	}
	
	public static void initG(String hg){
		G = hg;
	}
	
	/*
	 * validation of number of GROUPING VARIABLES
	 */
	public static void checkN(String N) throws Exception{
		if(N.length()==0){
			throw new Exception("number of grouping variable can not be empty");
		}
		int n;
		try{
			 n = Integer.parseInt(N);
		}catch(NumberFormatException e ){
			throw new Exception("number of grouping variable must be integer");
		}
		if(n < 0){
			throw new Exception("number of grouping variable can't be a negatives");
		}
	}
	
	/*
	 * validation of grouping attributes
	 */
	public static void checkV(String V) throws Exception{
		if(V.length()==0){
			throw new Exception("grouping attributes can not be empty");
		}
		String[] ga = V.split(",");
		for(int i=0; i< ga.length; i++){
			ga[i] = ga[i].trim();
		}
		for(int i=0; i < ga.length; i++){
			if(!TableSchema.isTableAttribute(ga[i])){
				throw new Exception("grouping attribute name invalid");
			}
		}
	}
	
	/*
	 * validation of aggregate function
	 */
	public static void checkF(String F) throws Exception{
		if(F.length()==0){
			throw new Exception("aggregate function can not be empty");
		}
		String[] f_arr = F.split(",");
		for(int i=0; i < f_arr.length; i++){
			f_arr[i] = f_arr[i].trim();
		}
		for(int i=0; i < f_arr.length; i++){
			if(f_arr[i].length()==0){
				throw new Exception("single aggregate function can't be empty");
			}else{
				checkAggregateFormat(f_arr[i]);
			}
		}
	}
	
	
	/*
	 * validation of selection SELECT ATTRIBUTE(S):
	 */
	public static void checkS(String S) throws Exception{
		if(S.length()==0){
			throw new Exception("select attributes can not be empty");
		}
		String[] pr = S.split(",");
		for(int i=0; i < pr.length; i++){
			pr[i] = pr[i].trim();
		}
		
		for(int i=0; i<pr.length; i++){
			if(pr[i].length()==0){
				throw new Exception("single selection can't be empty");
			}else{
				if(!isGroupingAttribute(pr[i])){
					checkAggregateFormat(pr[i]);
				}
			}
		}
	}
	
	/*
	 * validation of sigma
	 */
	public static void checkO(String O) throws Exception{
		if(O.length()==0){
			throw new Exception("such that can not be empty");
		}
		
		
	}
	
	/*
	 * validation of having clause
	 */
	public static void checkG(String G) throws Exception{
		if(G.length()==0){
			return;
		}
		
	}
	
	/*
	 * check if it is one of grouping attributes
	 */
	private static boolean isGroupingAttribute(String s){
		for(int i=0; i < MFConfig.V.length; i++){
			if(s.equalsIgnoreCase(MFConfig.V[i])){
				return true;
			}
		}
		return false;
	}
	
	/*
	 * if string not match format like 1_quant_sum, then return false.
	 */
	private static void checkAggregateFormat(String s) throws Exception{
		String[] gv_af_at = s.split("_");
		int gv = Integer.parseInt(gv_af_at[0]);
		if(gv < 0 || gv > MFConfig.N){
			throw new Exception("aggregate function is invalid: grouping variable invalid");
		}
		if(!gv_af_at[1].equalsIgnoreCase("sum") && !gv_af_at[1].equalsIgnoreCase("count") && !gv_af_at[1].equalsIgnoreCase("avg") && !gv_af_at[1].equalsIgnoreCase("max") && !gv_af_at[1].equalsIgnoreCase("min") ){
			throw new Exception("aggregate function is invalid: aggregate function name invalid ");
		}
		if(!TableSchema.isTableAttribute(gv_af_at[2])){
			throw new Exception("aggregate function is invalid: column name invalid");
		}
	}
	
	/*
	 * bind all grouping attributes as a key
	 */
	public static String bindGA(ArrayList list){
		StringBuffer buff = new StringBuffer();
		buff.append(list.get(0));
		
		for(int i=1;i<list.size();i++){
			buff.append("-");
			buff.append(list.get(i));
			i++;
		}
		return buff.toString();
	}
	
	public static String[] debindGA(String s){
		return s.split("-");
	}
	
	
	
	
}
