package OLAP;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class IO {
	
	/*
	 * prompt user to input 6 operand
	 */
	public static void prompt() throws endProgramExeption{
		String gv = null, ga = null, af = null
				,pr = null, pd = null, hg = null;
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		while(MFConfig.F==null||MFConfig.G==null||MFConfig.N==null
				||MFConfig.O==null||MFConfig.S==null||MFConfig.V==null){
			try {
				//validate number of grouping variables
				if(MFConfig.N==null){
					System.out.println("NUMBER OF GROUPING VARIABLES(n):");
					gv = br.readLine();
					isEndProgram(gv);
					MFConfig.checkN(gv);
					MFConfig.initN(gv);
				}
				
				//validate grouping attribute
				if(MFConfig.V==null){
					System.out.println("GROUPING ATTRIBUTES(V): for example(cust,prod)");
					ga = br.readLine();
					isEndProgram(ga);
					MFConfig.checkV(ga);
					MFConfig.initV(ga);
				}
				
				//validate aggregate function
				if(MFConfig.F==null){
					System.out.println("F-VECT([F]): for example(1_sum_quant, 1_avg_quant, 2_sum_quant)");
					af = br.readLine();
					isEndProgram(af);
					MFConfig.checkF(af);
					MFConfig.initF(af);
				}
				
				//validate select attributes
				if(MFConfig.S==null){
					System.out.println("SELECT ATTRIBUTE(S): for example(cust,prod,1_sum_quant,2_sum_quant)");
					pr = br.readLine();
					isEndProgram(pr);
					MFConfig.checkS(pr);
					MFConfig.initS(pr);
				}
				
				//validate "such that" AKA sigma
				String temp;
				if(MFConfig.O==null){
					pd = "";
					for(int i=1; i <= Integer.parseInt(gv);i++){
						System.out.println("SELECT CONDITION-VECT([σ]) for groupping variable X" + i + ": for example(1.cust=cust and 1.state = 'NY')");
						temp = br.readLine();
						MFConfig.checkO(temp);
						if(i<Integer.parseInt(gv)){
							pd += temp + ",";
						}else{
							pd += temp;
						}
					}
					MFConfig.initO(pd);
				}
				
				
				//validate having clause
				System.out.println("HAVING_CONDITION(G):");
				hg = br.readLine();
				MFConfig.checkG(hg);
				MFConfig.initG(pd);
				
				//MFConfig.initConfig(pr, gv, ga, af, pd, hg);
				
				//System.out.println(MFConfig.S[0]);

			}catch(IOException e1){
				System.out.print(e1);
			}catch(endProgramExeption e2){
				throw new endProgramExeption();
			}catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	
	/*
	 * format and output result base on MFStructure
	 */
	public static void render(HashMap<String, HashMap<String, String>> map){
		System.out.println(map);
		String table_header = "";
		//display table header
		for(int i=0; i<MFConfig.S.length; i++){
			String temp = String.format("%-10s", MFConfig.S[i]);
			table_header += temp + "\t";
		}
		System.out.println(table_header);
		Iterator iter1 = map.entrySet().iterator();
		while(iter1.hasNext()) {
				String output_line_ga = "";
				Map.Entry entry1 = (Map.Entry)iter1.next();
				String ga = entry1.getKey().toString();
				String[] ga_arr = MFConfig.debindGA(ga);
				for(int i=0;i<ga_arr.length;i++){
					output_line_ga += String.format("%-10s", ga_arr[i]) + "\t";
				}
				//get grouping variables
				String output_line_gv = "";
				for(int i=0;i<MFConfig.S.length;i++){
					HashMap<String, String> map2 = map.get(ga);
					Iterator iter2 = map2.entrySet().iterator();
					while(iter2.hasNext()){
						Map.Entry entry2 = (Map.Entry)iter2.next();
						String gv = entry2.getKey().toString();
							if(gv.equalsIgnoreCase(MFConfig.S[i])){
								output_line_gv += String.format("%"+ String.valueOf(MFConfig.S[i].length()) +"s", map2.get(gv)) + "\t";
							}
					}
				}
				System.out.println(output_line_ga + output_line_gv);
				//System.out.println(output_line_ga);
	           
		}
		
		
	}
	
	public static void isEndProgram(String s) throws endProgramExeption{
		if(s.equalsIgnoreCase("quit")){
			throw new endProgramExeption();
		}
	}
	
}
