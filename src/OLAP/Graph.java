package OLAP;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Graph {
	
	public static HashMap<String, ArrayList<String>> graph;
	
	public static HashMap<String, Integer> id_table;
	
	public static ArrayList<String> zero_indegree_node_list;
	
	//result of topological sort
	public static String[] seq_arr;
	
	public static void initG(){
		graph = new HashMap<String, ArrayList<String>>();
		//init graph base on node nums
		for(int i=0;i<MFConfig.O.length;i++){
			ArrayList<String> node = new ArrayList<String>();
			graph.put(String.valueOf(i+1), node);
		}
		//draw diagram base on such that dependency
		for(int i=0;i<MFConfig.O.length;i++){
			char[] arr = MFConfig.O[i].toCharArray();
			for(int j=0;j<arr.length;j++){
				//find like avg(1.quant) in X2
				if((""+arr[j]).equals(".") && Character.isLetter(arr[j+1])){
					if(Integer.parseInt(""+arr[j-1])!=i+1){
						graph.get(""+arr[j-1]).add(String.valueOf(i+1));
					}
				}
			}
		}
		seq_arr = new String[MFConfig.O.length+1];
		//always scan X0 first
		seq_arr[0] = "0";
	}
	
	/*
	 * topological sort base on indegree
	 */
	public static void topoligicalSort() throws Exception{
		HashMap<String, ArrayList<String>> graph_clone = (HashMap<String, ArrayList<String>>) graph.clone();
		int i = 0;
		while(!graph_clone.isEmpty()){
			//compute indegree table
			updateIDTable(graph_clone);
			i++;
			//get the element who has 0 indegree and denote the num
			updateZeroIndegreeList();
			if(zero_indegree_node_list.size()==0){
				throw new Exception("dependency has a circle");
			}
			seq_arr[i] = zero_indegree_node_list.get(0);
			//remove this element and related edge
			removeNodeAndArc(graph_clone,zero_indegree_node_list.get(0));
		}
	}
	
	/*
	 * update indegree table
	 */
	public static void updateIDTable(HashMap<String, ArrayList<String>> graph_clone){
		id_table = new HashMap<String,Integer>();
		Iterator iter1 = graph_clone.entrySet().iterator();
		//init indegree table with all value 0
		while(iter1.hasNext()){
			Map.Entry entry1 = (Map.Entry)iter1.next();
			String key1 = entry1.getKey().toString();
			id_table.put(key1, 0);
		}
		//compute indegree table
		Iterator iter2 = graph_clone.entrySet().iterator();
		while(iter2.hasNext()){
			Map.Entry entry2 = (Map.Entry)iter2.next();
			String key2 = entry2.getKey().toString();
			ArrayList list = graph_clone.get(key2);
			for(int i=0;i<list.size();i++){
				id_table.put((String)list.get(i),id_table.get(list.get(i))+1);
			}
		}
	}
	
	/*
	 * get node with 0 indegree base on indegree table
	 */
	public static void updateZeroIndegreeList(){
		zero_indegree_node_list = new ArrayList<String>();
		Iterator iter = id_table.entrySet().iterator();
		//init indegree table with all value 0
		while(iter.hasNext()){
			Map.Entry entry = (Map.Entry)iter.next();
			String key = entry.getKey().toString();
			if(id_table.get(key)==0){
				zero_indegree_node_list.add(key);
			}
		}
	}
	
	/*
	 * remove particular node and arc in graph
	 */
	public static void removeNodeAndArc(HashMap<String, ArrayList<String>> graph_clone,String node){
		graph_clone.remove(node);
		Iterator iter = graph_clone.entrySet().iterator();
		while(iter.hasNext()){
			Map.Entry entry = (Map.Entry)iter.next();
			String key = entry.getKey().toString();
			ArrayList<String> list = graph_clone.get(key);
			for(int i=0;i<list.size();i++){
				if(list.get(i).equals(node)){
					list.remove(i);
				}
			}
		}
	}
	
	/*
	 * test graph class
	 */
	public static void main(String[] args){
		//System.out.println(MFConfig.O);
		String pr = "1.cust=cust, 2.quant>avg(1.quant) , 3.quant>sum(1.quant) ,"
				+ " 4.quant<avg(3.quant) and 4.quant>sum(2.quant) and 4.quant>avg(6.quant), 5.quant<sum(2.quant) and "
				+ "5.quant>avg(3.quant) and 5.quant<avg(6.quant), 6.month=month";
		MFConfig.initO(pr);
		initG();
		try{
			topoligicalSort();
		}catch(Exception e){
			System.out.println(e);
		}
		for(int i=0;i<seq_arr.length;i++){
			System.out.println(seq_arr[i]);
		}
	}
	
}
