package EngineTest;
import javax.script.*;

public class Test {
	public static void main(String[] args){
		Test t = new Test();
		t.param();
	}
	
	public void param(){
		ScriptEngineManager manager = new ScriptEngineManager();  
        ScriptEngine engine = manager.getEngineByName("js");  
        engine.put("a", 4);  
        engine.put("b", 6);  
        try {  
            Object maxNum = engine.eval("function max_num(a,b){return (a>b)?a:b;}max_num(a,b);");  
            System.out.println("max_num:" + maxNum);  
        } catch (Exception e) {  
            e.printStackTrace();  
        } 
	}
}
