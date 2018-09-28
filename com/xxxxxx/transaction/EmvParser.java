package com.xxxxxx.transaction;

public class EmvParser {

	private static String emv_data=null;
	String emvback="";
	String emvfield="";
	int emvlength;
	String emv_name="";
	private static String emv_parameters[]={"82","84","91","95","9A","9C","5F2A","5F34","9F02","9F03","9F09","9F10","9F1A","9F1E","9F26","9F27","9F33","9F34","9F35","9F36","9F37"};
	public EmvParser(String emv_data) {
		this.emv_data=emv_data;
	}
	
	public String emvparse(String emv_data){
		
		
		for (int i=0;i<emv_parameters.length;i++){
			if(emv_data.startsWith(emv_parameters[i])){
				emvfield=emv_parameters[i];
				int emvfield_len=emvfield.length();
				emvlength=Integer.parseInt(emv_data.substring(emvfield_len, emvfield_len+2), 16);
				emv_name=emv_data.substring(emvfield_len+2, (emvfield_len+2)+(emvlength*2));
				//System.out.println("emv: "+emvfield+"  "+emv_name);
				
				emv_data=emv_data.substring((emvfield_len+2)+(emvlength*2), emv_data.length());
				//System.out.println("new emv : "+emv_data);
				emvback +="|"+emvfield+"|"+emv_name;
			}
			
		}
		
		
		
		return emvback+"|";
		
	}
	
}
