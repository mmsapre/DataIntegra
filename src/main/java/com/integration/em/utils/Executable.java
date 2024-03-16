package com.integration.em.utils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.util.List;
public class Executable {

    @Parameter
    protected List<String> params;
    
    public List<String> getParams() {
		return params;
	}
    
    private boolean silent = false;
	public void setSilent(boolean silent) {
		this.silent = silent;
	}
    
    protected boolean parseCommandLine(Class<?> cls, String... args) {
        try {
        	if(!silent) {
	        	System.err.println(BuildInfo.getJarPath(getClass()));
	        	System.err.println(String.format("%s version %s", cls.getName(), BuildInfo.getBuildTimeString(cls)));
        		System.err.println("		 __      __.___        __                     ");
        		System.err.println("		 /  \\    /  \\   | _____/  |_  ____     _______ ");
        		System.err.println("		 \\   \\/\\/   /   |/    \\   __\\/ __ \\    \\_  __ \\");
        		System.err.println("		  \\        /|   |   |  \\  | \\  ___/     |  | \\/");
        		System.err.println("		   \\__/\\  / |___|___|  /__|  \\___  > /\\ |__|   ");
        		System.err.println("		        \\/           \\/          \\/  \\/        ");
        	}
        	
            @SuppressWarnings("unused")
			JCommander cmd = new JCommander(this, args);
            
            return true;
        } catch(Exception e) {
        	System.err.println(e.getMessage());
        	usage(args);
            return false;
        }
    }
    
    protected void usage(String... args) {
    	System.out.println(TblStringUtils.join(args, " "));
        JCommander cmd = new JCommander(this);
        cmd.usage();
    }
	
}
