package sosp.network;

import sosp.jobs.Job;

public class Coflow {
	// predefined constants
	public Job _job = null;
	
	public Macroflow[] macroflows = null; // note that macroflow index is not host ID
	public double size = 0; // normalized
	
	// runtime constants (modified by only once)
	public double startTime = -1;
	public double finishTime = -1; // shuffle-finish-time of a job
	
	// functions
	
	public Coflow(Job colleagues){
		_job = colleagues;
	}
	
	public boolean isAllMacroflowsFinished_const(){
		for(Macroflow mf:macroflows)
			if(mf.finishTime<0)
				return false;
		return true;
	}
	
	public void start(double time){
		startTime = time;
	}
	
	public void finish(double time){
		finishTime = time;
	}
}
