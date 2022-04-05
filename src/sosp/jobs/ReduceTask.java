package sosp.jobs;

import sosp.network.Macroflow;

public class ReduceTask extends Task{
	
	// predefined constants
//	public Job _job = null;
	public int reducerId = -1;
	public Macroflow macroflow = null;
//	public double computationDelay = 0; // s
	public double predictFinishTime = -1;
	public double deadline = -1;
	
	
	// runtime constants (modified by only once)
//	public int host = -1;
//	public double startTime = -1;
	public double networkFinishTime = -1; // last macroflow finished
	public double computationFinishTime = -1; // all finished

	public int mfPriority = -1;
	
	public ReduceTask(Job parents,int id){
		_job = parents;
		reducerId = id;
	}
	
	public void emit(int hostId, double time){
		host = hostId;
		startTime = time;
		macroflow.start(time);
	}
	
	public void networkFinished(double time){
		networkFinishTime = time;
	}
	
	public void computationFinished(double time){
		computationFinishTime = time;
	}
}
