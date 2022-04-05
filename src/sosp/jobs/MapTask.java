package sosp.jobs;

public class MapTask extends Task{
	
	// predefined constants
//	public Job _job = null;
	public int mapperId = -1;
	public int[] hdfsHost = null;
	
	// runtime constants (modified by only once)
//	public int host = -1;
//	public double startTime = -1;
	public double finishTime = -1;
	public double predictComputationDelay = 0;
	
	public MapTask(Job parents, int id){
		_job = parents;
		mapperId = id;
	}
	
	public void emit(int hostId, double time){
		host = hostId;
		startTime = time;
	}
	
	public void finish(double time){
		finishTime = time;
	}
}
