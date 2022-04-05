package sosp.network;

public class Flow {
	// predefined constants
	public Coflow _coflow = null;
	public Macroflow _macroflow = null;
	
	public int flowId = -1;
	public double size = 0; // normalized
	
	// runtime variables
	public double allocatedBw = 0; // allocated bandwidth in each max-min iteration
	public double sentSize = 0; // total sent size
	
	// runtime consts
	public int sender = -1;
	public int receiver = -1;
	public double startTime = -1;
	public double finishTime = -1;
	public int[] route = null; // link sequences
	
	
	public Flow(Macroflow parents, Coflow parents2, int id){
		_macroflow = parents;
		_coflow = parents2;
		flowId = id;
	}
	
	public long getUniqueFlowId_const(){
		long id = _coflow._job.jobId;
		id = id*1000000 + _macroflow._reducer.reducerId;
		id = id*1000000 + flowId;
		return id;
	}
	
	public void Finish(double time){
		finishTime = time;
	}
}
