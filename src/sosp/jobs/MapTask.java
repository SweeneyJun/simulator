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

	public double inputSize = -1;
	public double inputFinishTime = -1;  // TODO 区分当前MapTask是否该运行? 如果在Input结束直接调用运行的话好像用不着这个变量, 先留着看吧
	public double allocatedInputBw = -1; // 结束Input阶段以后归还使用
	
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
