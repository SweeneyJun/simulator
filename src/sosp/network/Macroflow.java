package sosp.network;

import sosp.jobs.ReduceTask;
import sosp.main.Settings;
import sosp.main.Topology;

public class Macroflow {
	// predefined constants
	public ReduceTask _reducer = null;
	public Coflow _coflow = null;
	
	public Flow[] flows = null; 
	public double size = 0; // normalized
	
	// runtime consts
	public double startTime = -1;
	public double finishTime = -1;
	
	
	public Macroflow(Coflow parents, ReduceTask brothers){
		_reducer = brothers;
		_coflow = parents;
	}
	
	public boolean isAllFlowsFinished_const(){
		for(Flow flow:flows)
			if(flow.finishTime<0)
				return false;
		return true;
	}
	
	public void setFlowSize() {
		int totalMappers = _coflow._job.nMappers;
		for(int i=0;i<Settings.nHosts;++i){
			Flow flow = flows[i];
			// normalized
			flow.size = size * _coflow._job.emittedMapperList[i].size() / totalMappers;
		}
	}
	
	public void start(double time){
		assert(Settings.nHosts == flows.length && _coflow._job.emittedMapperList.length == flows.length);
		startTime = time;
		//int totalMappers = _coflow._job.nMappers;
		for(int i=0;i<Settings.nHosts;++i){
			Flow flow = flows[i];
			flow.startTime = time;
			flow.receiver = _reducer.host;
			flow.route = Topology.getPath(flow.sender, flow.receiver);
			if(flow.route.length==0)
				flow.finishTime = time;
			if(flow.size < Settings.epsilon)
				flow.finishTime = time;
		}
	}
	
	public void finish(double time){
		finishTime = time;
	}
}
