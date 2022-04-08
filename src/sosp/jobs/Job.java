package sosp.jobs;

import java.util.ArrayList;

import sosp.main.Settings;
import sosp.network.Coflow;
import sosp.network.Macroflow;

public class Job {

	public JobQueue jobQueue;

	// predefined constants
	public int jobId = -1;
	public int userId = 0;

	//public int[] hdfsHosts = null; // input file position of mappers (default: 3 replications) 
	public Coflow coflow = null;
	public double arriveTime = -1;
	public int nMappers = -1;
	public int nReducers = -1;
	
	// runtime constants (modified by only once)
	public double mapStageFinishTime = -1;
	public double reduceStageFinishTime = -1;
	public double estimatedRunningTime = 0;
	
	// runtime variables (modified by many times)
	public int nActiveMappers = 0; 
	public int nActiveReducers = 0;
	public ArrayList<MapTask> pendingMapperList = null;
	public ArrayList<ReduceTask> pendingReducerList = null;
	public ArrayList<MapTask>[] emittedMapperList = null; // the j-th mapper on the i-th host, see $hdfsHosts$ for host number
	public ArrayList<ReduceTask> emittedReducerList = null;

	public double mapInputSize = 0;
	
	// global static variables
	public static int nArrivedJobs = 0;
	
	
	// functions
	public Job(int id){
		jobId = id;
	}
	
	public int nActiveTasks_const(){
		return nActiveMappers + nActiveReducers;
	}
	
	public boolean hasPendingTasks_const(){
		return pendingMapperList.size()>0 || pendingReducerList.size()>0;
	}
	
	public int countReadyTasks_const() {
		return mapStageFinishTime<=0 ? pendingMapperList.size() : pendingReducerList.size();
	}
	
	public boolean hasPendingMappers_const(){
		return pendingMapperList.size()>0;
	}
	
	public void oneMapperStarted(int host, MapTask mapper){
		++nActiveMappers;
		assert(pendingMapperList.remove(mapper));
		//for(int i=0;i<Settings.nReplications;++i){
		//	if(hdfsHosts[i]==host){
				emittedMapperList[host].add(mapper);
		//		return;
		//	}
		//}
		//assert(false); // this host has no input file
	}
	
	public void oneMapperFinished(){
		--nActiveMappers;
	}
	
	public void oneReducerStarted(int host, ReduceTask reducer){
		++nActiveReducers;
		assert(pendingReducerList.remove(reducer));
		emittedReducerList.add(reducer);
	}
	
	public void oneReducerFinished(){
		--nActiveReducers;
	}
	
	public void mapStageFinish(double time){
		mapStageFinishTime = time;
		for(Macroflow mf:coflow.macroflows)
			mf.setFlowSize();
	}
	
	public void reduceStageFinish(double time){
		reduceStageFinishTime = time;
	}
	
	public boolean isAllMapperFinished_const(){
		if(pendingMapperList.size()>0)
			return false;
		for(ArrayList<MapTask> list:emittedMapperList)
			for(MapTask m:list)
				if(m.finishTime<0)
					return false;
		return true;
	}
	
	public boolean isAllReducerFinished_const(){
		if(pendingReducerList.size()>0)
			return false;
		for(ReduceTask r:emittedReducerList)
			if(r.computationFinishTime<0)
				return false;
		return true;
	}

	public boolean isAllShuffleFinished_const(){
		if(pendingReducerList.size()>0)
			return false;
		for(ReduceTask r:emittedReducerList)
			if(r.networkFinishTime<0)
				return false;
		return true;
	}

}


/* JOB STRUCTURE
* 	Job
*	|-	mappers[]
*	|	|-	mapper
*	|-	reducers[]
*	|	|-	reducer
*	|	|	|-	macroflow
*	|	|		|-	flows[]
*	|	|			|-	flow
*	|-	coflow
*		|-	macroflows[]
*			|-	macroflow
*				|-	flows[]
*					|-	flow
*/