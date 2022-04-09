package sosp.main;

import sosp.algorithm.Max3Reducer;
import sosp.jobs.ReduceTask;

public class HostInfo {
    public int hostId;
    public double hostShuffleSize;
    public double minCoflowSize;
    public int shuffleReducerNum;
    public ReduceTask[] shuffleReducer;
    public double shuffleTime;
    public int freeSlots;

    public static double hostMaxMinCoflowSize;
    public static int hostMaxReducerNum;

    public HostInfo(int hostId) {
        this.hostId = hostId;
        this.shuffleReducerNum = 0;
        this.hostShuffleSize = 0;
        this.minCoflowSize = 0;
        if(hostId < Settings.nHosts) {
            this.freeSlots = Settings.nSlots;
        }
        else if(hostId >= Settings.nHosts){
            this.freeSlots = 0;
        }
        else{
            assert (false); // should not go here
        }
//        if (Settings.algo instanceof Max3Reducer) {
//            shuffleReducer = new ReduceTask[Settings.reduceNum];
//        }
//        else{
            shuffleReducer = new ReduceTask[Settings.nSlots];
//        }
    }

    public void updateHostInfo() {
        if (shuffleReducerNum != 0) {
            for (int i = 0; i < shuffleReducerNum; i++) {
                if (i == 0) {
                    minCoflowSize = shuffleReducer[i]._job.coflow.size;
                } else {
                    if (minCoflowSize > shuffleReducer[i]._job.coflow.size) {
                        minCoflowSize = shuffleReducer[i]._job.coflow.size;
                    }
                }
            }
        }
        else {
            minCoflowSize = 0;
        }
        shuffleTime = hostShuffleSize / Settings.speed;
        if (shuffleTime == 0) {
            shuffleTime = Double.MAX_VALUE;
        }
    }
}
