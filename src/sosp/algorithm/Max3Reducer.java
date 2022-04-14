package sosp.algorithm;

import jdk.nashorn.internal.ir.IfNode;
import sosp.jobs.*;
import sosp.main.HostInfo;
import sosp.main.Priority;
import sosp.main.TestPushBox;
import sosp.main.Settings;
import sosp.network.Flow;

import java.util.*;

public class Max3Reducer implements Algorithm {

    private static Random error = new Random(2);

    @Override
    public HostAndTask allocateHostAndTask() {

//        Job chosenJob = Settings.fairJobTestPushBox ? Algorithm.fairJobSelector() : Algorithm.smallestJobSelector();
//        Job[] chosenJobs = Settings.fairJobTestPushBox ? Algorithm.jobSorterFair():Algorithm.jobSorterFifo();
//        Job[] chosenJobs = Algorithm.jobSorterSJF();

        ArrayList<JobQueue> jobQueues = new ArrayList<>();
        jobQueues = TestPushBox.jobQueues;

        Collections.sort(jobQueues, new Comparator<JobQueue>() {
            @Override
            public int compare(JobQueue o1, JobQueue o2) {
                return Double.compare(o1.resourceRatio(), o2.resourceRatio());
            }
        });

        for (JobQueue jobQueue: TestPushBox.jobQueues) {
            if (1.0*jobQueue.nActiveTasks()/Settings.nSlots/Settings.nHosts > jobQueue.maxResource) {
                continue;
            }
            Job[] chosenJobs = jobQueue.jobSorter();
            Task chosenTask = null;
            int giveUp = 0;
            double alpha = Math.pow(Settings.sizeEstimationError, 1-2*error.nextDouble());
//            double alpha = 0;
//            if (alpha < 1){
//                alpha = 1;
//            }
            for (Job chosenJob : chosenJobs) {
                if (chosenJob.hasPendingMappers_const()) {
                    for (MapTask mt : chosenJob.pendingMapperList) {
                        for (int i = 0; i < mt.hdfsHost.length; ++i) {
                            if (giveUp == 0) { // wcx: slotYield = 0 in Algorithm 2, line 2
                                if (TestPushBox.freeSlots[mt.hdfsHost[i]] > 0) {
                                    return new HostAndTask(mt.hdfsHost[i], mt);
                                }
                            } else { // wcx: notHinderTask test in Algorithm 2, line 12 - 13
                                if (TestPushBox.freeSlots[mt.hdfsHost[i]] > 0 && TestPushBox.hostInfos[mt.hdfsHost[i]].shuffleReducerNum == 0) {
                                    return new HostAndTask(mt.hdfsHost[i], mt);
                                }
                                if (TestPushBox.freeSlots[mt.hdfsHost[i]] > 0 && TestPushBox.hostInfos[mt.hdfsHost[i]].shuffleReducerNum != 0 && mt.computationDelay <= TestPushBox.hostInfos[mt.hdfsHost[i]].shuffleTime * alpha) {
                                    return new HostAndTask(mt.hdfsHost[i], mt);
                                }
                            }
                        }
                    }
                    //                chosenTask = chosenJob.pendingMapperList.get(0);
                    //                // host
                    //                int host = -1;
                    //                for (int i = 0; i < TestPushBox.freeSlots.length; ++i) {
                    //                    if (TestPushBox.freeSlots[i] == 0)
                    //                        continue;
                    //                    host = (host < 0) ? i : host;
                    //                    if (TestPushBox.hostInfos[i].shuffleTime == 0 || TestPushBox.hostInfos[host].shuffleTime < TestPushBox.hostInfos[i].shuffleTime)
                    //                        host = i;
                    //                }
                    //                if (host < 0)
                    //                    return null;
                    //                else {
                    ////                    if (giveUp == 0) {
                    ////                        return new HostAndTask(host, chosenTask);
                    ////                    }
                    ////                    else {
                    //                        if (TestPushBox.hostInfos[host].shuffleReducerNum == 0) {
                    //                            return new HostAndTask(host, chosenTask);
                    //                        }
                    //                        if (TestPushBox.hostInfos[host].shuffleReducerNum != 0 && chosenTask.computationDelay < TestPushBox.hostInfos[host].shuffleTime * alpha) {
                    //                            return new HostAndTask(host, chosenTask);
                    //                        }
                    ////                    }
                    //                }
                }
                if (chosenJob.mapStageFinishTime > 0 && chosenJob.hasPendingTasks_const()) { // wcx: need to chose reducer task
                    chosenTask = chosenJob.pendingReducerList.get(0); // wcx: get one reducer

                    HostInfo[] tempHostInfos = new HostInfo[Settings.nHosts];
                    for (int i = 0; i < Settings.nHosts; i++) {
                        tempHostInfos[i] = TestPushBox.hostInfos[i];
                    }

                    Arrays.sort(tempHostInfos, new Comparator<HostInfo>() {
                        @Override
                        public int compare(HostInfo o1, HostInfo o2) {
                            if (o1.hostShuffleSize != o2.hostShuffleSize) {
                                return Double.compare(o1.hostShuffleSize, o2.hostShuffleSize);
                            } else {
                                return o1.shuffleReducerNum - o2.shuffleReducerNum;
                                //                            return Double.compare(o2.minCoflowSize, o1.minCoflowSize);
                            }
                        }
                    });

                    if (jobQueue.mode.equals("FIFO")) {
                        if (jobQueue.canTransfer(chosenJob)) {
                            if (chosenJob.coflow.size >= HostInfo.hostMaxMinCoflowSize) {
                                if (tempHostInfos[0].shuffleReducerNum >= Settings.reduceNum || TestPushBox.freeSlots[tempHostInfos[0].hostId] == 0) {
                                    giveUp = 1;
                                    continue;
                                }
                                return new HostAndTask(tempHostInfos[0].hostId, chosenTask);
                            } else {
                                for (int i = 0; i < Settings.nHosts; i++) {
                                    if (chosenJob.coflow.size < tempHostInfos[i].minCoflowSize && TestPushBox.freeSlots[tempHostInfos[i].hostId] != 0) {
                                        return new HostAndTask(tempHostInfos[i].hostId, chosenTask);
                                    }
                                }
                            }
                        }
                    }
                    else{
                        if (chosenJob.coflow.size >= HostInfo.hostMaxMinCoflowSize) {
                            if (tempHostInfos[0].shuffleReducerNum >= Settings.reduceNum || TestPushBox.freeSlots[tempHostInfos[0].hostId] == 0) {
                                giveUp = 1;
                                continue;
                            }
                            return new HostAndTask(tempHostInfos[0].hostId, chosenTask);
                        } else {
                            for (int i = 0; i < Settings.nHosts; i++) {
                                if (chosenJob.coflow.size < tempHostInfos[i].minCoflowSize && TestPushBox.freeSlots[tempHostInfos[i].hostId] != 0) {
                                    return new HostAndTask(tempHostInfos[i].hostId, chosenTask);
                                }
                            }
                        }
                    }
                }
            }
            for (Job chosenJob : chosenJobs) {
                if (chosenJob.hasPendingMappers_const()) {
                    chosenTask = chosenJob.pendingMapperList.get(0);
                    // host
                    HostInfo[] tempHostInfos = new HostInfo[Settings.nHosts];
                    for (int i = 0; i < Settings.nHosts; i++) {
                        tempHostInfos[i] = TestPushBox.hostInfos[i];
                    }

                    Arrays.sort(tempHostInfos, new Comparator<HostInfo>() {
                        @Override
                        public int compare(HostInfo o1, HostInfo o2) {
                            if (o1.shuffleTime != o2.shuffleTime) {
                                return Double.compare(o2.hostShuffleSize, o1.hostShuffleSize);
                            } else {
                                return o2.freeSlots - o1.freeSlots;
                                //                            return Double.compare(o2.minCoflowSize, o1.minCoflowSize);
                            }
                        }
                    });
                    int host = -1;
                    for (int i = 0; i < Settings.nHosts; i++) {
                        if (tempHostInfos[i].freeSlots != 0) {
                            host = tempHostInfos[i].hostId;
                        }
                    }
                    if (host < 0) {
                        return null;
                    } else {
                        if (giveUp == 0) {
                            return new HostAndTask(host, chosenTask);
                        } else {
                            if (TestPushBox.hostInfos[host].shuffleReducerNum == 0) {
                                return new HostAndTask(host, chosenTask);
                            }
                            if (TestPushBox.hostInfos[host].shuffleReducerNum != 0 && ((MapTask) chosenTask).predictComputationDelay < TestPushBox.hostInfos[host].shuffleTime) {
                                return new HostAndTask(host, chosenTask);
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    @Override
    public void releaseHost(HostAndTask ht) {
        // do nothing
    }

    @Override
    public ArrayList<Flow>[] getPriority() {
        int nPriority = Settings.reduceNum;
        @SuppressWarnings("unchecked")
        ArrayList<Flow>[] activeFlows = new ArrayList[nPriority];
        for(int i=0;i<activeFlows.length;++i)
            activeFlows[i] = new ArrayList<Flow>();

        for (int i = 0; i < Settings.nHosts; i ++) {
            for (int j = 0; j < TestPushBox.hostInfos[i].shuffleReducerNum; j ++) {
                for(Flow flow: TestPushBox.hostInfos[i].shuffleReducer[j].macroflow.flows) {
                    if(flow.finishTime>=0)
                        continue;
                    if (nPriority - j - 1 >= 0){
                        activeFlows[nPriority - j - 1].add(flow);
                    }
                    else {
                        activeFlows[0].add(flow);
                    }
                }
            }
        }
        return activeFlows;
//        return Priority.infinitePrioritiesCoflow();
//        return Priority.HybridPriority(1);
    }
}
