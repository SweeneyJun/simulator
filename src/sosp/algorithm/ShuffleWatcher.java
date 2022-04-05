package sosp.algorithm;

import sosp.jobs.*;
import sosp.main.HostInfo;
import sosp.main.Scheduler;
import sosp.main.Settings;
import sosp.network.Flow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

public class ShuffleWatcher implements Algorithm {

    @Override
    public HostAndTask allocateHostAndTask() {
        // task
//        Job chosenJob = Settings.fairJobScheduler ? Algorithm.fairJobSelector() : Algorithm.smallestJobSelector();
//        Job[] chosenJobs = Settings.fairJobScheduler ? Algorithm.jobSorterFair():Algorithm.jobSorterFifo();
//        Job[] chosenJobs = Algorithm.jobSorterSJF();

        ArrayList<JobQueue> jobQueues = new ArrayList<>();
        jobQueues = Scheduler.jobQueues;

        Collections.sort(jobQueues, new Comparator<JobQueue>() {
            @Override
            public int compare(JobQueue o1, JobQueue o2) {
                return Double.compare(o1.resourceRatio(), o2.resourceRatio());
            }
        });

        for (JobQueue jobQueue: Scheduler.jobQueues) {
            if (1.0*jobQueue.nActiveTasks()/Settings.nSlots/Settings.nHosts > jobQueue.maxResource) {
                continue;
            }
            Job[] chosenJobs = jobQueue.jobSorter();

            Task chosenTask = null;
            boolean pendingMapper = false;
            for (Job chosenJob : chosenJobs) {
                if (chosenJob.hasPendingMappers_const()) {
                    pendingMapper = true;
                }
            }

            for (Job chosenJob : chosenJobs) {
                if (chosenJob.hasPendingMappers_const()) {
                    for (MapTask mt : chosenJob.pendingMapperList) {
                        for (int i = 0; i < mt.hdfsHost.length; ++i) {
                            if (Scheduler.freeSlots[mt.hdfsHost[i]] > 0) {
                                return new HostAndTask(mt.hdfsHost[i], mt);
                            }
                        }
                    }
                }

                if (chosenJob.mapStageFinishTime > 0 && chosenJob.hasPendingTasks_const()) {
                    chosenTask = chosenJob.pendingReducerList.get(0);

                    HostInfo[] tempHostInfos = new HostInfo[Settings.nHosts];
                    for (int i = 0; i < Settings.nHosts; i++) {
                        tempHostInfos[i] = Scheduler.hostInfos[i];
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
                                if (tempHostInfos[0].shuffleReducerNum >= Settings.reduceNum || Scheduler.freeSlots[tempHostInfos[0].hostId] == 0) {
                                    continue;
                                }
                                return new HostAndTask(tempHostInfos[0].hostId, chosenTask);
                            } else {
                                for (int i = 0; i < Settings.nHosts; i++) {
                                    if (chosenJob.coflow.size < tempHostInfos[i].minCoflowSize && Scheduler.freeSlots[tempHostInfos[i].hostId] != 0) {
                                        return new HostAndTask(tempHostInfos[i].hostId, chosenTask);
                                    }
                                }
                            }
                        }
                    }
                    else{
                        if (chosenJob.coflow.size >= HostInfo.hostMaxMinCoflowSize) {
                            if (tempHostInfos[0].shuffleReducerNum >= Settings.reduceNum || Scheduler.freeSlots[tempHostInfos[0].hostId] == 0) {
                                continue;
                            }
                            return new HostAndTask(tempHostInfos[0].hostId, chosenTask);
                        } else {
                            for (int i = 0; i < Settings.nHosts; i++) {
                                if (chosenJob.coflow.size < tempHostInfos[i].minCoflowSize && Scheduler.freeSlots[tempHostInfos[i].hostId] != 0) {
                                    return new HostAndTask(tempHostInfos[i].hostId, chosenTask);
                                }
                            }
                        }
                    }
                }
            }
            // host
            int host = -1;
            for (int i = 0; i < Scheduler.freeSlots.length; ++i) {
                if (Scheduler.freeSlots[i] == 0)
                    continue;
                host = (host < 0) ? i : host;
                if (Scheduler.freeSlots[host] < Scheduler.freeSlots[i])
                    host = i;
            }
            if (host < 0)
                return null;
            for (Job chosenJob : chosenJobs) {
                if (chosenJob.hasPendingMappers_const()) {
                    chosenTask = chosenJob.pendingMapperList.get(0);
                    return new HostAndTask(host, chosenTask);
                }
            }
        }
        return null;
    }

    @Override
    public void releaseHost(HostAndTask ht) {
        //do nothing
    }

    @Override
    public ArrayList<Flow>[] getPriority() {
        @SuppressWarnings("unchecked")
        ArrayList<Flow>[] activeFlows = new ArrayList[1];
        for(int i=0;i<activeFlows.length;++i)
            activeFlows[i] = new ArrayList<Flow>();
        for (ReduceTask rt: Scheduler.activeReducers) {
            for (Flow flow: rt.macroflow.flows) {
                if(flow.finishTime>=0)
                    continue;
                activeFlows[0].add(flow);
            }
        }
        return activeFlows;

//        // FIFO priority
//        int nPriority = Settings.nPriorities;
//        @SuppressWarnings("unchecked")
//        ArrayList<Flow>[] activeFlows = new ArrayList[nPriority];
//        for(int i=0;i<activeFlows.length;++i)
//            activeFlows[i] = new ArrayList<Flow>();
//
//        ArrayList<Integer> jobid = new ArrayList<>();
//        for (Job job :Scheduler.activeJobs) {
//            jobid.add(job.jobId);
//        }
//
//        Collections.sort(jobid);
//
//        for (ReduceTask rt: Scheduler.activeReducers) {
//            int a = jobid.indexOf(rt._job.jobId);
//            for (Flow flow: rt.macroflow.flows){
//                if(flow.finishTime>=0)
//                    continue;
//                if (nPriority - a - 1 >= 0){
//                    activeFlows[nPriority - a - 1].add(flow);
//                }
//                else {
//                    activeFlows[0].add(flow);
//                }
//            }
//        }
//
//        return activeFlows;
    }
}
