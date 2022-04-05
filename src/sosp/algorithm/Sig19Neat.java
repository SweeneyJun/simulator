package sosp.algorithm;

import sosp.jobs.*;
import sosp.main.Priority;
import sosp.main.Scheduler;
import sosp.main.Settings;
import sosp.main.Topology;
import sosp.network.Coflow;
import sosp.network.Flow;
import sosp.network.Macroflow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class Sig19Neat implements Algorithm {

    private ArrayList<Coflow>[] link_cf = null; // coflow list on each link
    //private double[] s = null; // current emitted size of a coflow
    private double[][] link_cf_size = null; // current emitted size of the i-th coflow, on the j-th link

    private double weight = 1;

    @SuppressWarnings("unchecked")
    @Override
    public HostAndTask allocateHostAndTask() {
        double[] bw = Topology.getLinkBw();
        if(link_cf==null){
            link_cf = new ArrayList[bw.length];
            for(int i=0;i<link_cf.length;++i)
                link_cf[i] = new ArrayList<Coflow>();
            //s = new double[Scheduler.jobs.length];
            link_cf_size = new double[Scheduler.jobs.length][bw.length];
        }

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
                int host = -1;
                for (int i = 0; i < Scheduler.freeSlots.length; ++i) {
                    if (pendingMapper) {
                        if (Scheduler.freeSlots[i] == 0 || Scheduler.hostInfos[i].shuffleReducerNum >= 3)
                            continue;
                    } else {
                        if (Scheduler.freeSlots[i] == 0)
                            continue;
                    }
                    host = (host < 0) ? i : host;
                    if (Scheduler.freeSlots[host] < Scheduler.freeSlots[i])
                        host = i;
                }
                if (host < 0)
                    continue;

                if (chosenJob.mapStageFinishTime > 0 && chosenJob.hasPendingTasks_const()) {
//                for(ReduceTask task : chosenJob.pendingReducerList){
//                    if(chosenTask == null)
//                        chosenTask = task;
//                    else
//                        chosenTask = (task.macroflow.size > ((ReduceTask)chosenTask).macroflow.size) ? task : chosenTask;
//                }
                    chosenTask = chosenJob.pendingReducerList.get(0);
                }
                if (jobQueue.mode.equals("FIFO") && !jobQueue.canTransfer(chosenJob)) {
//                    System.out.println("dskajfkljsdaklfjlsajfljsaklfjlsajflk");
                    chosenTask = null;
                }
                if (chosenTask == null) {
                    continue;
                }
                Macroflow mf = ((ReduceTask) chosenTask).macroflow;

                // 2. find a host for reducer
                double bestDelta = Double.POSITIVE_INFINITY; // best task completion time forecast
                int bestSlot = -1; // best host to place the task
                double[] bestLinkSize = null; // the best size on each link after choosing the best host

                for (int i = 0; i < Scheduler.freeSlots.length; ++i) {
                    if (Scheduler.freeSlots[i] == 0)
                        continue;
                    double[] deltaLinkSize = new double[bw.length]; // flow size delta in each link
                    double[] linkSize = new double[bw.length]; // macroflow size distribution in each link
                    double delta = 0; // time delta
                    boolean[] relatedLink = new boolean[bw.length]; // macroflow related link
                    for (Flow flow : mf.flows) {
                        int[] linkList = Topology.getPath(flow.sender, i);
                        for (int k = 0; k < linkList.length; ++k) {
                            int link = linkList[k];
                            linkSize[link] += flow.size;
                            relatedLink[link] = true;
                        }
                    }
                    for (int link = 0; link < deltaLinkSize.length; ++link) {
                        if (!relatedLink[link])
                            continue;
                        int selectedJob = chosenTask._job.jobId;
                        double selectedCoflowSize = chosenTask._job.coflow.size;
                        // selected job link size change during the selected job running
                        deltaLinkSize[link] = link_cf_size[selectedJob][link] + linkSize[link];
                        for (Coflow coflow : link_cf[link]) {
                            int job = coflow._job.jobId;
                            double coflowSize = coflow.size;
                            if (job == selectedJob)
                                continue;
                            // related job link size change during the selected job running
                            if (coflowSize <= selectedCoflowSize)
                                deltaLinkSize[link] += link_cf_size[job][link] + (link_cf_size[selectedJob][link] + linkSize[link]) * coflowSize / selectedCoflowSize * weight;
                            else
                                deltaLinkSize[link] += link_cf_size[job][link] * (selectedCoflowSize) / coflowSize * weight + (link_cf_size[selectedJob][link] + linkSize[link]);
                        }
                        deltaLinkSize[link] /= bw[link];
                        delta = Math.max(delta, deltaLinkSize[link]);
                    }
                    //System.out.print(delta+ (i==Scheduler.freeSlots.length-1?"\n":" "));
                    if (delta < bestDelta) {
                        bestDelta = delta;
                        bestSlot = i;
                        bestLinkSize = linkSize;
                    }
                }
                if (Double.isInfinite(bestDelta)) {
                    continue;
                }

                // 3. update information
                //s[chosenTask._job.jobId] += mf.size;
                for (Flow flow : mf.flows) {
                    int[] linkList = Topology.getPath(flow.sender, bestSlot);
                    for (int i = 0; i < linkList.length; ++i) {
                        int link = linkList[i];
                        if (!link_cf[link].contains(mf._coflow))
                            link_cf[link].add(mf._coflow);
                        link_cf_size[chosenTask._job.jobId][link] += bestLinkSize[link];
                    }
                }
                return new HostAndTask(bestSlot, chosenTask);
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
        if(ht.task instanceof ReduceTask){
            ReduceTask reducer = (ReduceTask) ht.task;
            Coflow c = reducer._job.coflow;
            for(ArrayList<Coflow> link:link_cf)
                link.remove(c);
        }
    }

    @Override
    public ArrayList<Flow>[] getPriority() {
        if(Settings.nPriorities>0)
            return Priority.HybridPriority(weight);
        else
            return Priority.infinitePrioritiesCoflow();
    }
}
