package sosp.jobs;

import sosp.main.TestPushBox;
import sosp.main.Settings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

public class JobQueue {
    public double resource;
    public double maxResource;
    public String mode;
    public ArrayList<Job> activeJobs = new ArrayList<>();

    public JobQueue(String mode, double resource, double maxResource) {
        this.mode = mode;
        this.resource = resource;
        this.maxResource = maxResource;
    }

    public Job[] jobSorter(){
        Job[] jobs = this.activeJobs.toArray(new Job[0]);
        if(mode.equals("FIFO")) {
            Arrays.sort(jobs, new Comparator<Job>() {
                @Override
                public int compare(Job arg0, Job arg1) {
                    return arg0.jobId - arg1.jobId;
                }
            });
        }
        else {
            Arrays.sort(jobs, new Comparator<Job>() {
                @Override
                public int compare(Job arg0, Job arg1) {
                    return arg0.nActiveTasks_const() - arg1.nActiveTasks_const();
                }
            });
        }
        return jobs;
    }

    public int nActiveTasks() {
        int sum = 0;
        for (Job job: activeJobs) {
            sum += job.nActiveTasks_const();
        }
        return sum;
    }

    public double resourceRatio() {
        return (1.0*nActiveTasks()/(Settings.nHosts*Settings.nSlots)/resource);
    }

    public boolean canTransfer(Job job) {
        if (mode.equals("FIFO")) {
            int index = activeJobs.indexOf(job);
            if (index < 0) {
                return false;
            }
            for (int i = 0; i < index; i++) {
                Job j = activeJobs.get(i);
                assert job.jobId > j.jobId;
                if (!j.isAllShuffleFinished_const()) {
                    return false;
                }
            }
        }
        return true;
    }
}
