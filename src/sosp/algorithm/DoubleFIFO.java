package sosp.algorithm;

import java.util.ArrayList;
import java.util.Arrays;

import sosp.jobs.Job;
import sosp.jobs.Task;
import sosp.main.Priority;
import sosp.main.Scheduler;
import sosp.main.TestSunderer;
import sosp.main.Settings;
import sosp.network.Flow;

public class DoubleFIFO implements  Algorithm {

    double weight = 1;
    boolean hybrid = false;

    public DoubleFIFO(String mode){
        if(mode.equalsIgnoreCase("coflow"))
            weight=1;
        else if (mode.equalsIgnoreCase("macroflow"))
            weight=0;
        else if (mode.equalsIgnoreCase("hybrid"))
            hybrid = true;
        else
            assert(false);
    }

    @Override
    public void releaseHost(HostAndTask ht) {
        // do nothing
    }

    @Override
    public ArrayList<Flow>[] getPriority() {
        if(Settings.nPriorities>0)
            return Priority.HybridPriority(hybrid?(TestSunderer.countFreestHost_const()>0?1:0):weight);
        else
            return hybrid? (TestSunderer.countFreestHost_const()>0?Priority.infinitePrioritiesCoflow():Priority.infinitePrioritiesMf())
                    : (weight>0?Priority.infinitePrioritiesCoflow():Priority.infinitePrioritiesMf());
    }

    /*  TODO 以上是从Algorithm.Default.java中复制的变量和函数, 主要是Reduce阶段使用
        直接使用可能有偏差, 后续看看是否需要修改
        以下部分allocateHostAndTask函数是wcx实现
     */


    @Override
    public HostAndTask allocateHostAndTask() {
        assert(Settings.isSeparate); // 只有存算分离场景下可以调用这个调度算法
        // check FreeBw
        // 4.11: check HostBw的同时别忘了Check SwitchBw!
        TestSunderer.totalFreeBw = 0;
        for(int i = 0; i < TestSunderer.freeBw.length; ++i){
            if(TestSunderer.freeBw[i] < Settings.epsilon){ // 单个链路带宽这里使用的误差界还是小一点好, 因为其并没有反复加减, 累计误差不会太高, 如果调高这个界会误把存在的带宽置为0, 导致Assert error
                TestSunderer.freeBw[i] = 0;
            }
            TestSunderer.totalFreeBw += TestSunderer.freeBw[i];
        }
        if(TestSunderer.totalFreeBw < 1e-6 || TestSunderer.switchFreeBw < 1e-6){
            if(TestSunderer.totalFreeBw < 1e-6){
                TestSunderer.totalFreeBw = 0;
            }
            if(TestSunderer.switchFreeBw < 1e-6){
                TestSunderer.switchFreeBw = 0;
            }
            return null;
        }

        // check host
        int num = 0;
        for(int i = 0; i < Settings.nHosts; ++i){
            num += TestSunderer.freeSlots[i];
        }
        if(num == 0){
            return null;
        }

        // First choose a Job  在存算分离场景下, fairSelector中的nActiveTask概念已被修改, hasPendingTasks_const和hasPendingMappers_const似乎没必要修改
        // 因为如果修改了的话其实就视为把Input阶段和Map阶段严格区分了, 实际上不应该严格区分
        // 卡在Input阶段的Mapper自然在pendingMapperList里面?
        // TODO 存算分离场景下 smallestJobSelector是否需要修改还未查看
        Job chosenJob = Settings.fairJobScheduler ? Algorithm.fairJobSelector() : Algorithm.smallestJobSelector();
        if(chosenJob == null) {
            return null;
        }

        // 在上述语境下, 选出来的Job可能处于一种情况: 所有MapTask都已经执行过或者正在执行Input过程, 但仍有MapTask还处于Input阶段而导致仍有pendingMapper
        // 此时此Job被选择, 但已经不能提供还未Input的MapTask了, 若此时Job仍处于MapStage, 则应该直接返回null跳出
        // 这个check保护后续部署Input阶段时找不到相应MapTask的危险
        if(chosenJob.mapStageFinishTime < 0 && chosenJob.notInputMapperList.size() == 0) {
            return null;
        }



        // Second choose a task and allocate
        Task chosenTask = null;

        // 选择MapTask时的准则如下
        // choose the ComputeHost with freeSlot and most freeBw
        // 选择ReduceTask时的准则如下
        // choose the ComputeHost with most freeSlot
        int chosenComputeHost = -1;
        for (int i = 0; i < Settings.nHosts; ++i) {
            chosenComputeHost = (chosenComputeHost < 0) ? i : chosenComputeHost;
            if(chosenJob.mapStageFinishTime < 0){
                if (TestSunderer.freeSlots[i] == 0){
                    continue; // 4.11: 虽然MapInput阶段以带宽为优先目标, 但要警惕有带宽无Slot的情况!
                }
                if (TestSunderer.freeBw[chosenComputeHost] < TestSunderer.freeBw[i]) {
                    chosenComputeHost = i;
                }
            }
            else{
                if (TestSunderer.freeBw[i] < 1e-6 || TestSunderer.freeBw[Settings.nHosts + i] < 1e-6){
                    if(TestSunderer.freeBw[i] < 1e-6){
                        TestSunderer.freeBw[i] = 0;
                    }
                    if(TestSunderer.freeBw[Settings.nHosts + i] < 1e-6){
                        TestSunderer.freeBw[Settings.nHosts + i] = 0;
                    }
                    continue; // 4.11: 虽然Reduce阶段以freeSlot为优先目标, 但要警惕有Slot无带宽的情况
                    // 上下行带宽同时check
                }
                if (TestSunderer.freeSlots[chosenComputeHost] < TestSunderer.freeSlots[i]){
                    chosenComputeHost = i;
                }
            }
        }

        if(chosenJob.mapStageFinishTime < 0) { // 分配MapTask
            assert(chosenJob.notInputMapperList.size() > 0);
            assert (chosenComputeHost >= 0); // 前面已经检查过是否有freeSlot以及freeBw, 如果逻辑正确这里不应该找不到计算节点 .. 4.11: 这里确实会找到计算结点, 但是在极端情况下这个计算结点可能无效!
            // 极端情况是指, 有freeSlot的host没freeBw, 有freeBw的host无freeSlot, 这种host能躲避前面的check, 最终从在找不到合适host的循环中返回一个初始化的值, 即host=0, 然后在这里导入错误, 所以这里还要检查
            if(TestSunderer.freeBw[chosenComputeHost] < 1e-3 || TestSunderer.switchFreeBw < 1e-3){
                // TestSunderer.freeBw[chosenComputeHost] = 0;
                return null; // 二次防卫!
            }
            else if(TestSunderer.freeSlots[chosenComputeHost] == 0){
                return null;
            }
            chosenTask = chosenJob.notInputMapperList.get(0);
        }
        else{ // 分配ReduceTask
            assert (chosenJob.hasPendingTasks_const());
            assert (chosenComputeHost >= 0);
            if(TestSunderer.freeSlots[chosenComputeHost] == 0){
                return null; // 二次防卫!
            }
            else if (TestSunderer.freeBw[chosenComputeHost] < 1e-3 || TestSunderer.switchFreeBw < 1e-3){
                return null;
            }
            chosenTask = chosenJob.pendingReducerList.get(0);
        }
        return new HostAndTask(chosenComputeHost, chosenTask);


//         TODO TestSunderer.totalFreeBw = Arrays.stream(TestSunderer.freeBw).sum(); 这里更新带宽没用, 因为带宽分配是在这个函数外面
//         在Simulate函数中完成, 故应该在那儿更新

    }


}
