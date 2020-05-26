package com.tianchi.django.calculate;

import java.util.*;
import java.util.stream.IntStream;

import com.google.common.collect.Lists;
import com.tianchi.django.common.utils.ScheduleUtils;
import com.tianchi.django.common.ISchedule;
import com.tianchi.django.common.pojo.*;
import com.tianchi.django.common.pojo.associate.GroupRuleAssociate;
import com.tianchi.django.common.utils.*;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import static java.util.stream.Collectors.toList;

@Slf4j
@AllArgsConstructor
public class CalculateSchedule implements ISchedule {

    private long start;//对象构建时传入的程序启动时间戳

    private static final ThreadLocal<Map<String, Integer>> ALL_MAX_INSTANCE_PER_NODE_LIMIT = new ThreadLocal<>();

    @Override
    public List<ScheduleResult> schedule(@NonNull final List<Node> nodes, @NonNull final List<App> apps, @NonNull final Rule rule) {

        List<NodeWithPod> nodeWithPods = sortAndInitNodeWithPods(nodes, rule);

        List<Pod> pods = sortAndInitPods(apps);

        ALL_MAX_INSTANCE_PER_NODE_LIMIT.set(RuleUtils.toAllMaxInstancePerNodeLimit(rule, GroupRuleAssociate.fromApps(apps)));

        calculate(nodeWithPods, pods, rule);

        return nodeWithPods.parallelStream()
            .map(
                nwp -> nwp.getPods().stream()
                    .map(
                        pod -> ScheduleResult.builder().sn(nwp.getNode().getSn()).group(pod.getGroup())
                            .cpuIDs(pod.getCpuIDs()).build()
                    )
                    .collect(toList())
            )
            .flatMap(Collection::stream).collect(toList());

    }

    /**
     * 通过node按照规则分数从大到小排序, 然后nodes信息转化为能够容乃下pod对象的NodeWithPod对象，与node对象一对一构建。
     *
     * @param nodes 宿主机列表
     * @param rule  规则对象
     * @return 能够容乃下pod对象的NodeWithPod对象
     */
    private List<NodeWithPod> sortAndInitNodeWithPods(List<Node> nodes, Rule rule) {
        return nodes.parallelStream()
            .sorted(
                Collections.reverseOrder(Comparator.comparingInt(node -> ScoreUtils.resourceScore(node, rule)))
            )
            .map(
                node -> NodeWithPod.builder().node(node).pods(Lists.newArrayList()).build()
            )
            .collect(toList());
    }

    /**
     * 通过apps列表对象转化为实际要扩容的pod列表
     *
     * @param apps 应用列表
     * @return 通过app信息构建的pods列表
     */
    private List<Pod> sortAndInitPods(List<App> apps) {

        Comparator<App> replicas = Collections.reverseOrder(Comparator.comparingInt(App::getReplicas));//app对应的pod数量从多到少排序

        Comparator<App> source = Collections.reverseOrder(Comparator.comparingInt(App::getGpu)
            .thenComparing(App::getCpu).thenComparing(App::getRam).thenComparing(App::getDisk));//按gpu、cpu、内存、磁盘从大到小排序。

        List<App> sortApps = apps.stream().sorted(replicas.thenComparing(source)).collect(toList());//排序后的app列表

        List<Pod> pods = Lists.newArrayList();

        for (App app : sortApps) {
            pods.addAll(IntStream.range(0, app.getReplicas()).mapToObj($ -> PodUtils.toPod(app)).collect(toList()));//按照app排序构建pod数据
        }

        log.info("schedule app transform pod count : {}", pods.size());

        return pods;
    }

    private void calculate(List<NodeWithPod> nodeWithPods, List<Pod> pods, Rule rule) {

        List<Pod> forsakePods = Lists.newArrayList();//装不下的容器

        for (Pod pod : pods) {//以打散为目的对排序后的pod、node贪心循环

            boolean forsake = true;//遗弃标识

            for (NodeWithPod nwp : nodeWithPods) {

                if (ScheduleUtils.ruleOverrunTimeLimit(rule, start)) {//时间上限约束，超时跳出。
                    log.info("overrun time limit");
                    return;
                }

                Map<String, Integer> allMaxInstancePerNodeLimit = ALL_MAX_INSTANCE_PER_NODE_LIMIT.get();

                if (ScheduleUtils.staticFillOnePod(nwp, pod, allMaxInstancePerNodeLimit)) {
                    forsake = false;
                    break;
                }
            }

            if (forsake) {//所有的机器都不满足此容器分配
                forsakePods.add(pod);
            }
        }

        if (forsakePods.size() > 0) {
            log.error("forsake pod count: {}", forsakePods.size());
        }

    }

}
