package com.tianchi.django.common.pojo.associate;

import java.util.List;

import com.tianchi.django.common.pojo.App;
import com.tianchi.django.common.pojo.Pod;
import com.tianchi.django.common.utils.HCollectors;
import lombok.*;
import org.apache.commons.collections4.ListUtils;

import static java.util.stream.Collectors.toList;

/**
 * Created by 烛坤 on 2020/5/20.
 *
 * @author 烛坤
 * @date 2020/05/20
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GroupRuleAssociate {

    private String group;//应用分组

    private int replicas;

    public static List<GroupRuleAssociate> fromApps(List<App> apps) {
        return ListUtils.emptyIfNull(apps).parallelStream()
            .map(app -> GroupRuleAssociate.builder().group(app.getGroup()).replicas(app.getReplicas()).build()).collect(toList());
    }

    public static List<GroupRuleAssociate> fromPods(List<Pod> pods) {
        return ListUtils.emptyIfNull(pods).stream().collect(HCollectors.countingInteger(Pod::getGroup)).entrySet().parallelStream()
            .map(entry -> GroupRuleAssociate.builder().group(entry.getKey()).replicas(entry.getValue()).build()).collect(toList());
    }

}
