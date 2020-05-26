package com.tianchi.django.pojo;

import lombok.*;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by 烛坤 on 2020/5/20.
 *
 * @author 烛坤
 * @date 2020/05/20
 */
@Slf4j
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RescheduleStatistic {

    private int migrateCount;

    private ScheduleStatistic resultStatistic;

    public static RescheduleStatistic from() {
        return null;
    }

}
