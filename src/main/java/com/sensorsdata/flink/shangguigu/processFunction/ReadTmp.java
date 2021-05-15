package com.sensorsdata.flink.shangguigu.processFunction;

import lombok.*;

/**
 * @Author: Li Guangwei
 * @Descriptions: TODO
 * @Date: 2021/5/2 22:09
 * @Version: 1.0
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class ReadTmp {
    private String name;
    private Long timeStamp;
    private Double tmp;
}
