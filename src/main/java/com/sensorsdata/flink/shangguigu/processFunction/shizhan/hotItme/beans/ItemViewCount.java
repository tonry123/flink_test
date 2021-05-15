package com.sensorsdata.flink.shangguigu.processFunction.shizhan.hotItme.beans;

/**
 * @Author: Li Guangwei
 * @Descriptions: TODO
 * @Date: 2021/5/10 22:05
 * @Version: 1.0
 */
public class ItemViewCount {
    private Long itemId;
    private Long windEnd;
    private Long count;

    public ItemViewCount() {

    }

    public ItemViewCount(Long itemId, Long windEnd, Long count) {
        this.itemId = itemId;
        this.windEnd = windEnd;
        this.count = count;
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public Long getWindEnd() {
        return windEnd;
    }

    public void setWindEnd(Long windEnd) {
        this.windEnd = windEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "ItemViewCount{" +
                "itemId=" + itemId +
                ", windEnd=" + windEnd +
                ", count=" + count +
                '}';
    }
}
