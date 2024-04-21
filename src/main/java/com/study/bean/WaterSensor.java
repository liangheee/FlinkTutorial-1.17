package com.study.bean;

/**
 * @author Hliang
 * @create 2023-08-12 11:05
 */
public class WaterSensor {
    public String id;
    public Long ts;
    public Integer vc;

    // 一定要提供一个 空参的构造器
    public WaterSensor() {
    }

    public WaterSensor(String id, Long ts, Integer vc) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Integer getVc() {
        return vc;
    }

    public void setVc(Integer vc) {
        this.vc = vc;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WaterSensor that = (WaterSensor) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (ts != null ? !ts.equals(that.ts) : that.ts != null) return false;
        return vc != null ? vc.equals(that.vc) : that.vc == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (ts != null ? ts.hashCode() : 0);
        result = 31 * result + (vc != null ? vc.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "WaterSensor{" +
                "id='" + id + '\'' +
                ", ts=" + ts +
                ", vc=" + vc +
                '}';
    }
}
