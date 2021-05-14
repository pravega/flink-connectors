package io.pravega.connectors.flink.dynamic.table;

import java.io.Serializable;

public class UserTest implements Serializable {
    private static final long serialVersionUID = 8241970228716425282L;
    private String name;
    private Integer phone;
    private Boolean vip;

    public void setName(String name) {
        this.name = name;
    }
    public String getName() {
        return name;
    }

    public void setPhone(int phone) {
        this.phone = phone;
    }
    public int getPhone() {
        return phone;
    }

    public void setVip(boolean vip) {
        this.vip = vip;
    }
    public boolean getVip() {
        return vip;
    }
}
