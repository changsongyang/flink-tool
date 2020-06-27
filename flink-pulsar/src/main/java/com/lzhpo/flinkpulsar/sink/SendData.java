package com.lzhpo.flinkpulsar.sink;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * 发送的消息数据
 *
 * @author lzhpo
 */
@Data
public class SendData implements Serializable {

    /** ID */
    private Long id;
    /** 消息内容 */
    private String body;
    /** 创建时间 */
    private Date createDate;
    /** 创建者 */
    private String creator;

    /**
     * builder
     *
     * @return Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private final SendData sendData;

        public Builder() {
            sendData = new SendData();
        }

        public Builder id(Long id) {
            this.sendData.id = id;
            return this;
        }

        public Builder body(String body) {
            this.sendData.body = body;
            return this;
        }

        public Builder createDate(Date createDate) {
            this.sendData.createDate = createDate;
            return this;
        }

        public Builder creator(String creator) {
            this.sendData.creator = creator;
            return this;
        }
        public SendData builded() {
            return sendData;
        }
    }

}
