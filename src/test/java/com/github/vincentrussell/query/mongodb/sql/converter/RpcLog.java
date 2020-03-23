package com.github.vincentrussell.query.mongodb.sql.converter;

import qeorm.MongodbModelBase;
import qeorm.annotation.Column;
import qeorm.annotation.Table;
import qeorm.annotation.Transient;

import java.util.Date;
import java.util.Map;


@Table(masterDbName = "mongo", slaveDbName = "mongo", primaryKey = "data_id", tableName = "rpc_logs", where = "data_id={dataId} and status={status}  and trace_id like {traceId}% and type={type} and create_at>{beginTime} and create_at<{endTime} and request_data={reqValue} and response_data={resValue} and url like {url}% and  error_msg like %{errorMsg}%")
public class RpcLog extends MongodbModelBase {

    @Column("data_id")
    private String dataId;

    //请求追踪id
    @Column("trace_id")
    private String traceId;

    //子请求追踪id
    @Column("child_trace_id")
    private String childTraceId;

    //请求方式
    @Column("request_method")
    private String requestMethod;

    //请求来源
    @Column("request_source")
    private String requestSource;

    //服务端ip
    @Column("server_ip")
    private String serverIp;
    //客户端ip
    @Column("client_ip")
    private String clientIp;

    //接口类型
    private String type;

    //响应状态：1:错误,2:成功
    private String status;
    //重试次数
    private Integer retry = 0;

    //请求地址
    private String url;

    //请求信息
    @Column("request_data")
    private Map<String, Object> requestData;

    //响应信息
    @Column("response_data")
    private Map<String, Object> responseData;

    //接口处理时间：毫秒
    @Column("handle_len")
    private Long handleLen;

    //错误信息
    @Column("error_msg")
    private String errorMsg;
    //计费（Y： 计费，N：不计费）
    @Column("charging")
    private String charging;

    //请求开始时间
    @Column("begin_at")
    private Date sendRequestAt;

    //请求结束时间
    @Column("end_at")
    private Date receiveResponseAt;

    //创建时间
    @Column("create_at")
    private Date createAt;


    //用于查询
    @Transient
    private Date beginTime;
    //用于查询
    @Transient
    private Date endTime;

    //精确查询
    //用于查询请求数据
    @Transient
    private Map<String, Object> reqValue;

    //精确查询
    //用于查询响应数据
    @Transient
    private Map<String, Object> resValue;

    public String getDataId() {
        return dataId;
    }

    public void setDataId(String dataId) {
        this.dataId = dataId;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public String getChildTraceId() {
        return childTraceId;
    }

    public void setChildTraceId(String childTraceId) {
        this.childTraceId = childTraceId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getServerIp() {
        return serverIp;
    }

    public void setServerIp(String serverIp) {
        this.serverIp = serverIp;
    }

    public String getClientIp() {
        return clientIp;
    }

    public void setClientIp(String clientIp) {
        this.clientIp = clientIp;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Integer getRetry() {
        return retry;
    }

    public void setRetry(Integer retry) {
        this.retry = retry;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Map<String, Object> getRequestData() {
        return requestData;
    }

    public void setRequestData(Map<String, Object> requestData) {
        this.requestData = requestData;
    }

    public Map<String, Object> getResponseData() {
        return responseData;
    }

    public void setResponseData(Map<String, Object> responseData) {
        this.responseData = responseData;
    }

    public Long getHandleLen() {
        return handleLen;
    }

    public void setHandleLen(Long handleLen) {
        this.handleLen = handleLen;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public Date getCreateAt() {
        return createAt;
    }

    public void setCreateAt(Date createAt) {
        this.createAt = createAt;
    }

    public Date getBeginTime() {
        return beginTime;
    }

    public void setBeginTime(Date beginTime) {
        this.beginTime = beginTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public Map<String, Object> getReqValue() {
        return reqValue;
    }

    public void setReqValue(Map<String, Object> reqValue) {
        this.reqValue = reqValue;
    }

    public Date getSendRequestAt() {
        return sendRequestAt;
    }

    public void setSendRequestAt(Date sendRequestAt) {
        this.sendRequestAt = sendRequestAt;
    }

    public String getRequestMethod() {
        return requestMethod;
    }

    public void setRequestMethod(String requestMethod) {
        this.requestMethod = requestMethod;
    }

    public Date getReceiveResponseAt() {
        return receiveResponseAt;
    }

    public void setReceiveResponseAt(Date receiveResponseAt) {
        this.receiveResponseAt = receiveResponseAt;
    }

    public String getRequestSource() {
        return requestSource;
    }

    public void setRequestSource(String requestSource) {
        this.requestSource = requestSource;
    }

    public Map<String, Object> getResValue() {
        return resValue;
    }

    public void setResValue(Map<String, Object> resValue) {
        this.resValue = resValue;
    }

    public String getCharging() {
        return charging;
    }

    public void setCharging(String charging) {
        this.charging = charging;
    }
}
