package com.wn.models;

import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class RecentChange {
    private long id;
    private String type;
    private String title;
    private int namespace;
    private String comment;
    private String parsedComment;
    private int timestamp;
    private String user;
    private boolean bot;
    private String serverUrl;
    private String serverName;
    private String serverScriptPath;
    private String wiki;

    private boolean minor;
    private boolean patrolled;
    private Length length;
    private Revision revision;

    private Integer logId;
    private String logType;
    private String logAction;
    private Object logParams;
    private String logActionComment;

    private Meta meta;
}
