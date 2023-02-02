package com.example.consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;

public class EventTableRecord {
    private static final String UNKNOWN_EVENT = "UNKNOWN_EVENT";
    long eventtime;
    String sid;
    String pcid;
    String uid;
    String event;

    public EventTableRecord(long eventTime, String sid, String pcid, String uid, String event) {
        this.eventtime = eventTime;
        this.sid = sid;
        this.pcid = pcid;
        this.uid = uid;
        this.event = event;
    }

    public static EventTableRecord of(long eventTime, String sid, String pcid, String uid, String event) {
        if (Strings.isBlank(event)) {
            return new EventTableRecord(eventTime, sid, pcid, uid, UNKNOWN_EVENT);
        }
        return new EventTableRecord(eventTime, sid, pcid, uid, event);
    }

    public long getEventtime() {
        return eventtime;
    }

    public void setEventtime(long eventtime) {
        this.eventtime = eventtime;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public String getPcid() {
        return pcid;
    }

    public void setPcid(String pcid) {
        this.pcid = pcid;
    }

    public String getUid() {
        if (Strings.isBlank(uid)) {
            return StringUtils.EMPTY;
        }
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getEvent() {
        if (Strings.isBlank(event)) {
            return UNKNOWN_EVENT;
        }
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }


    @Override
    public String toString() {
        return "EventTableRecord{" + "eventtime=" + eventtime + ", sid='" + sid + '\'' + ", pcid='" + pcid + '\'' + ", uid='" + uid + '\'' + ", event='" + event + '\'' + '}';
    }
}
