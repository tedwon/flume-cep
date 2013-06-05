package com.realtimecep.events;

/**
 * <p/>
 *
 * @author <a href=mailto:realtimecep@gmail.com">Ted Won</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public class LogEvent {

    private String hostname;

    private long timestamp;

    private String log;

    public LogEvent() {
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getLog() {
        return log;
    }

    public void setLog(String log) {
        this.log = log;
    }

    @Override
    public String toString() {
        return "LogEvent{" +
                "hostname='" + hostname + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", log='" + log + '\'' +
                '}';
    }
}
