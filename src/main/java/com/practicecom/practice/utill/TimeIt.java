package com.practicecom.practice.utill;

import com.google.common.base.Stopwatch;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeIt {
  
    private static ThreadMXBean threadMXBean = null;

    static {
        try {
            threadMXBean = ManagementFactory.getThreadMXBean();
        } catch (Exception e) {
        	// this.getlogger().warn("TimeIt cannot initialize ThreadMXBean with exception %s", e.getMessage());
        }
    }

    private final String methodName;
    private final String correlationId;
    private final String entityId;
    private final String connectorName;
    private long startCpuTime = 0L;
    private long startUserTime = 0L;
    private long startSystemTime = 0L;
    private final Stopwatch stopWatch;
    private final long threadId;
    private String msg;

    public TimeIt( String methodName, String correlationId, String entityId, String connectorName) {
        this.methodName = methodName;
        this.correlationId = correlationId;
        this.entityId = entityId;
        this.connectorName = connectorName;
        this.threadId = Thread.currentThread().getId();
        this.stopWatch = Stopwatch.createUnstarted();
        this.msg = null;
    }

    public void start() {
        try {
            stopWatch.start();
            startCpuTime = getCpuTime();
            startUserTime = getUserTime();
            startSystemTime = getSystemTime(startCpuTime, startUserTime);
        } catch (Exception e) {
            final String msg =
                    String.format("TimeIt [%s] [Thread: %s] [Cannot Start with exception: %s]",
                            threadId, methodName, e.getMessage());
            this.getlogger().error(msg, correlationId, entityId, connectorName);
        }
    }

    public void reset() {
        stopWatch.reset();
        startCpuTime = 0L;
        startUserTime = 0L;
        startSystemTime = 0L;
    }

    public void stop() {
        stop(msg);
    }

    public void stop(final String message) {
        try {
            final String m = message != null ? "[message: " + message + "]" : "";
            stopWatch.stop();
            long endCpuTime = getCpuTime();
            long endUserTime = getUserTime();
            long endSystemTime = getSystemTime(endCpuTime, endUserTime);
            final String msg = String.format("TimeIt [%s] [Thread: %s] [ Took Cpu Time: %s(ns), User Time: %s(ns)," +
                            " System Time: %s(ns), stopwatch time: %s(ns)] %s",
                    methodName, threadId,
                    (endCpuTime - startCpuTime), (endUserTime - startUserTime),
                    (endSystemTime - startSystemTime), stopWatch.elapsed(TimeUnit.NANOSECONDS), m
            );
            this.getlogger().info(msg, correlationId, entityId, connectorName);
        } catch (Exception e) {
            final String msg =
                    String.format("TimeIt [%s] [Thread: %s] [Cannot Stop with exception: %s]",
                            threadId, methodName, e.getMessage());
            this.getlogger().error(msg, correlationId, entityId, connectorName);
        }
    }

    public String getMessage() {
        return msg;
    }

    public void setMessage(final String msg) {
        this.msg = msg;
    }

    /**
     * Get CPU time in nanoseconds.
     */
    private long getCpuTime() {
        long time = 0L;
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        if (threadMXBean != null && threadMXBean.isThreadCpuTimeSupported()) {
            final long t = bean.getThreadCpuTime(threadId);
            if(t != -1) {
                time = t;
            }
        }

        return time;
    }

    /**
     * Get user time in nanoseconds.
     */
    private long getUserTime() {
        long time = 0L;
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        if (threadMXBean != null && threadMXBean.isThreadCpuTimeSupported()) {
            final long t = bean.getThreadUserTime(threadId);
            if(t != -1) {
                time = t;
            }
        }

        return time;
    }

    /**
     * Get system time in nanoseconds.
     */
    private long getSystemTime(final long threadCpuTime, final long threadUserTime) {
        return threadCpuTime != -1 && threadUserTime != -1 ? (threadCpuTime - threadUserTime) : 0L;
    }
    
    /**
	 * Initializing the logger
	 * 
	 * @return
	 */
	private Logger getlogger() {
		return LoggerFactory.getLogger(TimeIt.class);
	}
}
