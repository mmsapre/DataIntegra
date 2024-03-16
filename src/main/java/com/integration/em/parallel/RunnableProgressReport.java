package com.integration.em.parallel;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.time.DurationFormatUtils;

import java.util.Map.Entry;
import java.util.concurrent.ThreadPoolExecutor;

@Slf4j
public class RunnableProgressReport implements Runnable{

    private ThreadPoolExecutor pool;
    private Thread thread;
    private Task userTask;
    private boolean stop;
    private String message;
    private boolean reportIfStuck = true;

    long start = 0;
    long tasks = 0;
    long done = 0;
    int stuckIterations = 0;;
    long last = 0;
    long lastTime = 0;
    public Task getUserTask() {
        return userTask;
    }

    public void setUserTask(Task userTask) {
        this.userTask = userTask;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public ThreadPoolExecutor getPool() {
        return pool;
    }

    public void setPool(ThreadPoolExecutor pool) {
        this.pool = pool;
    }

    public void setReportIfStuck(boolean reportIfStuck) {
        this.reportIfStuck = reportIfStuck;
    }

    public boolean getReportIfStuck() {
        return reportIfStuck;
    }

    public void run() {
        try {
            initialise();
            while (!stop) {
                Thread.sleep(10000);

                if (!stop) {
                    print();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void initialise() {
        start = System.currentTimeMillis();
        tasks = pool.getTaskCount();
        done = pool.getCompletedTaskCount();
        stuckIterations = 0;
        ;
        last = 0;
        lastTime = System.currentTimeMillis();
    }

    public void print() {

        if (System.currentTimeMillis() - lastTime >= 10000) {

            tasks = pool.getTaskCount();
            done = pool.getCompletedTaskCount();

            long soFar = System.currentTimeMillis() - start;
            long pauseTime = System.currentTimeMillis() - lastTime;
            long left = (long) (((float) soFar / done) * (tasks - done));
            float itemsPerSecAvg = (float) done / (float) (soFar / 1000.0f);
            float itemsPerSecNow = (float) (done - last) / (pauseTime / 1000.0f);

            if ((((float) soFar) / done) == Float.POSITIVE_INFINITY) {
                left = -1;
            }
            String ttl = DurationFormatUtils.formatDuration(soFar, "HH:mm:ss.S");
            String remaining = DurationFormatUtils.formatDuration(left, "HH:mm:ss.S");

            String usrMsg = message == null ? "" : message + ": ";
            log.info(String.format(
                    "%s%,d of %,d tasks completed after %s (%d/%d active threads). Avg: %.2f items/s, Current: %.2f items/s, %s left.",
                    usrMsg, done, tasks, ttl, pool.getActiveCount(), pool.getPoolSize(), itemsPerSecAvg, itemsPerSecNow,
                    remaining));

            if (userTask != null)
                userTask.execute();

            if (done == last) {
                stuckIterations++;
            } else {
                last = done;
                stuckIterations = 0;
            }

            if (stuckIterations >= 3 && reportIfStuck) {
                log.trace("ThreadPool seems to be stuck!");
                int threadCnt = 0;
                int parkedCnt = 0;
                Entry<Thread, StackTraceElement[]> main = null;
                for (Entry<Thread, StackTraceElement[]> e : Thread.getAllStackTraces().entrySet()) {
                    if (e.getKey().getName().contains("Parallel")) {
                        threadCnt++;

                        if (e.getValue()[0].toString().startsWith("sun.misc.Unsafe.park")) {
                            parkedCnt++;
                        } else {
                            log.trace(e.getKey().getName());
                            for (StackTraceElement elem : e.getValue()) {
                                log.trace("\t" + elem.toString());
                            }
                        }

                    }

                    if (e.getKey().getName().equals("main")) {
                        main = e;
                    }
                }

                log.trace(String.format("%s %d Parallel.X threads (%d parked) --- %d total",
                        pool.isTerminated() ? "[pool terminated]" : "", threadCnt, parkedCnt,
                        Thread.getAllStackTraces().size()));

                if (main != null) {
                    log.trace(main.getKey().getName());
                    for (StackTraceElement elem : main.getValue()) {
                        log.error("\t" + elem.toString());
                    }
                }
            }

            lastTime = System.currentTimeMillis();

        }
    }

    public void start() {
        stop = false;
        thread = new Thread(this);
        thread.start();
    }

    public void stop() {
        stop = true;
    }


}
