package com.integration.em.parallel;

import com.integration.em.utils.Func;
import com.integration.em.utils.Q;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

public class Parallel<T> {
    private static int MAX_FAILED_TASKS = 1;
    private static int defaultNumProcessors = Runtime.getRuntime().availableProcessors();
    private static int defaultQueueSize = 10000;
    private int overrideNumProcessors = 0;
    private static Parallel<?> currentTask = null;
    private static Map<ITask, Thread> runningTasks = new ConcurrentHashMap<ITask, Thread>();
    private static ThreadPoolExecutor defaultExecutor;

    private static boolean reportIfStuck = true;

    public Parallel()
    {

    }

    public Parallel(int numProcessors)
    {
        if(numProcessors>0) {
            overrideNumProcessors = numProcessors;
        }
    }

    public interface ITask
    {
        void execute() throws Exception;
    }

    public static void SetDefaultNumProcessors(int numProcessors)
    {
        defaultNumProcessors = numProcessors;
    }

    private int getNumProcessors()
    {
        if(overrideNumProcessors>0)
            return overrideNumProcessors;
        else
            return defaultNumProcessors;
    }

    private static int getNumProcessors(Parallel<?> obj)
    {
        // if this is a nested parallel process, only use 1 thread ...
        if(currentTask == null || currentTask == obj)
            return obj.getNumProcessors();
        else
            return 1;
    }

    private static boolean startParallelProcess(Parallel<?> obj)
    {
        // only set obj as current parallel task if it uses more than one thread ...
        if(currentTask == null && obj.getNumProcessors()>1)
        {
            currentTask = obj;
            return true;
        }
        else
            return false;
    }

    private static void endParallelProcess(Parallel<?> obj)
    {
        if(currentTask == obj)
            currentTask = null;
    }
    public static ThreadPoolExecutor getExecutor(int numProcessors) {
        if(numProcessors==defaultNumProcessors)
        {
            if(getDefaultExecutor()==null)
            {
                setDefaultExecutor(new ThreadPoolExecutor(
                        defaultNumProcessors,
                        defaultNumProcessors,
                        0,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<Runnable>(getDefaultQueueSize()),
                        new ThreadFactory() {

                            public Thread newThread(Runnable r) {
                                return new Thread(r, "Parallel.x thread");
                            }
                        }));
            }

            return getDefaultExecutor();
        }
        else
        {
            return new ThreadPoolExecutor(
                    numProcessors,
                    numProcessors,
                    0,
                    TimeUnit.SECONDS,
                    new ArrayBlockingQueue<Runnable>(Integer.MAX_VALUE),
                    new ThreadFactory() {

                        public Thread newThread(Runnable r) {
                            return new Thread(r, "Parallel.x thread");
                        }
                    });
        }
    }
    public static ThreadPoolExecutor getDefaultExecutor() {
        return defaultExecutor;
    }

    public static void setDefaultExecutor(ThreadPoolExecutor aDefaultExecutor) {
        defaultExecutor = aDefaultExecutor;
    }

    public static Thread run(final ITask task)
    {
        Runnable r = new Runnable() {

            public void run() {
                try {
                    task.execute();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        Thread t = new Thread(r, "Parallel.run thread");

        runningTasks.put(task, t);

        t.start();

        return t;
    }

    public static Thread run(Runnable r)
    {
        Thread t = new Thread(r, "Parallel.run Thread");

        t.start();

        return t;
    }

    public static void forLoop(int from, int to, final Consumer<Integer> loopBody) throws Exception
    {
        forLoop(from, to, loopBody, null);
    }

    public static void forLoop(int from, int to, final Consumer<Integer> loopBody, String message) throws Exception
    {
        List<Integer> lst = new LinkedList<Integer>();

        for(int i = from; i < to; i++)
            lst.add(i);

        new Parallel<Integer>().foreach(lst, loopBody, message);
    }

    public boolean tryForeach(Iterable<T> items, final Consumer<T> loopBody)
    {
        return tryForeach(items, loopBody, null);
    }

    public boolean tryForeach(Iterable<T> items, final Consumer<T> loopBody, String message)
    {
        try {
            foreach(items, loopBody, message);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public void foreach(Iterable<T> items, final Consumer<T> loopBody) throws Exception
    {
        foreach(items, loopBody, null);
    }

    public void foreach(Iterable<T> items, final Consumer<T> body, String message) throws Exception
    {
        if(startParallelProcess(this) && getNumProcessors(this) > 1) {

            Iterator<T> it = items.iterator();

            final ThreadPoolExecutor pool = new ThreadPoolExecutor(
                    getNumProcessors(this),
                    getNumProcessors(this),
                    0,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {

                public Thread newThread(Runnable r) {
                    return new Thread(r, "Parallel.foreach thread");
                }
            });

            final HashMap<ExtendedRunnable, Integer> failedTasks = new HashMap<>();

            while(it.hasNext())
            {
                final T value = it.next();

                Runnable r = new ExtendedRunnable() {
                    public void run() {

                        try {

                            // try to execute the task for the current item
                            body.execute(value);
                        } catch(Exception e) {
                            // in case of failure, print the exception
                            e.printStackTrace();

                            this.setException(e);

                            synchronized (failedTasks) {
                                Integer cnt = failedTasks.get(this);

                                if(cnt==null) {
                                    cnt = 0;
                                }

                                // count how often this task failed
                                cnt++;

                                failedTasks.put(this, cnt);

                                if(cnt<MAX_FAILED_TASKS) {
                                    if(!(e instanceof InterruptedException)) {
                                        // queue the current item again to re-try execution
                                        pool.execute(this);
                                    }
                                } else {
                                    // and if any item failed more than MAX_FAILED_TASKS times, cancel the whole loop
                                    pool.shutdownNow();
                                }
                            }
                        }

                    }
                };

                pool.execute(r);
            }

            RunnableProgressReport p = new RunnableProgressReport();
            p.setPool(pool);
            p.setMessage(message);
            p.setReportIfStuck(reportIfStuck);

            p.initialise();

            do {

                p.print();

            } while(pool.getQueue().size()>0 || pool.getActiveCount()>0);

            pool.shutdown();

            pool.awaitTermination(1, TimeUnit.DAYS);
            p.stop();

//    		System.out.println("Parallel.foreach completed");

            // check whether the pool shut down normally or not
            Entry<ExtendedRunnable, Integer> maxFailed = Q.max(failedTasks.entrySet(), new Func<Integer, Entry<ExtendedRunnable, Integer>>() {
                @Override
                public Integer invoke(Entry<ExtendedRunnable, Integer> in) {
                    return in.getValue();
                }
            });

            // if the max failed count is 3, the execution was cancelled
            if(maxFailed!=null && maxFailed.getValue()>=MAX_FAILED_TASKS) {
                // so we re-throw the exception that caused the cancellation
                throw maxFailed.getKey().getException();
            }
        } else {
            sequentialFor(items, body, message);
        }


        endParallelProcess(this);
    }

    public static void setDefaultQueueSize(int size)
    {
        defaultQueueSize = size;
    }

    public static int getDefaultQueueSize()
    {
        return defaultQueueSize;
    }

    protected void sequentialFor(Iterable<T> items, final Consumer<T> body, String message) {

        for(T item : items) {
            body.execute(item);
        }

    }

    public void producerConsumer(final Producer<T> producer, final Consumer<T> consumer)
    {
        boolean isOuter = startParallelProcess(this);
        //final Timer tim = new Timer("Parallel.producerConsumer");

        if(getNumProcessors(this)>1) {

            ThreadPoolExecutor pool = new ThreadPoolExecutor(
                    getNumProcessors(this),
                    getNumProcessors(this),
                    0,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {

                public Thread newThread(Runnable r) {
                    return new Thread(r, "Parallel.producerConsumer thread");
                }
            });

            //ThreadPoolExecutor pool = getExecutor(getNumProcessors());

            producer.setConsumer(consumer);
            producer.setPool(pool);

            RunnableProgressReport rpr = new RunnableProgressReport();
            rpr.setPool(pool);
            //p.setTimer(timerToReport);
            if(isOuter)
                rpr.start();

            // start the producer thread
            ITask producerTask = new Task() {

                @Override
                public void execute() {
                    //Timer tp = new Timer("Producer", tim);
                    producer.execute();
                }
            };

            run(producerTask);

            join(producerTask);

            pool.shutdown();

            try {
                pool.awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            rpr.stop();

        } else {
            producer.setRunSingleThreaded(true);
            producer.setConsumer(consumer);
            producer.execute();
        }

        endParallelProcess(this);
    }

    public static boolean join(Thread t)
    {
        try {
            t.join();
            return true;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean join(ITask task)
    {
        if(!runningTasks.containsKey(task))
            return false;

        try {
            Thread t = runningTasks.get(task);
            t.join();
            runningTasks.remove(task);
            return true;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean cancel(ITask task) {
        if(!runningTasks.containsKey(task))
            return false;

        Thread t = runningTasks.get(task);
        t.interrupt();
        runningTasks.remove(task);
        return true;
    }
}
