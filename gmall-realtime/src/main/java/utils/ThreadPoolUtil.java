package utils;

import sun.nio.ch.ThreadPool;

import java.util.Objects;
import java.util.concurrent.*;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.08
 * @desc:
 */
public class ThreadPoolUtil {
    private static ThreadPoolExecutor threadPoolExecutor;

    private ThreadPoolUtil() {}

    public static ThreadPoolExecutor getThreadPoolExecutor(){
        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(2,
                            4,
                            1L,
                            TimeUnit.MINUTES,
                            new LinkedBlockingDeque<>());
                }







                
            }
        }

        return threadPoolExecutor;
    }
}
