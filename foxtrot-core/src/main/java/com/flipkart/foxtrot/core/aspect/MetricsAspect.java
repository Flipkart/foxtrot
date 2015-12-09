package com.flipkart.foxtrot.core.aspect;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

/**
 * Created by santanu.s on 03/12/15.
 */
@Aspect
public class MetricsAspect {

    @Pointcut(value = "execution(@com.yammer.metrics.annotation.Timed * *(..))")
    public void timeChecked() {}

    @Around(value = "timeChecked()")
    public Object around(ProceedingJoinPoint joinPoint) throws Throwable {
        MetricsRegistry registry = Metrics.defaultRegistry();

        Timer timer = registry.newTimer(joinPoint.getThis().getClass(), joinPoint.getSignature().getName());
        TimerContext time = timer.time();
        try {
            return joinPoint.proceed();
        } finally {
            time.stop();
        }
    }
}
