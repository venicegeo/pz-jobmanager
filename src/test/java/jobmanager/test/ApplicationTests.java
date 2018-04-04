package jobmanager.test;

import jobmanager.Application;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import util.PiazzaLogger;

import java.lang.reflect.Method;
import java.util.concurrent.Executor;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@TestPropertySource(locations="classpath:test.properties")
public class ApplicationTests {

    @Configuration
    static class ContextConfiguration {

        // this bean will be injected into the Application class
        @Bean
        public Application orderService() {
            return new Application();
        }
    }

    @Autowired
    private Application application;

    /**
     * Initialize Mock objects.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetAsyncExecutor() {
        Executor executor = this.application.getAsyncExecutor();

        Assert.assertNotNull(executor);
    }

    @Test
    public void testUncaughtExceptionHandler()
        throws NoSuchMethodException
    {
        AsyncUncaughtExceptionHandler handler = this.application.getAsyncUncaughtExceptionHandler();

        handler.handleUncaughtException(new Exception("test exception"), this.getClass().getMethod("testUncaughtExceptionHandler"), "Exception Param 1", "Exception Param 2");

        Assert.assertNotNull(handler);
    }



}
