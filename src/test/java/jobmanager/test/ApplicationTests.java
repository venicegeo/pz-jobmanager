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
import org.springframework.test.util.ReflectionTestUtils;
import util.PiazzaLogger;

import java.lang.reflect.Method;
import java.util.concurrent.Executor;

public class ApplicationTests {

    //@Mock
    private Application application = new Application();

    /**
     * Initialize Mock objects.
     */
    @Before
    public void setup() {

        MockitoAnnotations.initMocks(this);

        ReflectionTestUtils.setField(application, "threadCountSize", 100);
        ReflectionTestUtils.setField(application, "threadCountLimit", 500);
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
