package io.dazzleduck.sql.logger;

import io.dazzleduck.sql.common.ingestion.FlightSender;
import org.junit.jupiter.api.*;
import org.mockito.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ArrowSimpleLoggerTest {

    @Mock
    private FlightSender mockSender;

    private ArrowSimpleLogger logger;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        doNothing().when(mockSender).enqueue(any(byte[].class));
        doNothing().when(mockSender).setIngestEndpoint(anyString());
        logger = new ArrowSimpleLogger("test-logger", mockSender);
    }

    @AfterEach
    void teardown() {
        logger.close();
    }

    @Test
    void testLoggerInitialization_ReadsConfig() {
        assertNotNull(logger);
        assertNotNull(logger.applicationId);
        assertNotNull(logger.applicationName);
        assertNotNull(logger.host);
    }

    @Test
    void testSingleLogEntry_EnqueueNotCalledYet() {
        logger.info("Hello {}", "World");
        assertEquals(1, logger.batchCounter.get());
        verify(mockSender, never()).enqueue(any(byte[].class));
    }

    @Test
    void testBatchFlushWhenMaxBatchSizeReached() {
        for (int i = 0; i < 10; i++) {
            logger.info("Log {}", i);
        }
        // After 10 logs, batch should flush
        verify(mockSender, atLeastOnce()).enqueue(any(byte[].class));
        assertEquals(0, logger.batchCounter.get());
    }

    @Test
    void testFlushMethod_SendsRemainingLogs() {
        logger.info("Log1");
        logger.info("Log2");
        assertEquals(2, logger.batchCounter.get());
        logger.flush();
        verify(mockSender, atLeastOnce()).enqueue(any(byte[].class));
        assertEquals(0, logger.batchCounter.get());
    }

    @Test
    void testClose_FlushesRemainingLogs() {
        logger.info("Log before close");
        logger.close();
        verify(mockSender, atLeastOnce()).enqueue(any(byte[].class));
    }

    @Test
    void testLogWithLoggingEvent() {
        org.slf4j.event.LoggingEvent event = mock(org.slf4j.event.LoggingEvent.class);
        when(event.getLevel()).thenReturn(org.slf4j.event.Level.INFO);
        when(event.getMessage()).thenReturn("Event log");
        logger.log(event);
        assertEquals(1, logger.batchCounter.get());
    }

    @Test
    void testFormat_ReplacesPlaceholders() throws Exception {
        var method = ArrowSimpleLogger.class.getDeclaredMethod("format", String.class, Object[].class);
        method.setAccessible(true);
        String formatted = (String) method.invoke(logger, "Hello {} {}", new Object[]{"World", 123});
        assertEquals("Hello World 123", formatted);
    }

    @Test
    void testWriteArrowAsync_HandlesExceptionGracefully() {
        // Simulate enqueue throwing exception
        doThrow(new RuntimeException("Test enqueue fail"))
                .when(mockSender).enqueue(any(byte[].class));

        // Write MAX_BATCH_SIZE logs to trigger flush
        for (int i = 0; i < 10; i++) {
            logger.info("Log {}", i);
        }

        // No exception should propagate, counter resets
        assertEquals(0, logger.batchCounter.get());
    }

    @Test
    void testMultipleBatches() {
        // Log 25 entries = 2 full batches + 5 remaining
        for (int i = 0; i < 25; i++) {
            logger.info("Log {}", i);
        }
        // Should have flushed 2 times (after 10 and 20)
        verify(mockSender, times(2)).enqueue(any(byte[].class));
        assertEquals(5, logger.batchCounter.get());
    }

    @Test
    void testLogLevels() {
        assertTrue(logger.isInfoEnabled());
        assertTrue(logger.isWarnEnabled());
        assertTrue(logger.isErrorEnabled());
        assertFalse(logger.isDebugEnabled());
        assertFalse(logger.isTraceEnabled());
    }

    /**
     * Test with a mock AbstractFlightSender to verify start() is called
     */
    @Test
    void testFlightSenderStart_IsCalled() {
        MockAbstractFlightSender mockAbstractSender = spy(new MockAbstractFlightSender());

        ArrowSimpleLogger testLogger = new ArrowSimpleLogger("test", mockAbstractSender);

        verify(mockAbstractSender, times(1)).start();

        testLogger.close();
    }

    /**
     * Mock implementation of AbstractFlightSender for testing
     */
    static class MockAbstractFlightSender extends FlightSender.AbstractFlightSender {
        @Override
        public long getMaxInMemorySize() {
            return 1024 * 1024; // 1MB
        }

        @Override
        public long getMaxOnDiskSize() {
            return 10 * 1024 * 1024; // 10MB
        }

        @Override
        protected void doSend(SendElement element) throws InterruptedException {
            // Mock implementation - do nothing
            element.cleanup();
        }
    }
}