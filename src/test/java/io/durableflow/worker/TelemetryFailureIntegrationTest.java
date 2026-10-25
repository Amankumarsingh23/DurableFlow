package io.durableflow.worker;
import io.durableflow.agent.*;
import io.durableflow.observability.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.*;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.*;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
@SpringBootTest(properties={"durableflow.timers.polling-enabled=false","durableflow.children.polling-enabled=false","durableflow.schedules.polling-enabled=false"})
@Import(TelemetryFailureIntegrationTest.Config.class)
class TelemetryFailureIntegrationTest {
    @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer("postgres:17.6");
    @DynamicPropertySource static void database(DynamicPropertyRegistry r) {r.add("spring.datasource.url",POSTGRES::getJdbcUrl);r.add("spring.datasource.username",POSTGRES::getUsername);r.add("spring.datasource.password",POSTGRES::getPassword);}
    static final class BrokenSink implements DurableFlowTelemetry {
        final CountDownLatch called=new CountDownLatch(1);final AtomicBoolean transactionObserved=new AtomicBoolean();
        private void fail() {transactionObserved.compareAndSet(false,TransactionSynchronizationManager.isActualTransactionActive());called.countDown();throw new IllegalStateException("sink down");}
        public void measure(Metric metric,Kind kind,double value,Map<String,String> labels) {fail();}
        public void event(String event,Map<String,Object> metadata) {fail();}
    }
    @TestConfiguration(proxyBeanMethods=false) static class Config {@Bean @Primary BrokenSink brokenSink() {return new BrokenSink();}}
    @Autowired BrokenSink sink;@Autowired AgentStartService start;@Autowired AgentLoopService loop;@Autowired AgentRepository agents;
    @Autowired TaskWorker worker;@Autowired JdbcTemplate jdbc;
    @Test void throwingTelemetryCannotRollbackEngineHistoryModelUsageOrTerminalAgent() throws Exception {
        var a=start.start(new StartAgentRequest("Return a synthetic final answer.","final",AgentLimits.defaults(),null));
        for(int n=0;n<12;n++) {if(loop.advance(a.id()).terminal())break;worker.runOne();}
        assertThat(agents.get(a.id()).status()).isEqualTo(AgentExecution.Status.COMPLETED);
        assertThat(agents.get(a.id()).modelAttempts()).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM workflow_event WHERE workflow_run_id=?",Long.class,a.workflowRunId())).isPositive();
        assertThat(sink.called.await(5,TimeUnit.SECONDS)).isTrue();assertThat(sink.transactionObserved).isFalse();
    }
}
