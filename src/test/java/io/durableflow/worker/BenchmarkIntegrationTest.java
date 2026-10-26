package io.durableflow.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.signal.*;
import io.durableflow.workflow.task.WorkflowTaskOrchestrator;
import io.durableflow.workflow.timer.TimerTransactions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.*;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;
import static org.assertj.core.api.Assertions.*;

/** Explicit opt-in, isolated PostgreSQL service/SQL benchmark. No fabricated observations.
 * Manual fake handler bypasses TaskWorker executor/heartbeat overhead: this measures the queue protocol. */
@EnabledIfSystemProperty(named="durableflow.benchmark",matches="true")
// Close pollers/pools before this class's disposable PostgreSQL container stops.
@org.springframework.test.annotation.DirtiesContext(classMode = org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
@Testcontainers
@SpringBootTest(properties={"durableflow.timers.polling-enabled=false","durableflow.children.polling-enabled=false","durableflow.schedules.polling-enabled=false"})
class BenchmarkIntegrationTest {
    @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer("postgres:17.6");
    @DynamicPropertySource static void database(DynamicPropertyRegistry r) {r.add("spring.datasource.url",POSTGRES::getJdbcUrl);r.add("spring.datasource.username",POSTGRES::getUsername);r.add("spring.datasource.password",POSTGRES::getPassword);}
    @Autowired WorkflowDefinitionService definitions;@Autowired WorkflowRunService runs;@Autowired WorkflowTaskOrchestrator orchestrator;
    @Autowired TaskTransactions transactions;@Autowired TimerTransactions timers;@Autowired SignalReceiver signals;@Autowired JdbcTemplate jdbc;
    private final Map<String,List<Double>> samples=new ConcurrentHashMap<>();
    private final AtomicInteger errors=new AtomicInteger(),completed=new AtomicInteger();
    private final Set<String> ownerships=ConcurrentHashMap.newKeySet();
    private int integer(String name,int fallback,int max) {int value=Integer.getInteger("bench."+name,fallback);if(value<1 || value>max)throw new IllegalArgumentException("Invalid benchmark "+name);return value;}
    @Test void benchmark() throws Exception {
        int count=integer("workflows",20,10000),workers=integer("workers",1,32),concurrency=integer("concurrency",4,32),seconds=integer("deadlineSeconds",60,600),fanout=integer("fanout",8,64);
        String mode=System.getProperty("bench.scenario","queue");if(!Set.of("start","queue","dag","timer","signal").contains(mode))throw new IllegalArgumentException("Unknown benchmark scenario");
        var steps=new ArrayList<StepDefinition>();
        if(mode.equals("dag")) {
            steps.add(new StepDefinition("root","benchNoop",List.of()));var branches=new ArrayList<String>();
            for(int n=0;n<fanout;n++) {String id="branch-"+n;branches.add(id);steps.add(new StepDefinition(id,"benchNoop",List.of("root")));}
            steps.add(new StepDefinition("join","benchNoop",branches));
        } else if(mode.equals("timer")) {
            steps.add(new StepDefinition("wait",null,List.of(),NodeType.TIMER,25L));steps.add(new StepDefinition("done","benchNoop",List.of("wait")));
        } else if(mode.equals("signal")) {
            steps.add(new StepDefinition("wait",null,List.of(),NodeType.SIGNAL,null));steps.add(new StepDefinition("done","benchNoop",List.of("wait")));
        } else steps.add(new StepDefinition("done","benchNoop",List.of()));
        String name="Benchmark-"+UUID.randomUUID();definitions.register(new RegisterWorkflowDefinitionRequest(name,1,steps));
        var ids=new ConcurrentLinkedQueue<UUID>();var begin=new CountDownLatch(1);var producers=Executors.newFixedThreadPool(concurrency);var futures=new ArrayList<Future<?>>();
        long startPhase=System.nanoTime();
        try {
            for(int n=0;n<count;n++)futures.add(producers.submit(() -> {
                try {begin.await();long before=System.nanoTime();var id=runs.start(new StartWorkflowRunRequest(name,1,Map.of())).runId();record("workflowStartMs",before);ids.add(id);}
                catch(Exception failed) {errors.incrementAndGet();}
            }));
            startPhase=System.nanoTime();begin.countDown();for(var f:futures)f.get(seconds,TimeUnit.SECONDS);
        } finally {producers.shutdownNow();}
        double startSeconds=(System.nanoTime()-startPhase)/1e9;
        long phase=System.nanoTime();int expectedTasks=mode.equals("start")?0:count*(mode.equals("dag")?fanout+2:1);
        if(!mode.equals("start")) {
            for(UUID id:ids)orchestrator.advanceRun(id);
            if(mode.equals("signal"))for(UUID id:ids) {long before=System.nanoTime();signals.receive(id,new ReceiveSignalRequest("bench-signal","wait",Map.of()));record("signalToDownstreamCommitMs",before);}
            if(mode.equals("timer")) {
                long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(seconds);int fired=0;
                while(fired<ids.size() && System.nanoTime()<deadline) {if(timers.fireOne())fired++;else LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));}
                if(fired!=ids.size())errors.incrementAndGet();
            }
            var pool=Executors.newFixedThreadPool(workers);var consumers=new ArrayList<Future<?>>();long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(seconds);
            try {
                for(int w=0;w<workers;w++) {String worker="bench-"+w;consumers.add(pool.submit(() -> {
                    while(completed.get()<expectedTasks && System.nanoTime()<deadline && !Thread.currentThread().isInterrupted()) {
                        try {
                            long before=System.nanoTime();var selected=transactions.claim(worker);record(selected.isPresent()?"successfulClaimCallMs":"emptyClaimCallMs",before);
                            if(selected.isEmpty()) {LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));continue;}
                            var claim=selected.get();
                            if(!ownerships.add(claim.taskId()+":"+claim.leaseGeneration()))errors.incrementAndGet();
                            before=System.nanoTime();transactions.complete(claim,Map.of("benchmark",true));record("completeCallMs",before);
                            before=System.nanoTime();orchestrator.advanceRun(claim.runId());record("advanceCallMs",before);completed.incrementAndGet();
                        } catch(Exception failed) {errors.incrementAndGet();return;}
                    }
                }));}
                for(var f:consumers)f.get(seconds+5L,TimeUnit.SECONDS);
            } finally {pool.shutdownNow();}
        }
        double processSeconds=(System.nanoTime()-phase)/1e9;
        var measurements=new TreeMap<String,Object>();samples.forEach((key,values) -> measurements.put(key,statistics(values)));
        // Immutable per-task first successful claim timestamps in this fresh no-retry fixture.
        if(!mode.equals("start"))measurements.put("queueWaitMs",statistics(jdbc.queryForList("SELECT extract(epoch from (claimed_at-available_at))*1000 FROM activity_task WHERE claimed_at IS NOT NULL",Double.class)));
        if(mode.equals("timer"))measurements.put("timerFiringLagMs",statistics(jdbc.queryForList("SELECT extract(epoch from (fired_at-wake_at))*1000 FROM workflow_timer WHERE fired_at IS NOT NULL",Double.class)));
        if(mode.equals("signal"))measurements.put("signalReceiptToReadyMs",statistics(jdbc.queryForList("SELECT extract(epoch from (t.created_at-s.received_at))*1000 FROM activity_task t JOIN activity_execution a ON a.id=t.activity_execution_id JOIN workflow_signal_delivery s ON s.workflow_run_id=a.workflow_run_id",Double.class)));
        long finished=mode.equals("start")?0:Objects.requireNonNull(jdbc.queryForObject("SELECT count(*) FROM workflow_run WHERE status='COMPLETED'",Long.class));
        var report=new LinkedHashMap<String,Object>();report.put("harnessVersion","sql-service-v1");report.put("scenario",mode);report.put("workers",workers);report.put("startConcurrency",concurrency);report.put("requestedWorkflows",count);report.put("startedWorkflows",ids.size());report.put("expectedTasks",expectedTasks);report.put("completedTasks",completed.get());report.put("completedWorkflows",finished);report.put("errors",errors.get());
        report.put("deadlineSeconds",seconds);report.put("fanout",mode.equals("dag")?fanout:0);report.put("workflowStartsPerSecond",ids.size()/startSeconds);
        if(!mode.equals("start"))report.put("completedTasksPerSecondIncludingAdvanceAndWaitSetup",completed.get()/processSeconds);
        report.put("measurements",measurements);report.put("javaVersion",System.getProperty("java.version"));report.put("os",System.getProperty("os.name"));report.put("postgresImage","postgres:17.6");
        boolean valid=errors.get()==0 && ids.size()==count && completed.get()==expectedTasks && (mode.equals("start") || finished==count);
        report.put("valid",valid);report.put("limitations","In-process SQL/service harness, no HTTP/network/worker executor/heartbeat costs, no warmup; isolated container. Not a production capacity claim.");
        Files.createDirectories(Path.of("target"));Files.writeString(Path.of("target/benchmark-"+mode+"-w"+workers+".json"),new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(report));
        assertThat(valid).withFailMessage("Benchmark invalid/incomplete; inspect generated report and error count").isTrue();
    }
    private void record(String name,long before) {samples.computeIfAbsent(name,k -> Collections.synchronizedList(new ArrayList<>())).add((System.nanoTime()-before)/1e6);}
    private Map<String,Object> statistics(List<Double> values) {
        var sorted=values.stream().sorted().toList();if(sorted.isEmpty())return Map.of("samples",0,"status","NO_OBSERVATIONS");
        return Map.of("samples",sorted.size(),"p50",quantile(sorted,0.50),"p95",quantile(sorted,0.95),"p99",quantile(sorted,0.99));
    }
    private double quantile(List<Double> sorted,double p) {return sorted.get(Math.max(0,(int)Math.ceil(p*sorted.size())-1));}
}
