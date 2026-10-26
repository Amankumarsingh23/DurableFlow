package io.durableflow.security;
import io.durableflow.workflow.definition.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.*;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import java.net.URI;
import java.net.http.*;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

// Close pollers/pools before this class's disposable PostgreSQL container stops.
@org.springframework.test.annotation.DirtiesContext(classMode = org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
@Testcontainers
@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT,properties={"durableflow.limits.http-body-bytes=1024","durableflow.limits.workflow-input-bytes=64","durableflow.limits.signal-bytes=32","durableflow.limits.schedule-input-bytes=64","durableflow.timers.polling-enabled=false","durableflow.children.polling-enabled=false","durableflow.schedules.polling-enabled=false"})
class SecurityBoundaryIntegrationTest {
    @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer("postgres:17.6");
    @DynamicPropertySource static void database(DynamicPropertyRegistry r) {r.add("spring.datasource.url",POSTGRES::getJdbcUrl);r.add("spring.datasource.username",POSTGRES::getUsername);r.add("spring.datasource.password",POSTGRES::getPassword);}
    @LocalServerPort int port;@Autowired WorkflowDefinitionService definitions;@Autowired JdbcTemplate jdbc;
    private HttpResponse<String> post(String path,String body) throws Exception {
        return HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create("http://localhost:"+port+"/api/"+path)).header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build(),HttpResponse.BodyHandlers.ofString());
    }
    @Test void bodyLimitAndUnknownControlFieldsAreRejectedWithoutPersistence() throws Exception {
        assertThat(post("workflow-definitions"," ".repeat(1025)).statusCode()).isEqualTo(413);
        assertThat(post("workflow-definitions","{\"name\":\"unknown-field\",\"version\":1,\"steps\":[{\"id\":\"a\",\"activity\":\"chargePayment\"}],\"adminOverride\":true}").statusCode()).isEqualTo(400);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM workflow_definition WHERE name='unknown-field'",Long.class)).isZero();
    }
    @Test void decodedWorkflowAndSignalPayloadLimitsFailBeforeAnyRunOrReceiptWrite() throws Exception {
        String name="Bounded-"+UUID.randomUUID();definitions.register(new RegisterWorkflowDefinitionRequest(name,1,List.of(new StepDefinition("a","chargePayment",List.of()))));
        String body="{\"workflowName\":\""+name+"\",\"version\":1,\"input\":{\"x\":\""+"x".repeat(100)+"\"}}";
        var response=post("workflow-runs",body);assertThat(response.statusCode()).isEqualTo(413);assertThat(response.body()).contains("WORKFLOW_INPUT_BYTES_LIMIT");
        var signal=post("workflow-runs/"+UUID.randomUUID()+"/signals","{\"signalId\":\"s\",\"signalNodeId\":\"approval\",\"payload\":{\"x\":\""+"x".repeat(50)+"\"}}");
        assertThat(signal.statusCode()).isEqualTo(413);assertThat(signal.body()).contains("SIGNAL_BYTES_LIMIT");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM workflow_run",Long.class)).isZero();assertThat(jdbc.queryForObject("SELECT count(*) FROM workflow_signal_delivery",Long.class)).isZero();
    }
}
