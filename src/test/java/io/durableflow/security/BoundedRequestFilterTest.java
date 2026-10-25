package io.durableflow.security;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.assertj.core.api.Assertions.*;

class BoundedRequestFilterTest {
    private BoundedRequestFilter filter() {return new BoundedRequestFilter(new ResourceLimits(16,64,2,1,64,32,32,32,4,20));}
    @Test void rejectsOversizedKnownLengthBeforeCallingControllers() throws Exception {
        var request=new MockHttpServletRequest("POST","/api/workflow-runs");request.setContent("x".repeat(17).getBytes(StandardCharsets.UTF_8));
        var response=new MockHttpServletResponse();var called=new AtomicBoolean();filter().doFilter(request,response,(r,s) -> called.set(true));
        assertThat(response.getStatus()).isEqualTo(413);assertThat(response.getContentAsString()).contains("HTTP_BODY_BYTES_LIMIT");assertThat(called.get()).isFalse();
    }
    @Test void unknownLengthCannotBypassActualByteLimit() throws Exception {
        var request=new MockHttpServletRequest("POST","/api/workflow-runs") {@Override public long getContentLengthLong() {return -1;}};
        request.setContent("x".repeat(17).getBytes(StandardCharsets.UTF_8));var response=new MockHttpServletResponse();
        filter().doFilter(request,response,(r,s) -> fail("Oversized unknown-length body reached controller"));assertThat(response.getStatus()).isEqualTo(413);
    }
    @Test void validBodyReachesJsonBindingUnchanged() throws Exception {
        var request=new MockHttpServletRequest("POST","/api/workflow-runs");byte[] body="{\"x\":\"ok\"}".getBytes(StandardCharsets.UTF_8);request.setContent(body);
        var response=new MockHttpServletResponse();var called=new AtomicBoolean();
        filter().doFilter(request,response,(r,s) -> {called.set(true);assertThat(r.getInputStream().readAllBytes()).isEqualTo(body);});assertThat(called.get()).isTrue();
    }
}
