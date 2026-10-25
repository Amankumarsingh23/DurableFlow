package io.durableflow.security;
import jakarta.servlet.*;
import jakarta.servlet.http.*;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Set;

/** Caps real bytes before JSON binding, including unknown Content-Length/chunked requests.
 * Bounded eager buffering is intentional for this JSON-only demo API. Ingress still needs time/rate limits. */
@Component @Order(Ordered.HIGHEST_PRECEDENCE)
public class BoundedRequestFilter extends OncePerRequestFilter {
    private final ResourceLimits limits;
    public BoundedRequestFilter(ResourceLimits limits) {this.limits=limits;}
    @Override protected void doFilterInternal(HttpServletRequest request,HttpServletResponse response,FilterChain chain) throws ServletException,IOException {
        if(!Set.of("POST","PUT","PATCH").contains(request.getMethod())) {chain.doFilter(request,response);return;}
        int max=limits.httpBodyBytes();
        if(request.getContentLengthLong()>max) {reject(response);return;}
        byte[] body=request.getInputStream().readNBytes(max+1);
        if(body.length>max) {reject(response);return;}
        chain.doFilter(new HttpServletRequestWrapper(request) {
            @Override public ServletInputStream getInputStream() {
                var input=new ByteArrayInputStream(body);
                return new ServletInputStream() {
                    @Override public int read() {return input.read();}
                    @Override public int read(byte[] bytes,int offset,int length) {return input.read(bytes,offset,length);}
                    @Override public boolean isFinished() {return input.available()==0;}
                    @Override public boolean isReady() {return true;}
                    @Override public void setReadListener(ReadListener listener) {throw new UnsupportedOperationException("Synchronous JSON body only");}
                };
            }
            @Override public BufferedReader getReader() {return new BufferedReader(new InputStreamReader(getInputStream(),StandardCharsets.UTF_8));}
        },response);
    }
    private void reject(HttpServletResponse response) throws IOException {
        response.setStatus(413);response.setContentType("application/json");response.getWriter().write("{\"status\":413,\"message\":\"HTTP_BODY_BYTES_LIMIT\"}");
    }
}
