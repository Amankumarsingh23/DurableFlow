package io.durableflow.ai.routing;
import io.durableflow.ai.provider.FakeModelProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
@Component
public class FakeBProvider extends RoutedFakeProvider {
    public FakeBProvider(FakeModelProvider delegate,@Value("${durableflow.routing.fake-b-script:SUCCESS}") String script) {super(delegate,script);}
    public String id() {return "fake-b";}
}
