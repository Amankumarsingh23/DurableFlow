package io.durableflow.ai.routing;
import io.durableflow.ai.provider.FakeModelProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
@Component
public class FakeAProvider extends RoutedFakeProvider {
    public FakeAProvider(FakeModelProvider delegate,@Value("${durableflow.routing.fake-a-script:SUCCESS}") String script) {super(delegate,script);}
    public String id() {return "fake-a";}
}
