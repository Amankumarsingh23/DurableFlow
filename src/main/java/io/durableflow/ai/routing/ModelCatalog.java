package io.durableflow.ai.routing;
import org.springframework.stereotype.Component;
import java.util.*;
@Component
public class ModelCatalog {
    private final List<ModelDescriptor> models;
    public ModelCatalog() {this(List.of(
        new ModelDescriptor("fake-a","balanced-v1",2,1,2,new ModelCapabilities(true,true,24000),true,Set.of("local","restricted")),
        new ModelDescriptor("fake-b","premium-v1",3,3,4,new ModelCapabilities(true,true,48000),true,Set.of("local")),
        new ModelDescriptor("fake-b","economy-v1",1,1,1,new ModelCapabilities(true,false,12000),true,Set.of("local")),
        new ModelDescriptor("fake-a","text-v1",1,0,1,new ModelCapabilities(false,false,12000),true,Set.of("local","restricted"))));}
    public ModelCatalog(List<ModelDescriptor> models) {
        if(models.isEmpty() || models.size()>32 || models.stream().map(m -> m.provider()+"/"+m.model()).distinct().count()!=models.size())throw new IllegalArgumentException("Invalid model catalog");this.models=List.copyOf(models);
    }
    public List<ModelDescriptor> models() {return models;}
}
