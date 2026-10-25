package io.durableflow.ai.routing;
import io.durableflow.workflow.activity.ActivityExecutionRepository;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;
import java.util.*;
@RestController @Profile("!worker")
@RequestMapping("/api/workflow-runs")
public class ModelRoutingController {
    private final ActivityExecutionRepository activities;private final ModelRouteRepository routes;
    public ModelRoutingController(ActivityExecutionRepository activities,ModelRouteRepository routes){this.activities=activities;this.routes=routes;}
    @GetMapping("/{runId}/model-routes")
    public List<Map<String,Object>> get(@PathVariable UUID runId) {
        var result=new ArrayList<Map<String,Object>>();
        for(var a:activities.findByWorkflowRunIdOrderByActivityIdAsc(runId)) {
            var route=routes.find(a.getId());if(route.isPresent())result.add(Map.of("activityExecutionId",a.getId().toString(),"activityStatus",a.getStatus().name(),"plan",route.get().plan().toMap(),
                "activeIndex",route.get().activeIndex(),"status",route.get().status(),"attempts",routes.attempts(a.getId())));
        }
        return result;
    }
}
