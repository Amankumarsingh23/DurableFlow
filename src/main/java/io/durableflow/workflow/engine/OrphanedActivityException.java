package io.durableflow.workflow.engine;

import io.durableflow.api.ApiException;
import org.springframework.http.HttpStatus;
import java.util.List;
import java.util.UUID;

/** RUNNING is evidence of an unacknowledged attempt, not evidence that its side effect failed. */
public class OrphanedActivityException extends ApiException {
    private final UUID runId;
    private final List<String> activityIds;

    public OrphanedActivityException(UUID runId, List<String> activityIds) {
        super(HttpStatus.CONFLICT, "Recovery required: orphaned RUNNING activities " + activityIds
                + ". Session 8 cannot safely determine whether the previous side effect occurred; automatic retry is disabled.");
        this.runId = runId;
        this.activityIds = List.copyOf(activityIds);
    }

    public UUID runId() { return runId; }
    public List<String> activityIds() { return activityIds; }
}
