package io.durableflow.worker;

import io.durableflow.api.ApiException;
import org.springframework.http.HttpStatus;

/** Rejected results must never be translated into a handler failure against the new owner. */
public class OwnershipLostException extends ApiException {
    public OwnershipLostException(ClaimedTask claim) {
        super(HttpStatus.CONFLICT, "Ownership lost or unconfirmed for task " + claim.taskId()
                + ", worker " + claim.workerId() + ", generation " + claim.leaseGeneration());
    }
}
