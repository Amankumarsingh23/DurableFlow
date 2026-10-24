package io.durableflow.workflow.signal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.durableflow.api.ApiException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.*;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.*;
import java.time.OffsetDateTime;
import java.util.*;

@Repository
public class SignalRepository {
    private final JdbcTemplate jdbc;
    private final ObjectMapper json = new ObjectMapper();
    public SignalRepository(JdbcTemplate jdbc) { this.jdbc = jdbc; }
    private static final RowMapper<SignalWait> WAIT = (row, index) -> {
        var received = row.getObject("received_at", OffsetDateTime.class);
        return new SignalWait(row.getObject("id", UUID.class), row.getObject("workflow_run_id", UUID.class),
                row.getString("signal_node_id"), SignalWait.Status.valueOf(row.getString("status")),
                row.getObject("created_at", OffsetDateTime.class).toInstant(), received == null ? null : received.toInstant());
    };
    private static final RowMapper<SignalReceipt> DELIVERY = (row, index) -> {
        var consumed = row.getObject("consumed_at", OffsetDateTime.class);
        return new SignalReceipt(row.getObject("id", UUID.class), row.getObject("workflow_run_id", UUID.class),
                row.getString("signal_id"), row.getString("signal_node_id"),
                row.getObject("received_at", OffsetDateTime.class).toInstant(), consumed == null ? null : consumed.toInstant());
    };
    public List<SignalWait> waits(UUID run) {
        return jdbc.query("SELECT * FROM workflow_signal_wait WHERE workflow_run_id=? ORDER BY signal_node_id", WAIT, run);
    }
    public Optional<SignalReceipt> delivery(UUID run, String signalId) {
        return jdbc.query("SELECT * FROM workflow_signal_delivery WHERE workflow_run_id=? AND signal_id=?", DELIVERY, run, signalId).stream().findFirst();
    }
    public Optional<SignalReceipt> forNode(UUID run, String node) {
        return jdbc.query("SELECT * FROM workflow_signal_delivery WHERE workflow_run_id=? AND signal_node_id=?", DELIVERY, run, node).stream().findFirst();
    }
    public boolean matches(SignalReceipt delivery, ReceiveSignalRequest request) {
        return Boolean.TRUE.equals(jdbc.queryForObject("SELECT signal_node_id=? AND payload_json=CAST(? AS jsonb) "
                + "FROM workflow_signal_delivery WHERE id=?", Boolean.class, request.signalNodeId(), encode(request.payload()), delivery.id()));
    }
    @Transactional(propagation = Propagation.MANDATORY)
    public SignalReceipt insert(UUID run, ReceiveSignalRequest request) {
        return Objects.requireNonNull(jdbc.queryForObject("""
                INSERT INTO workflow_signal_delivery (id, workflow_run_id, signal_id, signal_node_id, payload_json)
                VALUES (?, ?, ?, ?, CAST(? AS jsonb)) RETURNING *
                """, DELIVERY, UUID.randomUUID(), run, request.signalId(), request.signalNodeId(), encode(request.payload())));
    }
    @Transactional(propagation = Propagation.MANDATORY)
    public SignalWait createWait(UUID run, String node) {
        return Objects.requireNonNull(jdbc.queryForObject("INSERT INTO workflow_signal_wait "
                + "(id, workflow_run_id, signal_node_id, status) VALUES (?, ?, ?, 'WAITING') RETURNING *", WAIT, UUID.randomUUID(), run, node));
    }
    /** All signal writers hold the parent run lock, so exact node and delivery matching cannot race. */
    @Transactional(propagation = Propagation.MANDATORY)
    public SignalWait consume(SignalWait wait, SignalReceipt delivery) {
        var received = jdbc.queryForObject("""
                UPDATE workflow_signal_wait w SET status='RECEIVED', received_at=clock_timestamp(), payload_json=d.payload_json
                FROM workflow_signal_delivery d WHERE w.id=? AND w.status='WAITING' AND d.id=?
                  AND d.workflow_run_id=w.workflow_run_id AND d.signal_node_id=w.signal_node_id AND d.consumed_at IS NULL
                RETURNING w.*
                """, WAIT, wait.id(), delivery.id());
        if (jdbc.update("UPDATE workflow_signal_delivery SET consumed_at=clock_timestamp(), consumed_wait_id=? "
                + "WHERE id=? AND consumed_at IS NULL", wait.id(), delivery.id()) != 1) {
            throw new IllegalStateException("Signal delivery already consumed");
        }
        return Objects.requireNonNull(received);
    }
    private String encode(Map<String, Object> payload) {
        try { return json.writeValueAsString(payload); }
        catch (JsonProcessingException failure) { throw new ApiException(HttpStatus.BAD_REQUEST, "Signal payload must be a JSON object"); }
    }
}
