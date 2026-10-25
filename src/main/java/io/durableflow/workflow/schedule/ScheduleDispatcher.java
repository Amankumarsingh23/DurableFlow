package io.durableflow.workflow.schedule;

import org.springframework.stereotype.Service;

@Service
public class ScheduleDispatcher {
    private final ScheduleTransactions transactions;
    private final ScheduleSettings settings;
    public ScheduleDispatcher(ScheduleTransactions transactions, ScheduleSettings settings) { this.transactions = transactions; this.settings = settings; }
    public int pollBatch() {
        int count = 0;
        while (count < settings.maxCatchUpOccurrences() && transactions.fireOne()) count++;
        return count;
    }
}
