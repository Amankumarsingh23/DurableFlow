package io.durableflow.evaluation;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class EvalReportRenderer {
    private EvalReportRenderer() {}
    public static String json(EvalRunner.Report report) {
        try {return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(report);}
        catch(Exception invalid) {throw new IllegalStateException("EVAL_REPORT_ENCODING",invalid);}
    }
    public static String markdown(EvalRunner.Report report) {
        var out=new StringBuilder("# DurableFlow offline evaluation\n\n");
        out.append("Suite: ").append(report.suiteId()).append(" / ").append(report.suiteVersion()).append("\n\n");
        out.append("Total: ").append(report.total()).append("; passed: ").append(report.passed()).append("; failed: ").append(report.failed()).append("; pass rate: ").append(report.passRate()).append("\n\n");
        out.append("| Case | Result | Failed categories |\n| --- | --- | --- |\n");
        for(var result:report.results())out.append("| ").append(result.caseId()).append(" | ").append(result.passed()?"PASS":"FAIL").append(" | ").append(result.checks().stream().filter(c -> !c.passed()).map(EvalResult.Check::category).distinct().toList()).append(" |\n");
        out.append("\nVersions: ").append(report.versions()).append("\n\nMeasured aggregates (not billing): ").append(report.aggregates()).append("\n");return out.toString();
    }
}
