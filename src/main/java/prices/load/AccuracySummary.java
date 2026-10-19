package prices.load;

import java.util.List;

record AccuracySummary(
        long nonEmptyAveragesChecked,
        long expectedEmptyResultsExcluded,
        long mismatches,
        List<String> mismatchExamples) {

    AccuracySummary {
        mismatchExamples = List.copyOf(mismatchExamples);
    }

    boolean accurate() {
        return mismatches == 0;
    }
}
