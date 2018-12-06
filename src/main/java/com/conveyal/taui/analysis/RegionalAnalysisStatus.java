package com.conveyal.taui.analysis;


import com.conveyal.taui.GridResultAssembler;

import java.io.Serializable;

/**
 * This model object is sent to the UI serialized as JSON in order to report regional job progress.
 */
public final class RegionalAnalysisStatus implements Serializable {
    public int total;
    public int complete;

    public RegionalAnalysisStatus() { /* No-arg constructor for deserialization only. */ }

    public RegionalAnalysisStatus(GridResultAssembler assembler) {
        total = assembler.nTotal;
        complete = assembler.nComplete;
    }
}
