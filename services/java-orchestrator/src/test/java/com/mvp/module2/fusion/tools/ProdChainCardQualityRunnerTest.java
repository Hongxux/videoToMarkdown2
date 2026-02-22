package com.mvp.module2.fusion.tools;

import org.junit.jupiter.api.Test;

public class ProdChainCardQualityRunnerTest {

    @Test
    void runProdChainQuality() throws Exception {
        String taskId = System.getProperty(
                "prod.chain.taskId",
                "storage:c786a1956e66ba020dfb2ed46a3b0c3c"
        );
        String markdownPath = System.getProperty(
                "prod.chain.markdownPath",
                "var/storage/storage/c786a1956e66ba020dfb2ed46a3b0c3c/enhanced_output.md"
        );
        String userId = System.getProperty(
                "prod.chain.userId",
                "prod_chain_quality"
        );

        ProdChainCardQualityRunner.main(new String[]{taskId, markdownPath, userId});
    }
}
