package com.hongxu.videoToMarkdownTest2

import androidx.activity.ComponentActivity
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.test.junit4.createAndroidComposeRule
import androidx.compose.ui.test.onAllNodesWithTag
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.performClick
import androidx.test.ext.junit.runners.AndroidJUnit4
import io.noties.markwon.Markwon
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class SemanticTopographyReaderNoiseCapsuleTest {

    @get:Rule
    val composeRule = createAndroidComposeRule<ComponentActivity>()

    @Test
    fun noiseCapsule_canExpandAndCollapse() {
        val nodeId = "noise-node"
        val capsuleTag = "noise_capsule_$nodeId"
        val expandedTag = "noise_expanded_$nodeId"
        val collapseTag = "noise_collapse_affordance_$nodeId"

        composeRule.setContent {
            val context = LocalContext.current
            SemanticTopographyReader(
                nodes = listOf(
                    SemanticNode(
                        id = nodeId,
                        text = "This paragraph is low relevance and should be hidden behind capsule by default.",
                        relevanceScore = 0.12f,
                        bridgeText = "Summary: this is low-priority contextual detail."
                    )
                ),
                markwon = Markwon.create(context),
                renderConfig = MarkdownReaderRenderConfig.defaults()
            )
        }

        waitForTag(capsuleTag)
        composeRule.onNodeWithTag(capsuleTag, useUnmergedTree = true).performClick()

        waitForTag(expandedTag)
        composeRule.onNodeWithTag(collapseTag, useUnmergedTree = true).performClick()

        waitForTag(capsuleTag)
    }

    private fun waitForTag(tag: String, timeoutMs: Long = 5_000L) {
        composeRule.waitUntil(timeoutMs) {
            composeRule.onAllNodesWithTag(tag, useUnmergedTree = true)
                .fetchSemanticsNodes()
                .isNotEmpty()
        }
    }
}
