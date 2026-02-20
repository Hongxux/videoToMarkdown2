package com.mvp.module2.fusion.controller;

import com.mvp.module2.fusion.service.SelectionSyntaxRefineService;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.lang.reflect.Field;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MobileCardControllerSelectionRefineTest {

    @Test
    void refineSelectionShouldKeepOriginalSourceTextOffsets() throws Exception {
        MobileCardController controller = new MobileCardController();
        CapturingSelectionSyntaxRefineService capturingService = new CapturingSelectionSyntaxRefineService();
        injectField(controller, "selectionSyntaxRefineService", capturingService);

        MobileCardController.CardSelectionRefineRequest request = new MobileCardController.CardSelectionRefineRequest();
        request.sourceText = "  智能体协作  ";
        request.cursorOffset = 4;
        request.currentTerm = "智能体";
        request.currentStartOffset = 2;
        request.currentEndOffset = 5;

        ResponseEntity<?> response = controller.refineSelection(request);
        assertEquals(200, response.getStatusCode().value());
        assertEquals(request.sourceText, capturingService.capturedSourceText);
        assertEquals(request.cursorOffset, capturingService.capturedCursorOffset);
        assertEquals(request.currentStartOffset, capturingService.capturedStartOffset);
        assertEquals(request.currentEndOffset, capturingService.capturedEndOffset);

        Object body = response.getBody();
        assertTrue(body instanceof Map);
        Map<?, ?> payload = (Map<?, ?>) body;
        assertEquals(Boolean.TRUE, payload.get("improved"));
    }

    private static void injectField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static class CapturingSelectionSyntaxRefineService extends SelectionSyntaxRefineService {
        private String capturedSourceText;
        private int capturedCursorOffset;
        private int capturedStartOffset;
        private int capturedEndOffset;

        @Override
        public SelectionRefineResult refineSelection(
                String sourceText,
                int cursorOffset,
                String currentTerm,
                int currentStartOffset,
                int currentEndOffset
        ) {
            this.capturedSourceText = sourceText;
            this.capturedCursorOffset = cursorOffset;
            this.capturedStartOffset = currentStartOffset;
            this.capturedEndOffset = currentEndOffset;
            return SelectionRefineResult.improved(currentTerm, currentStartOffset, currentEndOffset, 0.92, "test");
        }
    }
}

