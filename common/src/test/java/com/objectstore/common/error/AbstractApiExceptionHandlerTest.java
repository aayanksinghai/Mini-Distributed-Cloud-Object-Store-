package com.objectstore.common.error;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class AbstractApiExceptionHandlerTest {

    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.standaloneSetup(new ThrowingController())
                .setControllerAdvice(new TestHandler())
                .build();
    }

    @Test
    void apiExceptionMapsStatusCodeAndDetails() throws Exception {
        mvc.perform(get("/test/api"))
                .andExpect(status().isConflict())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.status").value(409))
                .andExpect(jsonPath("$.error").value("Conflict"))
                .andExpect(jsonPath("$.code").value("OBJECT_NOT_READY"))
                .andExpect(jsonPath("$.message").value("still uploading"))
                .andExpect(jsonPath("$.path").value("/test/api"))
                .andExpect(jsonPath("$.details.objectId").value("abc"))
                .andExpect(jsonPath("$.timestamp").exists());
    }

    @Test
    void unexpectedExceptionBecomes500WithoutLeakingInternals() throws Exception {
        mvc.perform(get("/test/boom"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.code").value(ErrorCode.INTERNAL_ERROR))
                .andExpect(jsonPath("$.message").value("An unexpected internal error occurred"));
    }

    @Test
    void missingParameterBecomes400() throws Exception {
        mvc.perform(get("/test/param"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value(ErrorCode.VALIDATION_ERROR));
    }

    @Test
    void typeMismatchBecomes400() throws Exception {
        mvc.perform(get("/test/param").param("n", "not-a-number"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value(ErrorCode.VALIDATION_ERROR));
    }

    @Test
    void malformedJsonBecomes400() throws Exception {
        mvc.perform(post("/test/json").contentType(MediaType.APPLICATION_JSON).content("{not json"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value(ErrorCode.MALFORMED_REQUEST));
    }

    @Test
    void unsupportedMethodBecomes405() throws Exception {
        mvc.perform(post("/test/boom"))
                .andExpect(status().isMethodNotAllowed())
                .andExpect(jsonPath("$.code").value(ErrorCode.METHOD_NOT_ALLOWED));
    }

    @RestControllerAdvice
    static class TestHandler extends AbstractApiExceptionHandler {
    }

    @RestController
    static class ThrowingController {
        @GetMapping("/test/api")
        String api() {
            throw new ApiException(409, "OBJECT_NOT_READY", "still uploading", Map.of("objectId", "abc"), null);
        }

        @GetMapping("/test/boom")
        String boom() {
            throw new IllegalStateException("secret internal detail");
        }

        @GetMapping("/test/param")
        String param(@RequestParam int n) {
            return "ok" + n;
        }

        @PostMapping("/test/json")
        String json(@RequestBody Map<String, String> body) {
            return "ok";
        }
    }
}

