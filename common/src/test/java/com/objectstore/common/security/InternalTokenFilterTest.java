package com.objectstore.common.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.objectstore.common.error.ErrorCode;
import com.objectstore.common.http.InternalHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class InternalTokenFilterTest {

    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        mvc = MockMvcBuilders.standaloneSetup(new SampleController())
                .addFilters(new InternalTokenFilter("super-secret", mapper))
                .build();
    }

    @Test
    void internalEndpointRejectsMissingOrWrongToken() throws Exception {
        mvc.perform(get("/internal/ping"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.code").value(ErrorCode.UNAUTHORIZED_INTERNAL));

        mvc.perform(get("/internal/ping").header(InternalHeaders.INTERNAL_TOKEN, "wrong"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.code").value(ErrorCode.UNAUTHORIZED_INTERNAL));
    }

    @Test
    void internalEndpointAllowsValidToken() throws Exception {
        mvc.perform(get("/internal/ping").header(InternalHeaders.INTERNAL_TOKEN, "super-secret"))
                .andExpect(status().isOk())
                .andExpect(content().string("pong"));
    }

    @Test
    void publicEndpointBypassesFilter() throws Exception {
        mvc.perform(get("/api/v1/public"))
                .andExpect(status().isOk())
                .andExpect(content().string("public-ok"));
    }

    @RestController
    static class SampleController {
        @GetMapping("/internal/ping")
        String internalPing() {
            return "pong";
        }

        @GetMapping("/api/v1/public")
        String publicEndpoint() {
            return "public-ok";
        }
    }
}

