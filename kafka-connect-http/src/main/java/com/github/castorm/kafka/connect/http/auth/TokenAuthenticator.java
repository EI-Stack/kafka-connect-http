package com.github.castorm.kafka.connect.http.auth;

/*-
 * #%L
 * Kafka Connect HTTP
 * %%
 * Copyright (C) 2020 - 2021 Cástor Rodríguez
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import com.github.castorm.kafka.connect.http.auth.spi.HttpAuthenticator;
import com.github.castorm.kafka.connect.http.client.okhttp.OkHttpClient;

import org.apache.kafka.connect.errors.RetriableException;

import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.MediaType;

public class TokenAuthenticator implements HttpAuthenticator {
    private final Function<Map<String, ?>, TokenAuthenticatorConfig> configFactory;
    private TokenAuthenticatorConfig config;

    public TokenAuthenticator() {
        this(TokenAuthenticatorConfig::new);
    }

    public TokenAuthenticator(Function<Map<String, ?>, TokenAuthenticatorConfig> configFactory) {
        this.configFactory = configFactory;
    }

    @Override
    public Optional<String> getAuthorizationHeader() {
        String response = "";
        try {
            response = fetchData();
        } catch (Exception e) {
            throw new RetriableException("Error: " + e.getMessage(), e);
        }
        if (response.isBlank()) {
            throw new RetriableException("Error: Access token is empty.");
        }
        return Optional.of("Bearer " + response);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.config = configFactory.apply(configs);
    }

    private String fetchData() {
        String tokenBody = config.getTokenBody().value();
        RequestBody requestBody = RequestBody.create(tokenBody, MediaType.parse("application/json; charset=utf-8"));

        okhttp3.OkHttpClient httpClient = OkHttpClient.getUnsafeOkHttpClient().build();
        Request request = new Request.Builder().url(config.getTokenUrl()).post(requestBody).build();
        Response response = null;
        String result = "";
        try {
            response = httpClient.newCall(request).execute();
            result = response.body().string();
        } catch (IOException e) {
            throw new RetriableException("Error: " + e.getMessage(), e);
        }
        return result;
    }

}
