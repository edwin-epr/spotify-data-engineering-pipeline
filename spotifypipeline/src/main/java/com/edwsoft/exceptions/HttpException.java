package com.edwsoft.exceptions;

import lombok.Getter;

@Getter
public class HttpException extends RuntimeException {

    private final int statusCode;
    private final String responseBody;

    public HttpException(int statusCode, String responseBody) {
        super(String.format("HTTP request failed with status code: %d", statusCode));
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }
}
