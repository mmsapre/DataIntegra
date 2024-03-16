package com.integration.em.utils;

import java.io.Serializable;

public interface Function<OutputType, InputType> extends Serializable {

    OutputType execute(InputType input);
}
