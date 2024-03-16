package com.integration.em.processing;

import java.io.Serializable;

public interface Action<InputType> extends Serializable {

    void execute(InputType input);
}
