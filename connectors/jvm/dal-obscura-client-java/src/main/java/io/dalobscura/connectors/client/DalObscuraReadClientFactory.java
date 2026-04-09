package io.dalobscura.connectors.client;

import java.io.Serializable;

@FunctionalInterface
public interface DalObscuraReadClientFactory extends Serializable {
    DalObscuraReadClient create();
}
