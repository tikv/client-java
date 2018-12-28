package org.tikv.txn.type;

import org.tikv.common.region.TiRegion;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class BatchKeys {
    private List<byte[]> keys;

    private Long regioId;

    public BatchKeys(){}

    public BatchKeys(Long regioId, List<byte[]> keysInput) {
        Objects.nonNull(regioId);
        Objects.nonNull(keysInput);
        this.regioId = regioId;
        this.keys = new ArrayList<>();
        this.keys.addAll(keysInput);
    }

    public List<byte[]> getKeys() {
        return keys;
    }

    public void setKeys(List<byte[]> keys) {
        this.keys = keys;
    }

    public Long getRegioId() {
        return regioId;
    }

    public void setRegioId(Long regioId) {
        this.regioId = regioId;
    }
}
