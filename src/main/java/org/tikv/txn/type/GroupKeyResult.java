package org.tikv.txn.type;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroupKeyResult extends BaseResult{

    private Map<Long, List<byte[]>> groupsResult;

    private Long firstRegion;

    public GroupKeyResult() {
        this.groupsResult = new HashMap<>();
    }

    public Map<Long, List<byte[]>> getGroupsResult() {
        return groupsResult;
    }

    public void setGroupsResult(Map<Long, List<byte[]>> groupsResult) {
        this.groupsResult = groupsResult;
    }

    public Long getFirstRegion() {
        return firstRegion;
    }

    public void setFirstRegion(Long firstRegion) {
        this.firstRegion = firstRegion;
    }
}
