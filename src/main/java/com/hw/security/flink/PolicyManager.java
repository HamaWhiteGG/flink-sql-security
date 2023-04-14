package com.hw.security.flink;

import com.hw.security.flink.policy.DataMaskPolicy;
import com.hw.security.flink.policy.RowFilterPolicy;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * @description: PolicyManager
 * @author: HamaWhite
 */
public class PolicyManager {

    private final List<RowFilterPolicy> rowFilterPolicyList;

    private final List<DataMaskPolicy> dataMaskPolicyList;

    public PolicyManager() {
        this.rowFilterPolicyList = new LinkedList<>();
        this.dataMaskPolicyList = new LinkedList<>();
    }

    public PolicyManager(List<RowFilterPolicy> rowFilterPolicyList, List<DataMaskPolicy> dataMaskPolicyList) {
        this.rowFilterPolicyList = rowFilterPolicyList;
        this.dataMaskPolicyList = dataMaskPolicyList;
    }

    public Optional<String> getRowFilterCondition(String username, String catalogName, String database, String tableName) {
        for (RowFilterPolicy policy : rowFilterPolicyList) {
            if (policy.getUsername().equals(username)
                    && policy.getCatalogName().equals(catalogName)
                    && policy.getDatabase().equals(database)
                    && policy.getTableName().equals(tableName)) {
                return Optional.ofNullable(policy.getCondition());
            }
        }
        return Optional.empty();
    }

    public Optional<String> getDataMaskCondition(String username, String catalogName, String database, String tableName
            , String columnName) {
        for (DataMaskPolicy policy : dataMaskPolicyList) {
            if (policy.getUsername().equals(username)
                    && policy.getCatalogName().equals(catalogName)
                    && policy.getDatabase().equals(database)
                    && policy.getTableName().equals(tableName)
                    && policy.getColumnName().equals(columnName)) {
                return Optional.ofNullable(policy.getCondition());
            }
        }
        return Optional.empty();
    }

    public boolean addPolicy(RowFilterPolicy policy) {
        return rowFilterPolicyList.add(policy);
    }

    public boolean removePolicy(RowFilterPolicy policy) {
        return rowFilterPolicyList.remove(policy);
    }

    public boolean addPolicy(DataMaskPolicy policy) {
       return dataMaskPolicyList.add(policy);
    }

    public boolean removePolicy(DataMaskPolicy policy) {
        return dataMaskPolicyList.remove(policy);
    }
}
