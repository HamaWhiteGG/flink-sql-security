package com.hw.security.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.flink.table.catalog.ObjectIdentifier;

import java.util.List;

/**
 * @description: TableEntity
 * @author: HamaWhite
 */
@Data
@AllArgsConstructor
@Accessors(chain = true)
public class TableEntity {

    private ObjectIdentifier tableIdentifier;

    private List<ColumnEntity> columnList;
}
