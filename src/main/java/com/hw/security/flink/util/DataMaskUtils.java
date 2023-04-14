package com.hw.security.flink.util;

import com.alibaba.fastjson2.JSON;
import com.hw.security.flink.model.DataMaskType;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @description: DataMaskUtils
 * @author: HamaWhite
 */
public class DataMaskUtils {

    private static final String DATA_MASK_TYPES_FILE = "data-mask-types.json";

    private static final List<DataMaskType> MASK_TYPE_LIST;

    private DataMaskUtils() {
    }

    static {
        try {
            byte[] bytes = ResourceReader.readFile(DATA_MASK_TYPES_FILE);
            MASK_TYPE_LIST = JSON.parseArray(new String(bytes), DataMaskType.class);
        } catch (Exception e) {
            throw new SecurityException(String.format("read file %s error", DATA_MASK_TYPES_FILE), e);
        }
    }


    public static DataMaskType getDataMaskType(String typeName) {
        DataMaskType ret = null;
        for (DataMaskType maskType : MASK_TYPE_LIST) {
            if (StringUtils.equals(maskType.getName(), typeName)) {
                ret = maskType;
                break;
            }
        }
        return ret;
    }
}
