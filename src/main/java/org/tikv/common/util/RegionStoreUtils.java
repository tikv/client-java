package org.tikv.common.util;

import com.google.protobuf.ByteString;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.TiRegion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Util class for keys split
 */
public class RegionStoreUtils {

    /**
     * split keys to groups
     * @param regionManager
     * @param keys
     * @return
     */
    public static Map<TiRegion, List<ByteString>> groupKeysByRegion(final RegionManager regionManager,
                                                                    List<ByteString> keys) {
        Map<TiRegion, List<ByteString>> groups = new HashMap<>();
        TiRegion lastRegion = null;
        BackOffer bo = ConcreteBackOffer.newCustomBackOff(BackOffer.tsoMaxBackoff);
        for (ByteString key : keys) {
            if (lastRegion == null || !lastRegion.contains(key)) {
                lastRegion = regionManager.getRegionByKey(key);
            }
            groups.computeIfAbsent(lastRegion, k -> new ArrayList<>()).add(key);
        }
        return groups;
    }
}
