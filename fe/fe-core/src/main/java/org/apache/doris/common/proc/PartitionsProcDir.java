// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.common.proc;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LimitElement;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.coercion.DateLikeType;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/*
 * SHOW PROC /dbs/dbId/tableId/partitions, or
 * SHOW PROC /dbs/dbId/tableId/temp_partitions
 * show [temp] partitions' detail info within a table
 */
public class PartitionsProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("PartitionId").add("PartitionName")
            .add("VisibleVersion").add("VisibleVersionTime")
            .add("State").add("PartitionKey").add("Range").add("DistributionKey")
            .add("Buckets").add("ReplicationNum").add("StorageMedium").add("CooldownTime").add("RemoteStoragePolicy")
            .add("LastConsistencyCheckTime").add("DataSize").add("IsInMemory").add("ReplicaAllocation")
            .add("IsMutable").add("SyncWithBaseTables").add("UnsyncTables").add("CommittedVersion")
            .add("RowCount")
            .build();

    private Database db;
    private OlapTable olapTable;
    private boolean isTempPartition = false;

    public PartitionsProcDir(Database db, OlapTable olapTable, boolean isTempPartition) {
        this.db = db;
        this.olapTable = olapTable;
        this.isTempPartition = isTempPartition;
    }

    public static boolean filter(String columnName, Comparable element, Map<String, Expr> filterMap)
            throws AnalysisException {
        if (filterMap == null) {
            return true;
        }
        Expr subExpr = filterMap.get(columnName.toLowerCase()); // predicate on this column.
        if (subExpr == null) {
            return true;
        }
        if (subExpr instanceof BinaryPredicate) {
            // show partitions only provide very limited filtering capacity in FE. so restrict here.
            if (!(subExpr.getChild(1) instanceof LiteralExpr)) {
                throw new AnalysisException("Not Supported. Use `select * from partitions(...)` instead");
            }

            BinaryPredicate binaryPredicate = (BinaryPredicate) subExpr;
            if (subExpr.getChild(1) instanceof StringLiteral
                    && binaryPredicate.getOp() == BinaryPredicate.Operator.EQ) {
                return ((StringLiteral) subExpr.getChild(1)).getValue().equals(element);
            }
            long leftVal;
            long rightVal;
            if (subExpr.getChild(1) instanceof DateLiteral) {
                Type type;
                switch (subExpr.getChild(1).getType().getPrimitiveType()) {
                    case DATE:
                    case DATETIME:
                        type = Type.DATETIME;
                        break;
                    case DATEV2:
                        type = Type.DATETIMEV2;
                        break;
                    case DATETIMEV2:
                        type = subExpr.getChild(1).getType();
                        break;
                    default:
                        throw new AnalysisException("Invalid date type: " + subExpr.getChild(1).getType());
                }
                leftVal = (new DateLiteral((String) element, type)).getLongValue();
                rightVal = ((DateLiteral) subExpr.getChild(1)).getLongValue();
            } else {
                leftVal = Long.parseLong(element.toString());
                rightVal = ((IntLiteral) subExpr.getChild(1)).getLongValue();
            }
            switch (binaryPredicate.getOp()) {
                case EQ:
                case EQ_FOR_NULL:
                    return leftVal == rightVal;
                case GE:
                    return leftVal >= rightVal;
                case GT:
                    return leftVal > rightVal;
                case LE:
                    return leftVal <= rightVal;
                case LT:
                    return leftVal < rightVal;
                case NE:
                    return leftVal != rightVal;
                default:
                    Preconditions.checkState(false, "No defined binary operator.");
            }
        } else {
            return like((String) element, ((StringLiteral) subExpr.getChild(1)).getValue());
        }
        return true;
    }

    private static boolean filterSubExpression(Expression expr, Comparable element) throws AnalysisException {
        // show partitions only provide very limited filtering capacity in FE. so restrict here.
        if (!(expr.child(1) instanceof Literal)) {
            throw new AnalysisException("Not Supported. Use `select * from partitions(...)` instead");
        }

        if (expr instanceof EqualTo && expr.child(1) instanceof StringLikeLiteral) {
            return ((StringLikeLiteral) expr.child(1)).getValue().equals(element.toString());
        }
        long leftVal;
        long rightVal;
        if (expr.child(1) instanceof org.apache.doris.nereids.trees.expressions.literal.DateLiteral) {
            DateLikeType dateLikeType;
            if (expr.child(1) instanceof DateV2Literal) {
                dateLikeType = DateV2Type.INSTANCE;
            } else if (expr.child(1) instanceof DateTimeLiteral) {
                dateLikeType = DateTimeType.INSTANCE;
            } else if (expr.child(1) instanceof DateTimeV2Literal) {
                dateLikeType = DateTimeV2Type.MAX;
            } else {
                throw new AnalysisException("Invalid date type: " + expr.child(1).getDataType());
            }
            leftVal = (new org.apache.doris.nereids.trees.expressions.literal.DateLiteral(
                    dateLikeType, (String) element)).getValue();
            rightVal = ((org.apache.doris.nereids.trees.expressions.literal.DateLiteral) expr.child(1)).getValue();
        } else {
            leftVal = Long.parseLong(element.toString());
            rightVal = ((IntegerLikeLiteral) expr.child(1)).getLongValue();
        }

        if (expr instanceof EqualTo) {
            return leftVal == rightVal;
        } else if (expr instanceof NullSafeEqual) {
            return leftVal == rightVal;
        } else if (expr instanceof GreaterThan) {
            return leftVal > rightVal;
        } else if (expr instanceof GreaterThanEqual) {
            return leftVal >= rightVal;
        } else if (expr instanceof LessThan) {
            return leftVal < rightVal;
        } else if (expr instanceof LessThanEqual) {
            return leftVal <= rightVal;
        } else {
            Preconditions.checkState(false, "No defined binary operator.");
        }
        return true;
    }

    public static boolean filterExpression(String columnName, Comparable element, Map<String, Expression> filterMap)
            throws AnalysisException {
        if (filterMap == null) {
            return true;
        }
        Expression subExpr = filterMap.get(columnName.toLowerCase()); // predicate on this column.
        if (subExpr == null) {
            return true;
        }

        if (subExpr instanceof ComparisonPredicate) {
            return filterSubExpression(subExpr, element);
        } else if (subExpr instanceof Not) {
            subExpr = subExpr.child(0);
            if (subExpr instanceof EqualTo) {
                return !filterSubExpression(subExpr, element);
            }
        } else {
            return like(element.toString(), ((StringLikeLiteral) subExpr.child(1)).getStringValue());
        }
        return false;
    }

    public static boolean like(String str, String expr) {
        expr = expr.toLowerCase();
        expr = expr.replace(".", "\\.");
        expr = expr.replace("?", ".");
        expr = expr.replace("%", ".*");
        str = str.toLowerCase();
        return str.matches(expr);
    }

    public ProcResult fetchResultByExpressionFilter(Map<String, Expression> filterMap, List<OrderByPair> orderByPairs,
                                                        LimitElement limitElement) throws AnalysisException {
        List<List<Comparable>> partitionInfos = getPartitionInfos();
        List<List<Comparable>> filterPartitionInfos;
        //where
        if (filterMap == null || filterMap.isEmpty()) {
            filterPartitionInfos = partitionInfos;
        } else {
            filterPartitionInfos = Lists.newArrayList();
            // TODO: we should change the order of loops to speed up. use filters to filter column value.
            for (List<Comparable> partitionInfo : partitionInfos) {
                if (partitionInfo.size() != TITLE_NAMES.size()) {
                    throw new AnalysisException("PartitionInfos.size() " + partitionInfos.size()
                        + " not equal TITLE_NAMES.size() " + TITLE_NAMES.size());
                }
                boolean isNeed = true;
                for (int i = 0; i < partitionInfo.size(); i++) {
                    isNeed = filterExpression(TITLE_NAMES.get(i), partitionInfo.get(i), filterMap);
                    if (!isNeed) {
                        break;
                    }
                }

                if (isNeed) {
                    filterPartitionInfos.add(partitionInfo);
                }
            }
        }

        // order by
        if (orderByPairs != null) {
            ListComparator<List<Comparable>> comparator;
            OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
            comparator = new ListComparator<>(orderByPairs.toArray(orderByPairArr));
            filterPartitionInfos.sort(comparator);
        }

        //limit
        if (limitElement != null && limitElement.hasLimit()) {
            int beginIndex = (int) limitElement.getOffset();
            int endIndex = (int) (beginIndex + limitElement.getLimit());
            if (endIndex > filterPartitionInfos.size()) {
                endIndex = filterPartitionInfos.size();
            }

            // means that beginIndex is bigger than filterPartitionInfos.size(), just return empty
            if (beginIndex > endIndex) {
                beginIndex = endIndex;
            }
            filterPartitionInfos = filterPartitionInfos.subList(beginIndex, endIndex);
        }

        return getBasicProcResult(filterPartitionInfos);
    }

    public ProcResult fetchResultByFilter(Map<String, Expr> filterMap, List<OrderByPair> orderByPairs,
            LimitElement limitElement) throws AnalysisException {
        List<List<Comparable>> partitionInfos = getPartitionInfos();
        List<List<Comparable>> filterPartitionInfos;
        //where
        if (filterMap == null || filterMap.isEmpty()) {
            filterPartitionInfos = partitionInfos;
        } else {
            filterPartitionInfos = Lists.newArrayList();
            // TODO: we should change the order of loops to speed up. use filters to filter column value.
            for (List<Comparable> partitionInfo : partitionInfos) {
                if (partitionInfo.size() != TITLE_NAMES.size()) {
                    throw new AnalysisException("PartitionInfos.size() " + partitionInfos.size()
                        + " not equal TITLE_NAMES.size() " + TITLE_NAMES.size());
                }
                boolean isNeed = true;
                for (int i = 0; i < partitionInfo.size(); i++) {
                    isNeed = filter(TITLE_NAMES.get(i), partitionInfo.get(i), filterMap);
                    if (!isNeed) {
                        break;
                    }
                }

                if (isNeed) {
                    filterPartitionInfos.add(partitionInfo);
                }
            }
        }

        // order by
        if (orderByPairs != null) {
            ListComparator<List<Comparable>> comparator;
            OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
            comparator = new ListComparator<>(orderByPairs.toArray(orderByPairArr));
            filterPartitionInfos.sort(comparator);
        }

        //limit
        if (limitElement != null && limitElement.hasLimit()) {
            int beginIndex = (int) limitElement.getOffset();
            int endIndex = (int) (beginIndex + limitElement.getLimit());
            if (endIndex > filterPartitionInfos.size()) {
                endIndex = filterPartitionInfos.size();
            }

            // means that beginIndex is bigger than filterPartitionInfos.size(), just return empty
            if (beginIndex > endIndex) {
                beginIndex = endIndex;
            }
            filterPartitionInfos = filterPartitionInfos.subList(beginIndex, endIndex);
        }

        return getBasicProcResult(filterPartitionInfos);
    }

    public BaseProcResult getBasicProcResult(List<List<Comparable>> partitionInfos) {
        // set result
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        for (List<Comparable> info : partitionInfos) {
            List<String> row = new ArrayList<String>(info.size());
            for (Comparable comparable : info) {
                row.add(comparable.toString());
            }
            result.addRow(row);
        }

        return result;
    }

    private List<List<Comparable>> getPartitionInfos() throws AnalysisException {
        List<Pair<List<Comparable>, TRow>> partitionInfosInrernal = getPartitionInfosInrernal();
        return partitionInfosInrernal.stream().map(pair -> pair.first).collect(Collectors.toList());
    }

    public List<TRow> getPartitionInfosForTvf() throws AnalysisException {
        List<Pair<List<Comparable>, TRow>> partitionInfosInrernal = getPartitionInfosInrernal();
        return partitionInfosInrernal.stream().map(pair -> pair.second).collect(Collectors.toList());
    }

    private List<Pair<List<Comparable>, TRow>> getPartitionInfosInrernal() throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(olapTable);
        Preconditions.checkState(olapTable.isManagedTable());

        // get info
        List<Pair<List<Comparable>, TRow>> partitionInfos = new ArrayList<Pair<List<Comparable>, TRow>>();
        Map<Long, List<String>> partitionsUnSyncTables = null;
        String mtmvPartitionSyncErrorMsg = null;

        List<TableIf> needLocked = Lists.newArrayList();
        needLocked.add(olapTable);
        if (olapTable instanceof MTMV) {
            MTMV mtmv = (MTMV) olapTable;
            for (BaseTableInfo baseTableInfo : mtmv.getRelation().getBaseTables()) {
                try {
                    TableIf baseTable = MTMVUtil.getTable(baseTableInfo);
                    needLocked.add(baseTable);
                } catch (Exception e) {
                    // do nothing, ignore not existed table
                }
            }
            needLocked.sort(Comparator.comparing(TableIf::getId));
        }
        MetaLockUtils.readLockTables(needLocked);
        try {
            if (olapTable instanceof MTMV) {
                try {
                    partitionsUnSyncTables = MTMVPartitionUtil
                            .getPartitionsUnSyncTables((MTMV) olapTable);
                } catch (AnalysisException e) {
                    mtmvPartitionSyncErrorMsg = e.getMessage();
                }
            }
            List<Long> partitionIds;
            PartitionInfo tblPartitionInfo = olapTable.getPartitionInfo();

            // for range partitions, we return partitions in ascending range order by default.
            // this is to be consistent with the behaviour before 0.12
            if (tblPartitionInfo.getType() == PartitionType.RANGE
                    || tblPartitionInfo.getType() == PartitionType.LIST) {
                partitionIds = tblPartitionInfo.getPartitionItemEntryList(isTempPartition, true).stream()
                        .map(Map.Entry::getKey).collect(Collectors.toList());
            } else {
                Collection<Partition> partitions = isTempPartition
                        ? olapTable.getAllTempPartitions() : olapTable.getPartitions();
                partitionIds = partitions.stream().map(Partition::getId).collect(Collectors.toList());
            }

            Joiner joiner = Joiner.on(", ");
            for (Long partitionId : partitionIds) {
                Partition partition = olapTable.getPartition(partitionId);

                List<Comparable> partitionInfo = new ArrayList<Comparable>();
                TRow trow = new TRow();
                String partitionName = partition.getName();
                partitionInfo.add(partitionId);
                trow.addToColumnValue(new TCell().setLongVal(partitionId));
                partitionInfo.add(partitionName);
                trow.addToColumnValue(new TCell().setStringVal(partitionName));
                partitionInfo.add(partition.getVisibleVersion());
                trow.addToColumnValue(new TCell().setLongVal(partition.getVisibleVersion()));
                String visibleTime = TimeUtils.longToTimeString(partition.getVisibleVersionTime());
                partitionInfo.add(visibleTime);
                trow.addToColumnValue(new TCell().setStringVal(visibleTime));
                partitionInfo.add(partition.getState());
                trow.addToColumnValue(new TCell().setStringVal(partition.getState().toString()));
                if (tblPartitionInfo.getType() == PartitionType.RANGE
                        || tblPartitionInfo.getType() == PartitionType.LIST) {
                    List<Column> partitionColumns = tblPartitionInfo.getPartitionColumns();
                    List<String> colNames = new ArrayList<>();
                    for (Column column : partitionColumns) {
                        colNames.add(column.getName());
                    }
                    String colNamesStr = joiner.join(colNames);
                    partitionInfo.add(colNamesStr);
                    trow.addToColumnValue(new TCell().setStringVal(colNamesStr));
                    String itemStr = tblPartitionInfo.getPartitionRangeString(partitionId);
                    partitionInfo.add(itemStr);
                    trow.addToColumnValue(new TCell().setStringVal(itemStr));
                } else {
                    partitionInfo.add("");
                    trow.addToColumnValue(new TCell().setStringVal(""));
                    partitionInfo.add("");
                    trow.addToColumnValue(new TCell().setStringVal(""));
                }

                // distribution
                DistributionInfo distributionInfo = partition.getDistributionInfo();
                if (distributionInfo.getType() == DistributionInfoType.HASH) {
                    HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                    List<Column> distributionColumns = hashDistributionInfo.getDistributionColumns();
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < distributionColumns.size(); i++) {
                        if (i != 0) {
                            sb.append(", ");
                        }
                        sb.append(distributionColumns.get(i).getName());
                    }
                    partitionInfo.add(sb.toString());
                    trow.addToColumnValue(new TCell().setStringVal(sb.toString()));
                } else {
                    partitionInfo.add("RANDOM");
                    trow.addToColumnValue(new TCell().setStringVal("RANDOM"));
                }

                partitionInfo.add(distributionInfo.getBucketNum());
                trow.addToColumnValue(new TCell().setIntVal(distributionInfo.getBucketNum()));
                // replica num
                short totalReplicaNum = tblPartitionInfo.getReplicaAllocation(partitionId).getTotalReplicaNum();
                partitionInfo.add(totalReplicaNum);
                trow.addToColumnValue(new TCell().setIntVal(totalReplicaNum));

                DataProperty dataProperty = tblPartitionInfo.getDataProperty(partitionId);
                partitionInfo.add(dataProperty.getStorageMedium().name());
                trow.addToColumnValue(new TCell().setStringVal(dataProperty.getStorageMedium().name()));
                String cooldownTimeStr = TimeUtils.longToTimeString(dataProperty.getCooldownTimeMs());
                partitionInfo.add(cooldownTimeStr);
                trow.addToColumnValue(new TCell().setStringVal(cooldownTimeStr));
                partitionInfo.add(dataProperty.getStoragePolicy());
                trow.addToColumnValue(new TCell().setStringVal(dataProperty.getStoragePolicy()));
                String lastCheckTime = TimeUtils.longToTimeString(partition.getLastCheckTime());
                partitionInfo.add(lastCheckTime);
                trow.addToColumnValue(new TCell().setStringVal(lastCheckTime));
                long dataSize = partition.getDataSize(false);
                Pair<Double, String> sizePair = DebugUtil.getByteUint(dataSize);
                String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(sizePair.first) + " "
                        + sizePair.second;
                partitionInfo.add(readableSize);
                trow.addToColumnValue(new TCell().setStringVal(readableSize));
                boolean isInMemory = tblPartitionInfo.getIsInMemory(partitionId);
                partitionInfo.add(isInMemory);
                trow.addToColumnValue(new TCell().setBoolVal(isInMemory));
                // replica allocation
                String replica = tblPartitionInfo.getReplicaAllocation(partitionId).toCreateStmt();
                partitionInfo.add(replica);
                trow.addToColumnValue(new TCell().setStringVal(replica));

                boolean isMutable = tblPartitionInfo.getIsMutable(partitionId);
                partitionInfo.add(isMutable);
                trow.addToColumnValue(new TCell().setBoolVal(isMutable));
                if (olapTable instanceof MTMV) {
                    if (StringUtils.isEmpty(mtmvPartitionSyncErrorMsg)) {
                        List<String> partitionUnSyncTables = partitionsUnSyncTables.getOrDefault(partitionId,
                                Lists.newArrayList());
                        boolean isSync = partitionsUnSyncTables.containsKey(partitionId) && CollectionUtils.isEmpty(
                                partitionUnSyncTables);
                        partitionInfo.add(isSync);
                        trow.addToColumnValue(new TCell().setBoolVal(isSync));
                        // The calculation logic of partitionsUnSyncTables is not protected in the current lock,
                        // so the obtained partition list may not be consistent with here
                        String unSyncTables = partitionsUnSyncTables.containsKey(partitionId)
                                ? partitionUnSyncTables.toString() : "not sure, please try again";
                        partitionInfo.add(unSyncTables);
                        trow.addToColumnValue(new TCell().setStringVal(unSyncTables));
                    } else {
                        partitionInfo.add(false);
                        trow.addToColumnValue(new TCell().setBoolVal(false));
                        partitionInfo.add(mtmvPartitionSyncErrorMsg);
                        trow.addToColumnValue(new TCell().setStringVal(mtmvPartitionSyncErrorMsg));
                    }
                } else {
                    partitionInfo.add(true);
                    trow.addToColumnValue(new TCell().setBoolVal(true));
                    partitionInfo.add(FeConstants.null_string);
                    trow.addToColumnValue(new TCell().setStringVal(FeConstants.null_string));
                }

                partitionInfo.add(partition.getCommittedVersion());
                trow.addToColumnValue(new TCell().setLongVal(partition.getCommittedVersion()));

                partitionInfo.add(partition.getRowCount());
                trow.addToColumnValue(new TCell().setLongVal(partition.getRowCount()));

                partitionInfos.add(Pair.of(partitionInfo, trow));
            }
        } finally {
            MetaLockUtils.readUnlockTables(needLocked);
        }
        return partitionInfos;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        List<List<Comparable>> partitionInfos = getPartitionInfos();
        return getBasicProcResult(partitionInfos);
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String partitionIdStr) throws AnalysisException {
        long partitionId = -1L;
        try {
            partitionId = Long.valueOf(partitionIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid partition id format: " + partitionIdStr);
        }

        olapTable.readLock();
        try {
            Partition partition = olapTable.getPartition(partitionId);
            if (partition == null) {
                throw new AnalysisException("Partition[" + partitionId + "] does not exist");
            }

            return new IndicesProcDir(db, olapTable, partition);
        } finally {
            olapTable.readUnlock();
        }
    }

    public static int analyzeColumn(String columnName) throws AnalysisException {
        for (int i = 0; i < TITLE_NAMES.size(); ++i) {
            if (TITLE_NAMES.get(i).equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_COLUMN_NAME,
                columnName, FeNameFormat.getColumnNameRegex());
        return -1;
    }
}
