package com.flipkart.foxtrot.core.nodegroup;

import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.elasticsearch.index.IndexInfoResponse;
import com.flipkart.foxtrot.common.elasticsearch.node.FileSystemDetails;
import com.flipkart.foxtrot.common.elasticsearch.node.FileSystemDetails.FileSystemOverview;
import com.flipkart.foxtrot.common.elasticsearch.node.NodeFSStatsResponse;
import com.flipkart.foxtrot.common.elasticsearch.node.NodeFSStatsResponse.NodeFSDetails;
import com.flipkart.foxtrot.common.exception.NodeGroupExecutionException;
import com.flipkart.foxtrot.common.nodegroup.*;
import com.flipkart.foxtrot.common.nodegroup.ESNodeGroupDetails.DiskUsageInfo;
import com.flipkart.foxtrot.common.nodegroup.visitors.MoveTablesRequest;
import com.flipkart.foxtrot.common.util.Utils;
import com.flipkart.foxtrot.core.config.NodeGroupActivityConfig;
import com.flipkart.foxtrot.core.nodegroup.repository.NodeGroupRepository;
import com.flipkart.foxtrot.core.nodegroup.visitors.AllocatedTableFinder;
import com.flipkart.foxtrot.core.nodegroup.visitors.AllocatedTableIndexFinder;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.table.TableManager;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.flipkart.foxtrot.common.elasticsearch.node.NodeFSStatsResponse.NodeFSDetails.DATA_ROLE;
import static com.flipkart.foxtrot.common.nodegroup.ESNodeGroup.AllocationStatus.ALLOCATED;
import static com.flipkart.foxtrot.common.nodegroup.ESNodeGroup.AllocationStatus.VACANT;
import static com.flipkart.foxtrot.common.nodegroup.TableAllocation.TableAllocationType.SPECIFIC;
import static com.flipkart.foxtrot.core.util.StorageSizeUtils.humanReadableByteCountSI;

@Slf4j
@Singleton
public class NodeGroupManagerImpl implements NodeGroupManager {

    private static final String DEFAULT_VACANT_GROUP_NAME = "vacant";
    private NodeGroupRepository nodeGroupRepository;
    private AllocationManager allocationManager;
    private ElasticsearchQueryStore queryStore;
    private TableManager tableManager;
    private NodeGroupActivityConfig nodeGroupActivityConfig;
    private ExecutorService executorService;
    private DateTime lastVacantGroupReadRepairTime;

    @Inject
    public NodeGroupManagerImpl(final NodeGroupRepository nodeGroupRepository,
                                final AllocationManager allocationManager,
                                final ElasticsearchQueryStore queryStore,
                                final TableManager tableManager,
                                final ExecutorService executorService,
                                final NodeGroupActivityConfig nodeGroupActivityConfig) {
        this.nodeGroupRepository = nodeGroupRepository;
        this.allocationManager = allocationManager;
        this.executorService = executorService;
        this.queryStore = queryStore;
        this.nodeGroupActivityConfig = nodeGroupActivityConfig;
        this.lastVacantGroupReadRepairTime = new DateTime();
        this.tableManager = tableManager;
    }

    @Override
    public ESNodeGroup createNodeGroup(ESNodeGroup nodeGroup) {

        validateCreateNodeGroupRequest(nodeGroup);

        saveNodeGroup(nodeGroup);

        readRepairVacantNodeGroup();

        return nodeGroup;
    }

    @Override
    public ESNodeGroup getNodeGroup(String groupName) {
        ESNodeGroup nodeGroup = nodeGroupRepository.get(groupName);
        if (nodeGroup == null) {
            throw new NodeGroupExecutionException("Node group doesn't exist with given name : " + groupName);
        }
        if (VACANT.equals(nodeGroup.getStatus())) {
            VacantGroupReadRepairResult readRepairResult = readRepairVacantNodeGroup();
            return readRepairResult.getVacantNodeGroup();
        }

        return nodeGroup;
    }

    @Override
    public AllocatedESNodeGroup getNodeGroupByTable(String table) {
        AllocatedTableFinder allocatedTableFinder = getAllocatedTableFinder();

        List<ESNodeGroup> esNodeGroups = nodeGroupRepository.getAll();

        return esNodeGroups.stream()
                .filter(esNodeGroup -> esNodeGroup instanceof AllocatedESNodeGroup)
                .filter(esNodeGroup -> {
                    AllocatedESNodeGroup allocatedESNodeGroup = (AllocatedESNodeGroup) esNodeGroup;
                    Set<String> allocatedTables = allocatedESNodeGroup.getTableAllocation()
                            .accept(allocatedTableFinder);
                    return allocatedTables.contains(table);
                })
                .map(AllocatedESNodeGroup.class::cast)
                .findFirst()
                .orElse(null);

    }

    @Override
    public void deleteNodeGroup(String groupName) {
        ESNodeGroup existingNodeGroup = nodeGroupRepository.get(groupName);

        if (existingNodeGroup == null) {
            throw new NodeGroupExecutionException("Node group doesn't exist with given name : " + groupName);
        }

        if (existingNodeGroup.getStatus()
                .equals(VACANT)) {
            throw new NodeGroupExecutionException("Can't delete vacant group");
        }

        nodeGroupRepository.delete(groupName);
        allocationManager.deleteNodeAllocationTemplate(groupName);

        readRepairVacantNodeGroup();

    }

    @Override
    public List<ESNodeGroup> getNodeGroups() {
        readRepairVacantNodeGroupAfterElapsedTime();
        return nodeGroupRepository.getAll();
    }


    @Override
    public List<ESNodeGroupDetailResponse> getNodeGroupDetails() {
        readRepairVacantNodeGroupAfterElapsedTime();
        List<ESNodeGroup> esNodeGroups = nodeGroupRepository.getAll();
        List<NodeFSDetails> dataNodes = getDataNodes();

        return esNodeGroups.stream()
                .map(esNodeGroup -> {
                    ESNodeGroupDetails esNodeGroupDetails = getEsNodeGroupDetails(esNodeGroup, dataNodes);
                    return ESNodeGroupDetailResponse.builder()
                            .nodeGroup(esNodeGroup)
                            .details(esNodeGroupDetails)
                            .build();
                })
                .collect(Collectors.toList());
    }

    /*
        1. Make node patterns in a node group more specific
           ("elasticsearch6*" can be updated to --> "elasticsearch62*","elasticsearch61*","elasticsearch63*","elasticsearch64*","elasticsearch65*")
        2. Remove node patterns from node group (removed nodes will go to vacant group)
        3. Add node patterns which match nodes from vacant group
        4. Add a table in SPECIFIC allocation group which not covered by any other SPECIFIC allocation node group
           (earlier it was served by COMMON allocation node group, now it will be served by input SPECIFIC allocation group)
        5. Remove a table (will be served by COMMON allocation node group thereafter)
        6. Change total shards per node
     */
    @Override
    public ESNodeGroup updateNodeGroup(String groupName,
                                       ESNodeGroup nodeGroup) {

        validateUpdateNodeGroupRequest(groupName, nodeGroup);

        saveNodeGroup(nodeGroup);

        // sync the updated node group
        AllocatedTableIndexFinder allocatedTableIndexFinder = getAllocatedTableIndexFinder();
        syncAllocation(allocatedTableIndexFinder, nodeGroup);

        // sync the common node group
        AllocatedESNodeGroup commonGroup = nodeGroupRepository.getCommonGroup();
        syncAllocation(allocatedTableIndexFinder, commonGroup);

        readRepairVacantNodeGroup();

        return nodeGroup;
    }

    @Override
    public void moveTablesBetweenGroups(MoveTablesRequest moveTablesRequest) {

        ESNodeGroup sourceNodeGroup = nodeGroupRepository.get(moveTablesRequest.getSourceGroup());
        ESNodeGroup destinationNodeGroup = nodeGroupRepository.get(moveTablesRequest.getDestinationGroup());

        validateMoveTablesRequest(moveTablesRequest, sourceNodeGroup, destinationNodeGroup);

        AllocatedESNodeGroup sourceGroup = (AllocatedESNodeGroup) sourceNodeGroup;
        AllocatedESNodeGroup destinationGroup = (AllocatedESNodeGroup) destinationNodeGroup;

        SpecificTableAllocation sourceTableAllocation = (SpecificTableAllocation) sourceGroup.getTableAllocation();
        SpecificTableAllocation destinationTableAllocation = (SpecificTableAllocation) destinationGroup.getTableAllocation();

        sourceTableAllocation.getTables()
                .removeAll(moveTablesRequest.getTables());
        destinationTableAllocation.getTables()
                .addAll(moveTablesRequest.getTables());

        // we've modified table allocation through references so save will work fine
        nodeGroupRepository.save(sourceNodeGroup);
        nodeGroupRepository.save(destinationNodeGroup);

        syncAllocation(moveTablesRequest.getTables(), destinationNodeGroup);
    }

    @Override
    public void syncAllocation(String groupName) {
        AllocatedTableIndexFinder allocatedTableIndexFinder = getAllocatedTableIndexFinder();
        ESNodeGroup nodeGroup = nodeGroupRepository.get(groupName);
        syncAllocation(allocatedTableIndexFinder, nodeGroup);
    }


    @Override
    public ESNodeGroupDetailResponse getNodeGroupDetails(String groupName) {
        ESNodeGroup nodeGroup = nodeGroupRepository.get(groupName);
        ESNodeGroupDetails esNodeGroupDetails;

        if (VACANT.equals(nodeGroup.getStatus())) {
            VacantGroupReadRepairResult vacantGroupReadRepairResult = readRepairVacantNodeGroup();
            nodeGroup = vacantGroupReadRepairResult.getVacantNodeGroup();
            esNodeGroupDetails = getEsNodeGroupDetails(nodeGroup, vacantGroupReadRepairResult.getDataNodes());

        } else {
            esNodeGroupDetails = getEsNodeGroupDetails(nodeGroup);
        }

        return ESNodeGroupDetailResponse.builder()
                .nodeGroup(nodeGroup)
                .details(esNodeGroupDetails)
                .build();
    }

    private void syncAllocation(AllocatedTableIndexFinder allocatedTableIndexFinder,
                                ESNodeGroup nodeGroup) {

        if (nodeGroup != null && nodeGroup.getStatus()
                .equals(ALLOCATED)) {

            // sync all indices in node group in async manner
            executorService.submit(() -> {

                AllocatedESNodeGroup allocatedESNodeGroup = (AllocatedESNodeGroup) nodeGroup;
                Set<String> allocatedTableIndices = allocatedESNodeGroup.getTableAllocation()
                        .accept(allocatedTableIndexFinder);
                allocationManager.syncAllocationSettings(allocatedTableIndices, allocatedESNodeGroup);
            });
        }

    }

    private AllocatedTableIndexFinder getAllocatedTableIndexFinder() {
        Set<String> allTableIndices = queryStore.getTableIndicesInfo()
                .stream()
                .map(IndexInfoResponse::getIndex)
                .collect(Collectors.toSet());
        List<ESNodeGroup> nodeGroups = nodeGroupRepository.getAll();

        Set<String> specificAllocatedTables = nodeGroups.stream()
                .filter(allocatedGroupPredicate())
                .filter(nodeGroup -> ((AllocatedESNodeGroup) nodeGroup).getTableAllocation()
                        .getType()
                        .equals(SPECIFIC))
                .map(nodeGroup -> ((SpecificTableAllocation) ((AllocatedESNodeGroup) nodeGroup).getTableAllocation()).getTables())
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());

        return new AllocatedTableIndexFinder(allTableIndices, specificAllocatedTables);
    }

    private AllocatedTableFinder getAllocatedTableFinder() {
        Set<String> tables = tableManager.getAll()
                .stream()
                .map(Table::getName)
                .collect(Collectors.toSet());
        List<ESNodeGroup> nodeGroups = nodeGroupRepository.getAll();

        Set<String> specificAllocatedTables = nodeGroups.stream()
                .filter(allocatedGroupPredicate())
                .filter(nodeGroup -> ((AllocatedESNodeGroup) nodeGroup).getTableAllocation()
                        .getType()
                        .equals(SPECIFIC))
                .map(nodeGroup -> ((SpecificTableAllocation) ((AllocatedESNodeGroup) nodeGroup).getTableAllocation()).getTables())
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());

        return new AllocatedTableFinder(tables, specificAllocatedTables);
    }

    private void syncAllocation(List<String> tables,
                                ESNodeGroup nodeGroup) {

        if (nodeGroup != null && nodeGroup.getStatus()
                .equals(ALLOCATED)) {
            executorService.submit(() -> {

                Set<String> allTableIndices = queryStore.getTableIndicesInfo()
                        .stream()
                        .map(IndexInfoResponse::getIndex)
                        .collect(Collectors.toSet());

                Set<String> indicesToSync = allTableIndices.stream()
                        .filter(index -> tables.contains(ElasticsearchUtils.getTableNameFromIndex(index)))
                        .collect(Collectors.toSet());

                AllocatedESNodeGroup allocatedESNodeGroup = (AllocatedESNodeGroup) nodeGroup;

                allocationManager.syncAllocationSettings(indicesToSync, allocatedESNodeGroup);
            });
        }

    }

    private void validateCreateNodeGroupRequest(ESNodeGroup nodeGroup) {
        checkIfNodeGroupWithSameNameAlreadyExists(nodeGroup);

        checkIfVacantGroupAlreadyExists(nodeGroup);

        checkNonOverlappingConditions(nodeGroup);

    }

    private void validateUpdateNodeGroupRequest(String groupName,
                                                ESNodeGroup nodeGroup) {
        if (!groupName.equals(nodeGroup.getGroupName())) {
            throw new NodeGroupExecutionException(
                    "Group names do not match: " + groupName + " , " + nodeGroup.getGroupName());
        }

        ESNodeGroup existingNodeGroup = nodeGroupRepository.get(groupName);

        if (existingNodeGroup == null) {
            throw new NodeGroupExecutionException("Couldn't find node group with name: " + groupName);
        }
        if (!existingNodeGroup.getStatus()
                .equals(nodeGroup.getStatus())) {
            throw new NodeGroupExecutionException("Can't change allocation status for ES node group");
        }

        checkNonOverlappingConditions(nodeGroup);
    }


    private void checkNonOverlappingConditions(ESNodeGroup nodeGroup) {
        List<ESNodeGroup> existingNodeGroups = nodeGroupRepository.getAll();

        List<AllocatedESNodeGroup> existingAllocatedGroups = existingNodeGroups.stream()
                .filter(allocatedGroupPredicate())
                .map(AllocatedESNodeGroup.class::cast)
                .collect(Collectors.toList());

        if (ALLOCATED.equals(nodeGroup.getStatus())) {
            AllocatedESNodeGroup allocatedESNodeGroup = (AllocatedESNodeGroup) nodeGroup;
            checkIfNodePatternIsOverlapping(allocatedESNodeGroup, existingAllocatedGroups);
            checkIfTableAllocationIsOverlapping(allocatedESNodeGroup, existingAllocatedGroups);
        } else {
            VacantESNodeGroup vacantESNodeGroup = (VacantESNodeGroup) nodeGroup;
            checkIfNodePatternIsOverlapping(vacantESNodeGroup, existingAllocatedGroups);
        }
    }

    private void validateMoveTablesRequest(MoveTablesRequest moveTablesRequest,
                                           ESNodeGroup sourceNodeGroup,
                                           ESNodeGroup destinationNodeGroup) {
        if (sourceNodeGroup == null || destinationNodeGroup == null) {
            throw new NodeGroupExecutionException(
                    "Bad request: either of the source or destination group name doesn't exist");
        }

        if (!destinationNodeGroup.getStatus()
                .equals(ALLOCATED) || !sourceNodeGroup.getStatus()
                .equals(ALLOCATED)) {
            throw new NodeGroupExecutionException("Bad request: source and destination should be allocated group");
        }

        AllocatedESNodeGroup sourceGroup = (AllocatedESNodeGroup) sourceNodeGroup;
        AllocatedESNodeGroup destinationGroup = (AllocatedESNodeGroup) sourceNodeGroup;

        if (!sourceGroup.getTableAllocation()
                .getType()
                .equals(SPECIFIC) || !destinationGroup.getTableAllocation()
                .getType()
                .equals(SPECIFIC)) {
            throw new NodeGroupExecutionException(
                    "Bad request: source and destination should have specific table allocation");
        }

        SpecificTableAllocation sourceTableAllocation = (SpecificTableAllocation) sourceGroup.getTableAllocation();

        Optional<String> nonExistingTable = moveTablesRequest.getTables()
                .stream()
                .filter(table -> !sourceTableAllocation.getTables()
                        .contains(table))
                .findFirst();

        if (nonExistingTable.isPresent()) {
            throw new NodeGroupExecutionException("Bad request: table " + nonExistingTable.get()
                    + " not present in source node group table allocation");
        }
    }

    private void saveNodeGroup(ESNodeGroup nodeGroup) {
        if (ALLOCATED.equals(nodeGroup.getStatus())) {
            AllocatedESNodeGroup esNodeGroup = (AllocatedESNodeGroup) nodeGroup;

            allocationManager.createNodeAllocationTemplate(esNodeGroup);

        }
        nodeGroupRepository.save(nodeGroup);
    }

    private VacantGroupReadRepairResult readRepairVacantNodeGroup(List<NodeFSDetails> dataNodesFsDetails) {
        log.debug("Data nodes present in cluster : {}", dataNodesFsDetails);
        List<String> dataNodes = dataNodesFsDetails.stream()
                .map(NodeFSDetails::getName)
                .collect(Collectors.toList());

        List<ESNodeGroup> allocatedNodeGroups = nodeGroupRepository.getAll()
                .stream()
                .filter(allocatedGroupPredicate())
                .collect(Collectors.toList());

        log.debug("Allocated node groups present: {}", allocatedNodeGroups);

        VacantESNodeGroup vacantGroup = nodeGroupRepository.getVacantGroup();

        log.debug("Vacant node group present: {}", vacantGroup);
        if (vacantGroup == null) {
            vacantGroup = VacantESNodeGroup.builder()
                    .groupName(DEFAULT_VACANT_GROUP_NAME)
                    .nodePatterns(new TreeSet<>())
                    .build();
        }

        List<String> vacantNodes = dataNodes;
        if (!allocatedNodeGroups.isEmpty()) {
            List<Pattern> allocatedNodeRegexPatterns = allocatedNodeGroups.stream()
                    .map(ESNodeGroup::getNodePatterns)
                    .flatMap(Collection::stream)
                    .map(Utils.wildcardRegexPattern())
                    .collect(Collectors.toList());

            log.debug("Allocated node regex patterns: {}", allocatedNodeRegexPatterns);

            vacantNodes = dataNodes.stream()
                    .filter(name -> allocatedNodeRegexPatterns.stream()
                            .noneMatch(pattern -> pattern.matcher(name)
                                    .matches()))
                    .collect(Collectors.toList());
            log.debug("Synced vacant nodes: {}", vacantNodes);
        }

        vacantGroup.setNodePatterns(new TreeSet<>(vacantNodes));

        log.debug("Saving synced vacant node group : {}", vacantGroup);

        nodeGroupRepository.save(vacantGroup);

        return VacantGroupReadRepairResult.builder()
                .vacantNodeGroup(vacantGroup)
                .dataNodes(dataNodesFsDetails)
                .build();
    }

    private void checkIfTableAllocationIsOverlapping(AllocatedESNodeGroup esNodeGroup,
                                                     List<AllocatedESNodeGroup> allocatedESNodeGroups) {
        // check if tables allocated to new node group overlap with existing allocated node group
        Optional<AllocatedESNodeGroup> overlappingTableGroup = allocatedESNodeGroups.stream()
                .filter(hasDifferentGroupNameThan(esNodeGroup))
                .filter(allocatedGroupPredicate())
                .filter(esNodeGroup::isAnyTableAllocationOverlappingWith)
                .findFirst();

        if (overlappingTableGroup.isPresent()) {
            throw new NodeGroupExecutionException(
                    "Table allocation overlapping with existing node group: " + overlappingTableGroup.get()
                            .getGroupName());
        }
    }

    private void checkIfNodePatternIsOverlapping(ESNodeGroup esNodeGroup,
                                                 List<AllocatedESNodeGroup> existingAllocatedGroups) {
        // check if node patterns in new node group overlap with existing allocated node groups
        Optional<AllocatedESNodeGroup> overlappingNodePatternGroup = existingAllocatedGroups.stream()
                .filter(hasDifferentGroupNameThan(esNodeGroup))
                .filter(esNodeGroup::isAnyNodePatternOverlappingWith)
                .findFirst();

        if (overlappingNodePatternGroup.isPresent()) {
            throw new NodeGroupExecutionException(
                    "Node patterns overlapping with existing node group: " + overlappingNodePatternGroup.get()
                            .getGroupName());
        }
    }

    private Predicate<AllocatedESNodeGroup> hasDifferentGroupNameThan(ESNodeGroup esNodeGroup) {
        return existingNodeGroup -> !existingNodeGroup.getGroupName()
                .equals(esNodeGroup.getGroupName());
    }

    private void checkIfNodeGroupWithSameNameAlreadyExists(ESNodeGroup nodeGroup) {
        ESNodeGroup existingNodeGroup = nodeGroupRepository.get(nodeGroup.getGroupName());
        if (existingNodeGroup != null) {
            throw new NodeGroupExecutionException(
                    "Node group already exists with given name : " + nodeGroup.getGroupName());
        }
    }

    private Predicate<ESNodeGroup> allocatedGroupPredicate() {
        return esNodeGroup1 -> ALLOCATED.equals(esNodeGroup1.getStatus());
    }

    // only one vacant node group can exist in cluster
    private void checkIfVacantGroupAlreadyExists(ESNodeGroup nodeGroup) {
        if (nodeGroup.getStatus()
                .equals(VACANT)) {
            VacantESNodeGroup existingVacantGroup = nodeGroupRepository.getVacantGroup();
            if (existingVacantGroup != null) {
                throw new NodeGroupExecutionException(
                        "Vacant Node group already exists with given name : " + existingVacantGroup.getGroupName());
            }
        }
    }

    private List<NodeFSDetails> getDataNodes() {
        NodeFSStatsResponse nodeFSStats = queryStore.getNodeFSStats();
        return nodeFSStats.getNodes()
                .values()
                .stream()
                .filter(nodeDetails -> nodeDetails.getRoles()
                        .contains(DATA_ROLE))
                .collect(Collectors.toList());
    }


    private ESNodeGroupDetails getEsNodeGroupDetails(ESNodeGroup nodeGroup,
                                                     List<NodeFSDetails> dataNodes) {
        List<Pattern> nodePatterns = nodeGroup.getNodePatterns()
                .stream()
                .map(Utils.wildcardRegexPattern())
                .collect(Collectors.toList());

        List<NodeFSDetails> groupNodes = dataNodes.stream()
                .filter(nodeFSDetails -> nodePatterns.stream()
                        .anyMatch(nodePattern -> nodePattern.matcher(nodeFSDetails.getName())
                                .matches()))
                .collect(Collectors.toList());

        Long totalStorageInBytes = groupNodes.stream()
                .map(NodeFSDetails::getFs)
                .map(FileSystemDetails::getTotal)
                .map(FileSystemOverview::getTotalInBytes)
                .reduce(0L, Long::sum);

        Long availableStorageInBytes = groupNodes.stream()
                .map(NodeFSDetails::getFs)
                .map(FileSystemDetails::getTotal)
                .map(FileSystemOverview::getAvailableInBytes)
                .reduce(0L, Long::sum);

        long usedStorageInBytes = totalStorageInBytes - availableStorageInBytes;

        String usedDiskPercentage =
                String.format("%.2f", ((double) usedStorageInBytes / totalStorageInBytes) * 100.0) + "%";

        return ESNodeGroupDetails.builder()
                .nodeCount(groupNodes.size())
                .nodeInfo(getNodeDiskInfo(groupNodes))
                .diskUsageInfo(DiskUsageInfo.builder()
                        .totalDiskStorage(humanReadableByteCountSI(totalStorageInBytes))
                        .availableDiskStorage(humanReadableByteCountSI(availableStorageInBytes))
                        .usedDiskStorage(humanReadableByteCountSI(usedStorageInBytes))
                        .usedDiskPercentage(usedDiskPercentage)
                        .build())
                .build();
    }

    private TreeMap<String, DiskUsageInfo> getNodeDiskInfo(List<NodeFSDetails> groupNodes) {
        return new TreeMap<>(groupNodes.stream()
                .collect(Collectors.toMap(NodeFSDetails::getName, nodeFSDetails -> {

                    Long totalDiskInBytes = nodeFSDetails.getFs()
                            .getTotal()
                            .getTotalInBytes();

                    Long availableDiskInBytes = nodeFSDetails.getFs()
                            .getTotal()
                            .getAvailableInBytes();

                    long usedDiskInBytes = totalDiskInBytes - availableDiskInBytes;

                    String usedPercentage =
                            String.format("%.2f", ((double) usedDiskInBytes / totalDiskInBytes) * 100.0) + "%";
                    return DiskUsageInfo.builder()
                            .totalDiskStorage(humanReadableByteCountSI(totalDiskInBytes))
                            .availableDiskStorage(humanReadableByteCountSI(availableDiskInBytes))
                            .usedDiskStorage(humanReadableByteCountSI(usedDiskInBytes))
                            .usedDiskPercentage(usedPercentage)
                            .build();
                })));
    }

    private ESNodeGroupDetails getEsNodeGroupDetails(ESNodeGroup nodeGroup) {
        List<NodeFSDetails> dataNodes = getDataNodes();
        return getEsNodeGroupDetails(nodeGroup, dataNodes);
    }

    private void readRepairVacantNodeGroupAfterElapsedTime() {
        DateTime currentTime = new DateTime();
        if ((currentTime.getMillis() - lastVacantGroupReadRepairTime.getMillis()) / (60 * 1000)
                > nodeGroupActivityConfig.getVacantGroupReadRepairIntervalInMins()) {
            log.debug("Syncing node info for vacant node group");
            readRepairVacantNodeGroup();
            lastVacantGroupReadRepairTime = currentTime;
        }
    }

    // Find out which all nodes are not part of any allocated node group and
    // read repair vacant node group
    private VacantGroupReadRepairResult readRepairVacantNodeGroup() {
        List<NodeFSDetails> dataNodesFsDetails = getDataNodes();
        return readRepairVacantNodeGroup(dataNodesFsDetails);
    }

}
