package io.axway.iron.spi.migration;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;
import io.axway.iron.spi.model.snapshot.SerializableSnapshot;

public class MigrationService {
    public static SerializableSnapshot migrateSnapshot(SerializableSnapshot snapshot,
                                                       String storeName,
                                                       int targetVersion,
                                                       Collection<IronStoreMigrationProcess> migrationProcesses,
                                                       Function<SerializableSnapshot, Long> versionDetector){

        long currentVersion = snapshot.getApplicationModelVersion();

        Map<Integer, IronStoreMigrationProcess> migrationProcessesPerVersion = new HashMap<>();
        migrationProcesses.forEach(p -> migrationProcessesPerVersion.put(p.getVersion(), p));

        if(currentVersion==0 && versionDetector != null){
             //check old-style client version information
              currentVersion = extractVersionFromVersionEntities(snapshot, versionDetector);
        }

        if(currentVersion<targetVersion){
            migrate(snapshot, storeName, currentVersion, targetVersion, migrationProcessesPerVersion);
        }
        snapshot.setApplicationModelVersion(targetVersion);
        return snapshot;
    }

    private static long  extractVersionFromVersionEntities(SerializableSnapshot snapshot, Function<SerializableSnapshot, Long> versionDetector){
        Long oldStyleVersion = versionDetector.apply(snapshot);
        return oldStyleVersion;
    }

    private static void migrate(SerializableSnapshot snapshot, String storeName, long fromVersion, int targetVersion, Map<Integer, IronStoreMigrationProcess> migrationProcesses){
        fromVersion++;
        for (int currentVersion = Integer.valueOf(fromVersion+""); currentVersion <= targetVersion; currentVersion++) {
            executeMigration(migrationProcesses.get(currentVersion), storeName, snapshot);
         }
    }

    private static void executeMigration(IronStoreMigrationProcess migrationStep, String storeName, SerializableSnapshot snapshot){
        migrationStep.migrate(storeName, snapshot);
    }
}
