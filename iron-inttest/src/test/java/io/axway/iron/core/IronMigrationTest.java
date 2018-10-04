package io.axway.iron.core;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;
import org.assertj.core.api.Assertions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import io.axway.iron.StoreManager;
import io.axway.iron.sample.model.BasicPerson;
import io.axway.iron.sample.model.VersionPO;
import io.axway.iron.spi.migration.IronStoreMigrationProcess;
import io.axway.iron.spi.model.snapshot.SerializableEntity;
import io.axway.iron.spi.model.snapshot.SerializableInstance;
import io.axway.iron.spi.model.snapshot.SerializableSnapshot;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;

import static io.axway.iron.spi.chronicle.ChronicleTestHelper.buildChronicleTransactionStoreFactory;
import static io.axway.iron.spi.file.FileTestHelper.*;
import static io.axway.iron.spi.jackson.JacksonTestHelper.*;

public class IronMigrationTest {

    SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();
    TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();

    StoreManager storeManager;

    final String personEntityName = "io.axway.iron.sample.model.BasicPerson";



    @DataProvider(name = "stores1")
    public Object[][] providesStores1() {
        Path filePath = Paths.get("tmp-iron-test");
        SnapshotStore fileSnapshotStore = buildFileSnapshotStore(filePath, "iron-sample1");
        TransactionStore fileTransactionStore = buildFileTransactionStore(filePath, "iron-sample");

        TransactionStore chronicleTransactionStore = buildChronicleTransactionStoreFactory("iron-sample", filePath);

        String storeBaseName = "migrationtest";

        return new Object[][]{ //
                {chronicleTransactionStore, fileSnapshotStore, storeBaseName}, //
                {fileTransactionStore, fileSnapshotStore, storeBaseName}, //
        };
    }

    @DataProvider(name = "stores2")
    public Object[][] providesStores2() {
        Path filePath = Paths.get("tmp-iron-test");
        SnapshotStore fileSnapshotStore = buildFileSnapshotStore(filePath, "iron-sample2");
        TransactionStore fileTransactionStore = buildFileTransactionStore(filePath, "iron-sample");

        TransactionStore chronicleTransactionStore = buildChronicleTransactionStoreFactory("iron-sample", filePath);

        String storeBaseName = "migrationtest";

        return new Object[][]{ //
                {chronicleTransactionStore, fileSnapshotStore, storeBaseName}, //
                {fileTransactionStore, fileSnapshotStore, storeBaseName}, //
        };
    }

    @DataProvider(name = "stores3")
    public Object[][] providesStores3() {
        Path filePath = Paths.get("tmp-iron-test");
        SnapshotStore fileSnapshotStore = buildFileSnapshotStore(filePath, "iron-sample3");
        TransactionStore fileTransactionStore = buildFileTransactionStore(filePath, "iron-sample");

        TransactionStore chronicleTransactionStore = buildChronicleTransactionStoreFactory("iron-sample", filePath);

        String storeBaseName = "migrationtest";

        return new Object[][]{ //
                {chronicleTransactionStore, fileSnapshotStore, storeBaseName}, //
                {fileTransactionStore, fileSnapshotStore, storeBaseName}, //
        };
    }

    @DataProvider(name = "stores4")
    public Object[][] providesStores4() {
        Path filePath = Paths.get("tmp-iron-test");
        SnapshotStore fileSnapshotStore = buildFileSnapshotStore(filePath, "iron-sample4");
        TransactionStore fileTransactionStore = buildFileTransactionStore(filePath, "iron-sample");

        TransactionStore chronicleTransactionStore = buildChronicleTransactionStoreFactory("iron-sample", filePath);

        String storeBaseName = "migrationtest";

        return new Object[][]{ //
                {chronicleTransactionStore, fileSnapshotStore, storeBaseName}, //
                {fileTransactionStore, fileSnapshotStore, storeBaseName}, //
        };
    }
    @Test(dataProvider = "stores1")
    public void testMigrateWithoutAppModelVersion(TransactionStore transactionStore, SnapshotStore snapshotStore, String storeName) throws Exception {

        StoreManager storeManager = StoreManagerBuilder.newStoreManagerBuilder() //
                .withTransactionSerializer(transactionSerializer) //
                .withTransactionStore(transactionStore) //
                .withSnapshotSerializer(snapshotSerializer) //
                .withSnapshotStore(snapshotStore) //
                .withEntityClass(BasicPerson.class) //
                .withMigration(2, getMigrationSteps(), (snapshot) -> 0l) //
                .build();

        storeManager.getStore("migrationtest").query(tx -> {
            Collection<BasicPerson> persons = tx.select(BasicPerson.class).all();
            Assertions.assertThat(persons.size()).isEqualTo(2);
            Assertions.assertThat(persons.stream().findFirst().get().name()).isEqualTo("Name1");
            Assertions.assertThat(persons.stream().skip(1).findFirst().get().name()).isEqualTo("Name2");
        });

    }

    @Test(dataProvider = "stores2")
    public void testMigrateWithSignificantAppModelVersion(TransactionStore transactionStore, SnapshotStore snapshotStore, String storeName) throws Exception {

        StoreManager storeManager = StoreManagerBuilder.newStoreManagerBuilder() //
                .withTransactionSerializer(transactionSerializer) //
                .withTransactionStore(transactionStore) //
                .withSnapshotSerializer(snapshotSerializer) //
                .withSnapshotStore(snapshotStore) //
                .withEntityClass(BasicPerson.class) //
                .withEntityClass(VersionPO.class) //
                .withMigration(3, getMigrationSteps(), (snapshot) -> 0l) //
                .build();

        storeManager.getStore("migrationtest").query(tx -> {
            Collection<BasicPerson> persons = tx.select(BasicPerson.class).all();
            Assertions.assertThat(persons.size()).isEqualTo(1);
        });
    }

    @Test(dataProvider = "stores3")
    public void testMigrateWithOldStyleVersionPO(TransactionStore transactionStore, SnapshotStore snapshotStore, String storeName) throws Exception {

        StoreManager storeManager = StoreManagerBuilder.newStoreManagerBuilder() //
                .withTransactionSerializer(transactionSerializer) //
                .withTransactionStore(transactionStore) //
                .withSnapshotSerializer(snapshotSerializer) //
                .withSnapshotStore(snapshotStore) //
                .withEntityClass(BasicPerson.class) //
                .withEntityClass(VersionPO.class) //
                .withMigration(4, getMigrationSteps(), getVersionDetector()) //
                .build();

        storeManager.getStore("migrationtest").query(tx -> {
            Collection<BasicPerson> persons = tx.select(BasicPerson.class).all();
            Assertions.assertThat(persons.size()).isEqualTo(1);
            Assertions.assertThat(persons.stream().findFirst().get().name()).isEqualTo("Name4");
        });

    }

    @Test(dataProvider = "stores4")
    public void testErrorStoreWithSeveralVersions(TransactionStore transactionStore, SnapshotStore snapshotStore, String storeName) throws Exception {

        try {
            StoreManager storeManager = StoreManagerBuilder.newStoreManagerBuilder() //
                    .withTransactionSerializer(transactionSerializer) //
                    .withTransactionStore(transactionStore) //
                    .withSnapshotSerializer(snapshotSerializer) //
                    .withSnapshotStore(snapshotStore) //
                    .withEntityClass(BasicPerson.class) //
                    .withEntityClass(VersionPO.class) //
                    .withMigration(4, getMigrationSteps(), getVersionDetector()) //
                    .build();
        }
        catch(Exception e){
            e.printStackTrace();
            Assertions.assertThat(e.getMessage()).isEqualTo("Error occurred when recovering from latest snapshot");
        }
    }

    //testGlobalAndTennantStores

    private Collection<IronStoreMigrationProcess> getMigrationSteps() {

        IronStoreMigrationProcess step1 = getMigrationStepWithIndex(1, "Name1", 1500d);
        IronStoreMigrationProcess step2 = getMigrationStepWithIndex(2, "Name2", 1550d);
        IronStoreMigrationProcess step3 = getMigrationStepWithIndex(3, "Name3", 1600d);
        IronStoreMigrationProcess step4 = getMigrationStepWithIndex(4, "Name4", 1400d);

        return Arrays.asList(step1, step2, step3, step4);
    }

    private static SerializableInstance createNewPerson(final long id, String name, Double salary) {
        SerializableInstance newPerson = new SerializableInstance();
        newPerson.setId(id);
        HashMap<String, Object> details = new HashMap<String, Object>();
        details.put("name", name);
        details.put("salary", salary);
        details.put("id", id);
        newPerson.setValues(details);

        return newPerson;
    }

    IronStoreMigrationProcess getMigrationStepWithIndex(int index, String name, Double salary) {
        IronStoreMigrationProcess step = new IronStoreMigrationProcess() {
            @Override
            public int getVersion() {
                return index;
            }

            @Override
            public void migrate(String storeName, SerializableSnapshot snapshot) {
                SerializableEntity personEntity = IronStoreMigrationProcess.findEntity(snapshot, personEntityName);
                long nextId = personEntity.getNextId();
                List<SerializableInstance> instances = personEntity.getInstances().stream().collect(Collectors.toList());
                instances.add(createNewPerson(nextId, name, salary));
                personEntity.setInstances(instances);
                personEntity.setNextId(nextId + 1);
            }
        };
        return step;
    }

    private Function<SerializableSnapshot, Long> getVersionDetector() {
        return (snapshot) -> {
            int currentVersion = 0;

            SerializableEntity versionEntity = null;
            SerializableInstance versionInstance = null;
            for (SerializableEntity entity : snapshot.getEntities()) {
                if (entity.getEntityName().toLowerCase().indexOf("versionpo") >= 0) {
                    versionEntity = entity;
                    for (SerializableInstance instance : versionEntity.getInstances()) {
                        int version = (Integer) instance.getValues().get("version");
                        if (version > currentVersion) {
                            currentVersion = version;
                        }
                    }
                    break;
                }
            }
            return Long.valueOf(currentVersion + "");
        };
    }
}
