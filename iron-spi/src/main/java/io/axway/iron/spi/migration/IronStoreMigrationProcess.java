package io.axway.iron.spi.migration;

import java.util.*;
import io.axway.iron.spi.model.snapshot.SerializableAttributeDefinition;
import io.axway.iron.spi.model.snapshot.SerializableEntity;
import io.axway.iron.spi.model.snapshot.SerializableInstance;
import io.axway.iron.spi.model.snapshot.SerializableRelationDefinition;
import io.axway.iron.spi.model.snapshot.SerializableSnapshot;

public interface IronStoreMigrationProcess {

    Map<String, StoreInfo> STORE_INFO = new HashMap<>();

    static void putStoreInfo(String storeName, Long tenantKey, String tenantUniqueName) {
        STORE_INFO.put(storeName, new StoreInfo(storeName, tenantKey, tenantUniqueName));
    }

    /**
     * Return the version that this process supports. Store is in version <code>version-1</code> before migration and <code>version</code> after migration.
     */
    int getVersion();

    default void migrate(String storeName, SerializableSnapshot snapshot){

    }
    static SerializableEntity findEntity(SerializableSnapshot snapshot, String entityName) {
        for (SerializableEntity entity : snapshot.getEntities()) {
            if (entity.getEntityName().equals(entityName)) {
                return entity;
            }
        }
        return null;
    }

    class SerializableEntityBuilder {
        private final String m_entityName;
        private final Map<String, SerializableRelationDefinition> m_relations = new HashMap<>();
        private final Collection<SerializableInstance> m_instances = new ArrayList<>();
        private final Map<String, SerializableAttributeDefinition> m_attributes = new HashMap<>();
        private final List<List<String>> m_uniques = new ArrayList<>();

        public SerializableEntityBuilder(String entityName) {
            m_entityName = entityName;
        }

        public SerializableEntityBuilder with(String relation, SerializableRelationDefinition relationDefinition) {
            m_relations.put(relation, relationDefinition);
            return this;
        }

        public SerializableEntityBuilder with(String attribute, SerializableAttributeDefinition attributeDefinition) {
            m_attributes.put(attribute, attributeDefinition);
            return this;
        }

        public SerializableEntityBuilder with(SerializableInstance instance) {
            m_instances.add(instance);
            return this;
        }

        public SerializableEntityBuilder with(List<String> uniques) {
            m_uniques.add(uniques);
            return this;
        }

        public SerializableEntity build() {
            SerializableEntity serializableEntity = new SerializableEntity();
            serializableEntity.setEntityName(m_entityName);
            serializableEntity.setRelations(m_relations);
            serializableEntity.setInstances(m_instances);
            serializableEntity.setAttributes(m_attributes);
            serializableEntity.setUniques(m_uniques);
            return serializableEntity;
        }
    }

    class SerializableAttributeDefinitionBuilder {
        private final String m_dataType;
        private boolean m_nullable = false;

        /**
         * Possible values :
         * * boolean, byte, char, short, int, long, float, double
         * * java.lang.String, java.util.Date
         *
         * @param dataType
         */
        public SerializableAttributeDefinitionBuilder(String dataType) {
            m_dataType = dataType;
        }

        public SerializableAttributeDefinitionBuilder nullable() {
            m_nullable = true;
            return this;
        }

        public SerializableAttributeDefinition build() {
            SerializableAttributeDefinition serializableAttributeDefinition = new SerializableAttributeDefinition();
            serializableAttributeDefinition.setDataType(m_dataType);
            serializableAttributeDefinition.setNullable(m_nullable);
            return serializableAttributeDefinition;
        }
    }

    class StoreInfo {
        private final String m_storeName;
        private final Long m_tenantKey;
        private final String m_tenantUniqueName;

        public StoreInfo(String storeName, Long tenantKey, String tenantUniqueName) {
            m_storeName = storeName;
            m_tenantKey = tenantKey;
            m_tenantUniqueName = tenantUniqueName;
        }

        public String getStoreName() {
            return m_storeName;
        }

        public Long getTenantKey() {
            return m_tenantKey;
        }

        public String getTenantUniqueName() {
            return m_tenantUniqueName;
        }
    }
}


