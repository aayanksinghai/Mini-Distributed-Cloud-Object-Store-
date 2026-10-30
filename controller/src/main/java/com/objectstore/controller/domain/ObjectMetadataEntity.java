package com.objectstore.controller.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * User-defined key/value metadata entry associated with an object.
 */
@Entity
@Table(name = "object_metadata")
public class ObjectMetadataEntity {

    @EmbeddedId
    private ObjectMetadataId id;

    @Column(name = "meta_value", length = 1024, nullable = false)
    private String metaValue;

    protected ObjectMetadataEntity() {
    }

    public ObjectMetadataEntity(UUID objectId, String metaKey, String metaValue) {
        this.id = new ObjectMetadataId(objectId, metaKey);
        this.metaValue = metaValue;
    }

    public ObjectMetadataId getId() {
        return id;
    }

    public UUID getObjectId() {
        return id.getObjectId();
    }

    public String getMetaKey() {
        return id.getMetaKey();
    }

    public String getMetaValue() {
        return metaValue;
    }

    public void setMetaValue(String metaValue) {
        this.metaValue = metaValue;
    }

    @Embeddable
    public static class ObjectMetadataId implements Serializable {

        @Column(name = "object_id", nullable = false)
        private UUID objectId;

        @Column(name = "meta_key", length = 128, nullable = false)
        private String metaKey;

        protected ObjectMetadataId() {
        }

        public ObjectMetadataId(UUID objectId, String metaKey) {
            this.objectId = objectId;
            this.metaKey = metaKey;
        }

        public UUID getObjectId() {
            return objectId;
        }

        public String getMetaKey() {
            return metaKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ObjectMetadataId that)) return false;
            return Objects.equals(objectId, that.objectId) && Objects.equals(metaKey, that.metaKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(objectId, metaKey);
        }
    }
}

