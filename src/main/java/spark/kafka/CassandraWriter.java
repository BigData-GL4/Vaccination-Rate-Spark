package spark.kafka;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;

public class CassandraWriter implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final String CASSANDRA_HOST = "cassandra";
    private static final String CASSANDRA_KEYSPACE = "spark";
    private static final String CASSANDRA_TABLE = "city_enroll";

    private transient Mapper<VaccinationData> mapper;

    public CassandraWriter() {
        Cluster cluster = Cluster.builder().addContactPoint(CASSANDRA_HOST).build();
        Session session = cluster.connect(CASSANDRA_KEYSPACE);
        MappingManager manager = new MappingManager(session);
        this.mapper = manager.mapper(VaccinationData.class);
    }

    public void writeVaccinationData(String state, int enroll) {
        VaccinationData data = new VaccinationData(state, enroll);

        if (this.mapper == null) {
            Cluster cluster = Cluster.builder().addContactPoint(CASSANDRA_HOST).build();
            Session session = cluster.connect(CASSANDRA_KEYSPACE);
            MappingManager manager = new MappingManager(session);
            this.mapper = manager.mapper(VaccinationData.class);
        }
        this.mapper.save(data);
    }

    @Table(keyspace = CASSANDRA_KEYSPACE, name = CASSANDRA_TABLE)
    public static class VaccinationData implements Serializable {
        private static final long serialVersionUID = 1L;

        @Column(name = "city")
        private String city;
        @Column(name = "enroll")
        private int enroll;

        public VaccinationData(String city, int enroll) {
            this.city = city;
            this.enroll = enroll;
        }
    }
}
