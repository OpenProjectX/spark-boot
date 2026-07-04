package org.openprojectx.spark.boot.examples.jdbciceberghms

import java.time.Duration
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

class MariaDbContainer : GenericContainer<MariaDbContainer>(
    DockerImageName.parse("ghcr.io/openprojectx/dockerhub/library/mariadb:10.6.27-jammy")
) {
    val jdbcUrl: String
        get() = "jdbc:mariadb://$host:${getMappedPort(MARIADB_PORT)}/$DB_NAME"

    val mysqlJdbcUrl: String
        get() = "jdbc:mysql://$host:${getMappedPort(MARIADB_PORT)}/$DB_NAME?useSSL=false&allowPublicKeyRetrieval=true"

    init {
        withExposedPorts(MARIADB_PORT)
        withEnv("MARIADB_DATABASE", DB_NAME)
        withEnv("MARIADB_USER", DB_USER)
        withEnv("MARIADB_PASSWORD", DB_PASSWORD)
        withEnv("MARIADB_ROOT_PASSWORD", "root")
        waitingFor(
            Wait.forListeningPort()
                .withStartupTimeout(Duration.ofMinutes(2))
        )
    }
}

const val DB_NAME = "spark_boot"
const val DB_USER = "spark"
const val DB_PASSWORD = "spark"
const val MARIADB_PORT = 3306
